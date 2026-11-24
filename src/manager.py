"""
Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
"""

import subprocess
from setup_execution import setup_workers
import os
import signal
import time
from run_report import RunReport
from pathlib import Path
import queue
import copy
from experiment_clock import ExperimentClock, validate_speed_factor
from pods_mapping import PodsMapping
from collector import Collector, CollectionCursor, CollectionError
from sampling import SamplingIntervals, configure_ksm
from kwok_config import read_workers, WORKERS
import yaml
from workload.manager import WorkloadManager
from replay_coordinator import ReplayCoordinator
from infrastructure.manager import InfrastructureManager
from infrastructure.service_monitor.service_monitor_manager import ServiceMonitorManager
import threading

class Manager:
    """
    Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
    of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
    nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
    Attributes:
        input_step (int): Time step interval for processing trace entries.
        data_path (string): The path of the JSON with the objects that will be applied by broker.
    """

    def __init__(self, data_path='/tmp', karpenter=True, infrastructure=None, workload=None, hpa=True, speed_factor=1, kwok_workers_config=None, output_dir=None, report=None):
        """
        Initializes the Manager class.
        """
        self.report = report or RunReport(output_dir, trace_path=data_path)
        self.output_dir = str(self.report.output_dir)
        self.speed_factor = validate_speed_factor(speed_factor, karpenter=karpenter, hpa=hpa)
        self.intervals = SamplingIntervals(self.speed_factor)
        self.workers_override = read_workers(kwok_workers_config)
        self.clock = ExperimentClock(self.speed_factor)
        self.clock.cancel = threading.Event()
        self.clock.defer_tail = True
        self.collection_stop = threading.Event()
        self.collection_error = None
        self.collection_diagnostics = dict(complete=False, errors=[], error_count=0, requested_intervals=self.intervals.snapshot())
        self.observability = {}
        self.infrastructure = infrastructure
        self.workload = workload

        self.report.update(configuration=dict(self.report.data['configuration'], karpenter=karpenter, hpa=hpa,
            infrastructure=infrastructure, workload=workload, speed_factor=self.speed_factor,
            requested_intervals=self.intervals.snapshot(), requested_workers=self.workers_override,
            setup_concurrency=setup_workers(self.speed_factor), bind_concurrency=setup_workers(self.speed_factor)))
        for name in ('infrastructure_description.json', 'workload_description.json'):
            self.report.trace(Path(data_path) / name, 'replay')
        self.infrastructure_manager = InfrastructureManager(f"{data_path}/infrastructure_description.json", karpenter, infrastructure, speed_factor=self.speed_factor)
        self.workload_manager = WorkloadManager(f"{data_path}/workload_description.json", workload, hpa, speed_factor=self.speed_factor)
        # Autoscalers change topology outside the trace; they keep independent producers at 1x.
        self.replay_coordinator = None
        if not karpenter and not hpa:
            self.replay_coordinator = ReplayCoordinator(self.infrastructure_manager, self.workload_manager)
            self.workload_manager.coordinate(self.replay_coordinator.limit_logical)
        self.report.update(configuration=dict(self.report.data['configuration'],
            replay_coordination='instant_barriers' if self.replay_coordinator else 'independent_producers',
            usage_completion_limit_logical=None, fidelity_policy='advisory',
            usage_warning_threshold_logical=self.replay_coordinator.limit_logical if self.replay_coordinator else None))

        self.report.trace(PodsMapping.PODS_ALLOCATION_PATH, 'allocation')
        self.pods_mapping = PodsMapping(karpenter)
        self.pods_mapping.issue_handler = lambda message, **context: self.report.issue(
            message, warning=True, component='pods_mapping', stage='preparation', **context)
        self.collector = Collector(step=self.intervals.query, intervals=self.intervals)
        self.service_monitor_manager = ServiceMonitorManager(interval_seconds=self.intervals.kwok)
        self.service_monitor_manager.issue_handler = lambda error, **context: self.report.issue(
            error, component='service_monitor', **context)

    def log(self, message):
        """
        Logs a message with a "[BROKER]" prefix.
        """
        print(f"[BROKER] {message}")

    def start_mapping_and_scheduler(self):
        """
        Starts the process of mapping pods and running the scheduler.

        This method performs the following actions:
        1. Executes the `run` method of the `pods_mapping` object to initiate pod mapping.
        2. Runs the `build-scheduler.sh` script using a subprocess call to set up the scheduler.
        """
        with self.report.phase('mapping', parent='preparation', concurrency=1):
            self.pods_mapping.run()
        workers = setup_workers(self.speed_factor)
        self.report.update(configuration=dict(self.report.data['configuration'], bind_concurrency=workers))
        with self.report.phase('scheduler_build_binds', parent='preparation', concurrency=workers):
            process = subprocess.Popen(
                ["bash", "src/build-scheduler.sh", str(workers)], start_new_session=True)
            try:
                code = process.wait()
                if code:
                    raise subprocess.CalledProcessError(code, process.args)
            except BaseException:
                # The shell, build and scheduler share this dedicated process group.
                try:
                    os.killpg(process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    process.wait(timeout=5)
                raise

    def prepare_observability(self):
        api = self.service_monitor_manager.k8s_api
        monitor = api.get_namespaced_custom_object(
            'monitoring.coreos.com', 'v1', 'monitoring', 'servicemonitors', 'kube-state-metrics')
        configured = configure_ksm(monitor, self.intervals.ksm)
        api.patch_namespaced_custom_object(
            'monitoring.coreos.com', 'v1', 'monitoring', 'servicemonitors', 'kube-state-metrics',
            {'spec': {'endpoints': configured['spec']['endpoints']}})
        self.initial_nodes = self.service_monitor_manager.prepare()
        config = api.v1.read_namespaced_config_map('kwok', 'kube-system')
        documents = list(yaml.safe_load_all(config.data['kwok.yaml']))
        options = next(doc['options'] for doc in documents if doc.get('kind') == 'KwokConfiguration')
        workers = {key: options.get(key) for key in WORKERS}
        if any(workers[key] != value for key, value in self.workers_override.items()):
            raise ValueError('installed KWOK worker configuration does not match requested overrides; run setup first')
        deployment = api.apps_v1.read_namespaced_deployment('kwok-controller', 'kube-system')
        images = {container.name: container.image for container in deployment.spec.template.spec.containers}
        self.observability = dict(workers=workers, images=images,
            kwok_config_api_version=next(doc['apiVersion'] for doc in documents if doc.get('kind') == 'KwokConfiguration'),
            requested_intervals=self.intervals.snapshot(),
            effective_intervals={**self.collector.wait_ready(self.intervals, self.initial_nodes),
                'query': self.collector.step,
                'reconcile': self.workload_manager.usage_manager.reconcile_interval_seconds})

    def scrape_watermark(self):
        """Require live targets except for nodes removed by completed replay actions."""
        if self.infrastructure_manager.karpenter:
            # Autoscaler-driven lifecycles keep their existing readiness policy.
            return self.collector.targets(self.intervals)
        expected = set(self.initial_nodes)
        actions = self.infrastructure_manager.replay_diagnostics.snapshot()['actions']
        for action in actions:
            if not action['completed']:
                continue
            context = action['context']
            if context['action'] == 'apply-node':
                expected.add(context['name'])
            elif context['action'] == 'delete-node':
                expected.discard(context['name'])
        monitor = self.service_monitor_manager
        try:
            live = {node.metadata.name for node in
                    monitor.k8s_api.v1.list_node(_request_timeout=10).items
                    if monitor._is_fake_node(node)}
        except Exception as error:
            raise CollectionError(f'could not confirm live emulated nodes: {error}') from error
        missing = expected - live
        if missing:
            raise CollectionError(f'emulated nodes disappeared without completed replay deletion: {sorted(missing)}')
        empty = not live and not expected
        self.collection_diagnostics['expected_nodes'] = len(expected)
        self.collection_diagnostics['live_nodes'] = len(live)
        self.collection_diagnostics['kwok_targets_not_required'] = empty
        return self.collector.targets(self.intervals, nodes=sorted(live), allow_empty_kwok=empty)

    def collect_loop(self, start_time):
        """Export confirmed grid points; stop wakes immediately and flushes the tail."""
        cursor = CollectionCursor(start_time, self.intervals.query)
        self.collection_cursor = cursor
        deadline = None
        try:
            while True:
                stopping = self.collection_stop.wait(60 if deadline is None else 1)
                if stopping and deadline is None:
                    deadline = time.monotonic() + 60  # Real ingestion timeout, never scaled.
                    self.collector.request_deadline = deadline
                end = self.collection_end if stopping else time.time()
                try:
                    confirmed = self.scrape_watermark()
                    while True:
                        window = cursor.window(min(end, confirmed))
                        if window is None:
                            break
                        begin, finish, last = window
                        self.collector.collect(begin, finish, self.output_dir)
                        cursor.commit(last)
                        self.collection_diagnostics['confirmed_through'] = finish
                    if stopping and cursor.window(end) is None:
                        self.collection_diagnostics['complete'] = True
                        break
                except (CollectionError, ValueError, KeyError, TypeError) as error:
                    self.collection_diagnostics['error_count'] = self.collection_diagnostics.get('error_count', 0) + 1
                    if len(self.collection_diagnostics['errors']) < 100:
                        self.collection_diagnostics['errors'].append(str(error))
                    self.log(f'[COLLECTION RETRY] {error}')
                if stopping:
                    if time.monotonic() >= deadline:
                        raise CollectionError('final sampling incomplete after real ingestion timeout')
                    # Event.wait would return immediately once stop is set.
                    time.sleep(1)
        except Exception as error:
            self.collection_error = error
            self.collection_diagnostics['error_count'] = self.collection_diagnostics.get('error_count', 0) + 1
            self.collection_diagnostics['errors'].append(str(error))

    def snapshot(self, final=True):
        progress, lag = {}, {}
        for name, manager in (('infrastructure', self.infrastructure_manager),
                              ('workload', self.workload_manager)):
            data = manager.replay_diagnostics.snapshot()
            actions = data.pop('actions')
            progress[name] = {key: value for key, value in data.items()
                              if 'lag' not in key and key not in ('late_events', 'worst_event')}
            lag[name] = {key: value for key, value in data.items()
                         if 'lag' in key or key in ('late_events', 'worst_event')}
            count = data['processed_entries']
            lag[name]['mean_lag_real'] = data['total_lag_real'] / count if count else 0
            lag[name]['mean_lag_logical'] = data['total_lag_logical'] / count if count else 0
            for action in actions if final else ():
                if not action['completed']:
                    self.report.issue(action['error'], stage='replay', component=name,
                                      action=action['context'], trace_timestamp=action['trace_timestamp'])
            if final and data['late_events']:
                self.report.issue(f"{data['late_events']} late entries; input order preserved",
                                  warning=True, stage='replay', component=name)
        with self.workload_manager.usage_lock:
            usage = copy.deepcopy(self.workload_manager.usage_progress)
        usage_errors = usage.pop('errors')
        fidelity_warnings = usage.pop('fidelity_warnings', [])
        for warning in fidelity_warnings if final else ():
            context = {key: value for key, value in warning.items() if key not in ('kind', 'message')}
            self.report.issue(warning['message'], warning=True, component='usage', stage='replay', **context)
        for error in usage_errors if final else ():
            context = {key: value for key, value in error.items() if key not in ('kind', 'message', 'completed')}
            self.report.issue(error.get('message', 'usage reconciliation failed'),
                              component='usage', stage='replay', **context)
        if final and usage['late_events']:
            self.report.issue(f"{usage['late_events']} late usage reconciliations",
                              warning=True, component='usage', stage='replay')
        absent = usage.get('outcomes', {}).get('target_absent')
        if final and absent:
            self.report.issue(f'{absent} usage events found no target workload; their usage was not applied',
                              warning=True, component='usage', stage='replay')
        count = usage['completed']
        usage['mean_lag_real'] = usage['total_lag_real'] / count if count else 0
        usage['mean_lag_logical'] = usage['total_lag_logical'] / count if count else 0
        fidelity = usage.pop('fidelity', None) or {}
        lag['usage'] = {key: value for key, value in usage.items()
                        if 'lag' in key or key in ('worst_event', 'late_events')}
        progress['usage'] = {key: value for key, value in usage.items() if key not in lag['usage']}
        coordinator = getattr(self, 'replay_coordinator', None)
        if coordinator is not None:
            progress['coordination'] = coordinator.snapshot()
            if self.clock.origin_monotonic is not None:
                # Part of producers_real: time spent waiting for usage between instants.
                self.report.timing(usage_barrier_real=progress['coordination']['barrier_wait_real'])
            observation = fidelity.get('violation')
            if final and observation is not None:
                self.report.issue('Usage timing exceeded the diagnostic threshold; inspect the graphs',
                                  warning=True, component='usage', stage='replay', observation=observation)
            self.report.update(fidelity=dict(
                policy='advisory', assessment='manual', temporal_valid=None,
                warning_threshold_logical=fidelity.get('limit_logical'), first_observation=observation))
        if self.clock.origin_monotonic is not None and not final:
            self.report.timing(progress_logical=max(
                progress[name]['final_timestamp'] or 0 for name in ('infrastructure', 'workload')))
        self.report.update(progress=progress, lag=lag, collection=self.collection_diagnostics,
                           observability=self.observability)
        for error in self.collection_diagnostics['errors'] if final else ():
            self.report.issue(error, warning=self.collection_error is None,
                              stage='collection', component='collector')
        for warning in getattr(self.collector, 'warnings', []) if final else ():
            self.report.issue(warning, warning=True, stage='collection', component='collector')

    def run(self):
        """Supervise replays, acknowledge usage, collect the tail, then finalize."""
        report = self.report
        threads = []
        collector_thread = None
        monitor_started = False
        interrupted = False
        finalization_start = None
        producers_end = None
        replay_end = None
        thread_errors = queue.Queue()
        last_snapshot = self.clock.monotonic()

        def guarded(name, function):
            try:
                function(self.clock)
            except BaseException as error:
                thread_errors.put((name, error))
                self.clock.cancel.set()

        def check_threads():
            nonlocal last_snapshot
            check_interruption()
            while not thread_errors.empty():
                name, error = thread_errors.get_nowait()
                report.issue(error, component=name, stage='collection' if name == 'collector' else 'replay',
                             **getattr(error, 'details', {}))
            if self.clock.cancel.is_set():
                raise RuntimeError('replay cancelled after component failure')
            self.workload_manager.check_usage()
            now = self.clock.monotonic()
            if now - last_snapshot >= 1:
                self.snapshot(final=False)
                last_snapshot = now
            if self.collection_error:
                raise RuntimeError('required collection failed; see collection diagnostics')

        def check_interruption():
            if report.interruption_requested:
                raise KeyboardInterrupt('interruption requested during final collection')

        try:
            report.stage('preparation')
            with report.phase('preparation', concurrency=1):
                # Only the coordinator and these synchronous setup methods write.
                self.infrastructure_manager.report = report
                self.workload_manager.report = report
                with report.phase('before_setup', parent='preparation', concurrency=1):
                    self.infrastructure_manager.before_setup()
                    self.workload_manager.before_setup()
                    self.service_monitor_manager.start()
                    monitor_started = True
                with report.phase('nodes', parent='preparation', concurrency=1):
                    self.infrastructure_manager.setup()
                with report.phase('workloads', parent='preparation', concurrency=1):
                    self.workload_manager.setup()
                self.start_mapping_and_scheduler()
                with report.phase('pods_operational', parent='preparation', concurrency=1):
                    self.workload_manager.wait_pods_operational()
                with report.phase('before_emulation', parent='preparation', concurrency=1):
                    self.workload_manager.before_emulation()
                    self.infrastructure_manager.before_emulation()
                with report.phase('port_forward', parent='preparation', concurrency=1):
                    subprocess.run(['bash', 'src/port-forward.sh'], check=True)
                with report.phase('metrics_ready', parent='preparation', concurrency=1):
                    self.prepare_observability()
                with report.phase('initial_usage_drain', parent='preparation',
                                  concurrency=self.workload_manager.usage_progress.get('api_concurrency'),
                                  unit='usage_acknowledgements'):
                    self.workload_manager.check_usage()
                    try:
                        self.workload_manager.drain_usage(check=check_threads)
                        if self.workload_manager.usage_progress['failed'] or self.workload_manager.usage_progress['errors']:
                            raise RuntimeError('Initial usage reconciliation failed')
                    finally:
                        progress = self.workload_manager.usage_progress
                        report.phase_progress('initial_usage_drain', expected=progress['enqueued'],
                                              completed=progress['completed'], failed=progress['failed'])
            self.clock.start()
            scheduled = max((entry['timestamp'] for manager in
                (self.infrastructure_manager, self.workload_manager)
                for entry in manager.data['emulation']), default=0)
            report.timing(preparation_real=self.clock.origin_monotonic - report.data['timing']['started_monotonic'],
                          origin_unix=self.clock.origin_unix, origin_monotonic=self.clock.origin_monotonic,
                          scheduled_logical=scheduled)
            report.stage('replay')
            self.collection_stop.clear()
            collector_thread = threading.Thread(target=guarded,
                args=('collector', lambda clock: self.collect_loop(clock.origin_unix)),
                name='CollectorThread', daemon=True)
            collector_thread.start()
            coordinator = getattr(self, 'replay_coordinator', None)
            producers = ([('replay', coordinator.run)] if coordinator is not None else
                         [('infrastructure', self.infrastructure_manager.emulation),
                          ('workload', self.workload_manager.emulation)])
            for name, function in producers:
                thread = threading.Thread(target=guarded, args=(name, function),
                                          name=name, daemon=True)
                thread.start()
                threads.append(thread)
            while any(thread.is_alive() for thread in threads):
                for thread in threads:
                    thread.join(.1)
                check_threads()
            check_threads()
            producers_end = self.clock.monotonic()
            report.timing(producers_real=producers_end - self.clock.origin_monotonic)
            # Acknowledgements, not queue.put, establish replay completion.
            # Keep periodic reconciliation alive for pods converging in the tail.
            self.workload_manager.drain_usage(check=check_threads)
            replay_end = self.clock.monotonic()
            report.timing(usage_drain_real=replay_end - producers_end)
            usage = self.workload_manager.usage_progress
            complete = (not usage['failed'] and not usage['errors']
                        and not usage.get('outcomes', {}).get('cancelled')
                        and all(not manager.replay_diagnostics.snapshot()['failed_actions']
                                for manager in (self.infrastructure_manager, self.workload_manager)))
            report.timing(replay_real=replay_end - self.clock.origin_monotonic,
                          progress_logical=scheduled, replay_complete=complete,
                          effective_factor=(scheduled / (replay_end - self.clock.origin_monotonic)
                                            if complete and replay_end > self.clock.origin_monotonic else None))
            finalization_start = replay_end
            report.stage('collection')
            report.begin_phase('final_collection')
            tail_start = self.clock.monotonic()
            self.clock.tail()
            report.timing(tail_real=self.clock.monotonic() - tail_start)
            check_threads()
            self.workload_manager.finish_usage(timeout=230, allow_progress=True, check=check_interruption)
        except BaseException as error:
            interrupted = isinstance(error, (KeyboardInterrupt, SystemExit))
            report.issue(error, component='coordinator', **getattr(error, 'details', {}))
        finally:
            self.clock.cancel.set()
            for thread in threads:
                thread.join(timeout=1)
                if thread.is_alive():
                    report.issue('thread did not stop within real cleanup deadline', component=thread.name)
            # Preserve any primary thread exception even if cancellation won the race.
            while not thread_errors.empty():
                name, error = thread_errors.get_nowait()
                report.issue(error, component=name, stage='collection' if name == 'collector' else 'replay',
                             **getattr(error, 'details', {}))
            now = self.clock.monotonic()
            if finalization_start is None:
                finalization_start = now
            if self.clock.origin_monotonic is not None and replay_end is None:
                timestamps = [manager.replay_diagnostics.snapshot()['final_timestamp'] or 0
                              for manager in (self.infrastructure_manager, self.workload_manager)]
                report.timing(replay_real=now - self.clock.origin_monotonic,
                              progress_logical=max(timestamps), effective_factor=None)
                if producers_end is not None:
                    # Partial wait for usage after producers, up to interruption.
                    report.timing(usage_drain_real=now - producers_end)
            elif self.clock.origin_monotonic is None:
                report.timing(preparation_real=now - report.data['timing']['started_monotonic'])
            if collector_thread and 'final_collection' not in report.data.get('phases', {}):
                report.begin_phase('final_collection')
            self.collection_end = time.time()
            self.collection_stop.set()
            if collector_thread:
                collector_thread.join(timeout=65)
                if collector_thread.is_alive():
                    report.issue('collection did not stop within real ingestion deadline',
                                 component='collector', stage='collection')
                elif not self.collection_diagnostics['complete'] and not self.collection_error:
                    report.issue('required collection incomplete', component='collector', stage='collection')
            if 'final_collection' in report.data.get('phases', {}):
                report.end_phase('final_collection', result=(
                    'interrupted' if interrupted or report.interruption_requested else
                    'completed' if self.collection_diagnostics['complete'] else 'failed'))
            report.stage('teardown')
            report.begin_phase('teardown')
            cleanups = [('workload', self.workload_manager.tear_down),
                        ('infrastructure', self.infrastructure_manager.tear_down)]
            if monitor_started:
                cleanups.append(('service_monitor', self.service_monitor_manager.stop))
            for name, cleanup in cleanups:
                try:
                    with report.phase(f'teardown_{name}', parent='teardown', concurrency=1):
                        cleanup()
                except BaseException as error:
                    interrupted = interrupted or isinstance(error, (KeyboardInterrupt, SystemExit))
                    report.issue(error, component=name)
            cleanup_results = {phase['result'] for phase in report.data['phases'].values()
                               if phase['parent'] == 'teardown'}
            report.end_phase('teardown', result='interrupted' if 'interrupted' in cleanup_results else
                             'failed' if 'failed' in cleanup_results else 'completed')
            self.snapshot()
            def finalize():
                report.end_phase('archive', result='recorded_before_close')
                report.timing(finalization_real=self.clock.monotonic() - finalization_start)
                report.finish(interrupted=interrupted)
            report.stage('archive')
            report.begin_phase('archive', concurrency=1)
            finalize()
            try:
                self.collector.archive(self.output_dir, finalize=finalize)
            except BaseException as error:
                interrupted = interrupted or isinstance(error, (KeyboardInterrupt, SystemExit))
                report.issue(error, component='archive', stage='archive')
                finalize()
                report.end_phase('archive', result='interrupted' if interrupted else 'failed')
        if report.data['status'] in ('failed', 'interrupted'):
            raise RuntimeError(f"emulation {report.data['status']}; see {report.path}")
