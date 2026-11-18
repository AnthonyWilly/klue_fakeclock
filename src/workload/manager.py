"""
This module defines the WorkloadManager class, which serves as a base class for managing workloads in a trace emulation environment.
"""

from setup_execution import apply_independent, setup_workers
from run_report import setup_phase, setup_progress
import json
import csv
from pods_mapping import PodsMapping
import time
from experiment_clock import ExperimentClock, ReplayDiagnostics, replay_entries, validate_speed_factor
from kubernetes import client
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_object_applier import KubernetesObjectApplier
import multiprocessing
import queue
import threading
from workload.usage.usage_manager import UsageManager
from workload.usage.usage_tracking import UsageTracker

class WorkloadManager:
    TIME_OUT = 200
    CREATION_TIMEOUT = 600
    SCHEDULING_TIMEOUT = 600
    # Usage events admitted and not yet terminal: transport, worker buffer and execution.
    USAGE_ADMISSION_LIMIT = 1024
    def __init__(self, data_path, emulation_phase, hpa, speed_factor=1):
        """
        Initializes the Workload Manager class.
        """
        self.speed_factor = validate_speed_factor(speed_factor, hpa=hpa)
        self.setup_concurrency = setup_workers(self.speed_factor)
        self.BEFORE_EMULATION_KINDS = {
            "horizontalpodautoscaler",
        }

        self.emulation_phase = emulation_phase
        self.hpa = hpa

        self.k8s_api = K8SAPI(timeout=self.TIME_OUT)
        self.k8s_object_applier = KubernetesObjectApplier(self.k8s_api, hpa)

        with open(data_path, 'r', encoding="utf-8") as file:
            self.data = json.load(file)
        
        self.replay_diagnostics = ReplayDiagnostics(len(self.data['emulation']))
        self.usage_queue = multiprocessing.Queue()
        self.usage_lock = threading.RLock()
        # Serializes producers only; never needed to consume acknowledgements.
        self.usage_admission_lock = threading.Lock()
        self.usage_status = multiprocessing.Queue()
        self.usage_manager = UsageManager(self.usage_queue, reconcile_interval_seconds=5 / self.speed_factor,
                                          status_queue=self.usage_status,
                                          event_workers=self.setup_concurrency)
        self.usage_progress = dict(api_concurrency=UsageManager.API_CONCURRENCY,
            event_workers=self.setup_concurrency,
            admission=dict(limit=self.USAGE_ADMISSION_LIMIT, waits=0, wait_real=0, max_wait_real=0),
            enqueued=0, completed=0, failed=0, pending=0,
            stopped=False, errors=[], late_events=0, max_lag_real=0, total_lag_real=0,
            max_lag_logical=0, total_lag_logical=0, worst_event=None)
        self.usage_tracker = UsageTracker()
        self.usage_progress.update(self.usage_tracker.summary())
        self.usage_stop_sent = False

    def log(self, message):
        """
        Logs a message with a "[WORKLOAD MANAGER]" prefix.
        """
        print(f"[WORKLOAD MANAGER] {message}")

    def before_setup(self):
        pass

    def setup(self):
        """Create namespaces, apply resources, confirm new pods, then queue usage."""
        # start UsageManager process
        self.log("[INFO] Starting UsageManager")
        self.usage_manager.start()

        setup = self.data["setup"]

        applied_objects = setup["applied_objects"]

        workload_actions = setup["workload_actions"]

        self.expected_owners = self.setup_owners()
        self.pods_before_setup = {
            (pod.metadata.namespace, pod.metadata.name)
            for pod in self.k8s_api.list_pod_for_all_namespaces(_request_timeout=10).items}
        objects = []
        for namespace, entries in applied_objects.items():
            self.create_namespace_if_not_exists(namespace)
            for obj in entries:
                if obj.get('kind', '').lower() not in self.BEFORE_EMULATION_KINDS:
                    if obj['metadata'].get('namespace', 'default') != namespace:
                        raise ValueError(f'Object namespace differs from setup namespace: {namespace}')
                    objects.append(obj)
        workers = getattr(self, 'setup_concurrency', 1)
        with setup_phase(self, 'workloads_apply', parent='workloads', expected=len(objects), concurrency=workers, unit='operations'):
            apply_independent(objects, self.apply_setup_object, workers,
                              lambda count: setup_progress(self, 'workloads_apply', completed=count))

        with setup_phase(self, 'workloads_converge', parent='workloads', unit='observed_pods'):
            self.wait_pods_created()

        expected = sum(action['action'] == 'set-usage' or (action['action'] == 'scale' and not self.hpa)
                       for action in workload_actions)
        with setup_phase(self, 'initial_usage', parent='workloads', expected=expected, concurrency=1, unit='operations'):
            completed = 0
            self.log(f"[INFO] Applying workload actions on setup")
            for workload_action in workload_actions:
                action = workload_action["action"]
                if action == "set-usage":
                    self.set_workload_usage(workload_action)
                elif action == "scale" and not self.hpa:
                    self.scale_workload(workload_action)
                else:
                    continue
                completed += 1
                setup_progress(self, 'initial_usage', completed=completed)

    def apply_setup_object(self, obj):
        response = self.k8s_object_applier.apply_object(obj)
        if hasattr(self, 'topology') and obj.get('kind', '').lower() == 'deployment':
            metadata = obj['metadata']
            self.observe_mutation((metadata.get('namespace', 'default'), metadata['name']),
                                  response, obj.get('spec', {}).get('replicas', 1))
        return response

    def before_emulation(self):
        if not self.hpa:
            return
        setup = self.data["setup"]

        applied_objects = setup["applied_objects"]

        self.log(f"[INFO] Deploying setup HPA objects")
        for namespace, objects in applied_objects.items():
            self.create_namespace_if_not_exists(namespace)
            for object in objects:
                if object.get("kind", "").lower() in self.BEFORE_EMULATION_KINDS:
                    self.k8s_object_applier.apply_object(object)

    def emulation(self, clock=None):
        """Replay workload entries without changing trace timestamps or units."""
        clock = clock or ExperimentClock(self.speed_factor).start()
        for entry, scheduled in replay_entries(
                self.data['emulation'], clock, self.replay_diagnostics,
                self.emulation_phase == "dynamic"):
            self.apply_entry_objects(entry, clock, scheduled)
            for action in entry.get('workload_actions', []):
                self.replay_action(action, entry, clock, scheduled)
            self.delete_entry_objects(entry, clock, scheduled)

    # Each phase keeps the input order of its entry. The coordinated replay
    # runs them per instant with usage barriers between topology changes.

    def apply_entry_objects(self, entry, clock, scheduled):
        diagnostics = self.replay_diagnostics
        for namespace, objects in entry.get('applied_objects', {}).items():
            for obj in objects:
                if self.ignore_hpa(obj):
                    diagnostics.skip()
                    continue
                diagnostics.apply(
                    clock, scheduled, {"action": "apply-object", "object": obj},
                    self.apply_workload_object, namespace, obj)

    def apply_entry_topology(self, entry, clock, scheduled):
        """Objects, then scale and other non-usage actions of one entry."""
        self.apply_entry_objects(entry, clock, scheduled)
        for action in entry.get('workload_actions', []):
            if action['action'] != 'set-usage':
                self.replay_action(action, entry, clock, scheduled)

    def publish_entry_usage(self, entry, clock, scheduled):
        """Deliver the set-usage actions of one entry; return their event ids."""
        delivered = []
        for action in entry.get('workload_actions', []):
            if action['action'] == 'set-usage':
                self.replay_action(action, entry, clock, scheduled, delivered)
        return delivered

    def replay_action(self, action, entry, clock, scheduled, delivered=None):
        diagnostics = self.replay_diagnostics
        kind = action['action']
        if kind == 'set-usage':
            # Queue metadata is serializable; the child process starts
            # before the common clock origin exists in the parent.
            timing = {
                "scheduled_monotonic": scheduled,
                "trace_timestamp": entry['timestamp'],
                "origin_monotonic": clock.origin_monotonic,
                "origin_unix": clock.origin_unix,
                "speed_factor": clock.speed_factor,
            }
            event = dict(action, replay_timing=timing)
            limit = self.usage_tracker.limit_logical
            if limit is not None:
                # Expectations are diagnostic; they never impose a logical timeout.
                event['expected_topology'] = self.expected_topology(
                    (action.get('namespace'), action.get('name')))
            sink = delivered if delivered is not None else []
            diagnostics.apply(clock, scheduled, {"action": "queue-usage"},
                              lambda *args: sink.append(self.set_workload_usage(*args)),
                              event, getattr(clock, 'cancel', None))
        elif kind == 'scale' and not self.hpa:
            diagnostics.apply(clock, scheduled, action, self.scale_workload, action)
        elif kind == 'scale':
            diagnostics.skip()
        else:
            diagnostics.apply(clock, scheduled, action, self.unsupported_action, kind)

    def delete_entry_objects(self, entry, clock, scheduled):
        diagnostics = self.replay_diagnostics
        for obj in entry.get('deleted_objects', []):
            if self.ignore_hpa(obj):
                diagnostics.skip()
                continue
            diagnostics.apply(clock, scheduled,
                              {"action": "delete-object", "object": obj},
                              self.delete_workload_object, obj)

    # ------------------------------------------------------------ topology

    def coordinate(self, limit_logical):
        """Enable diagnostic timing thresholds and topology expectations.

        Expectations start from the setup deployments and follow every
        confirmed mutation: the API response gives UID, generation and
        replicas, so no read is issued per usage event.
        """
        self.usage_tracker.limit_logical = limit_logical
        self.topology = {}
        for namespace, objects in self.data.get('setup', {}).get('applied_objects', {}).items():
            for obj in objects:
                if obj.get('kind', '').lower() == 'deployment':
                    self.topology[(obj['metadata'].get('namespace', namespace), obj['metadata']['name'])] = dict(
                        present=True, uid=None, generation=None, replicas=None)

    def expected_topology(self, key):
        expected = getattr(self, 'topology', {}).get(key)
        return dict(expected) if expected else dict(present=False, uid=None, generation=None, replicas=None)

    def observe_mutation(self, key, response, replicas=None):
        """Record the identity a confirmed deployment mutation returned."""
        if not hasattr(self, 'topology'):
            self.topology = {}
        metadata = getattr(response, 'metadata', None)
        spec = getattr(response, 'spec', None)
        def value(source, name, kind):
            result = getattr(source, name, None)
            return result if isinstance(result, kind) and not isinstance(result, bool) else None
        previous = self.topology.get(key) or {}
        uid = value(metadata, 'uid', str)
        same = uid is None or uid == previous.get('uid')
        observed = value(spec, 'replicas', int)
        self.topology[key] = dict(
            present=True,
            uid=uid or previous.get('uid'),
            generation=value(metadata, 'generation', int) or (previous.get('generation') if same else None),
            replicas=observed if observed is not None else replicas)

    def apply_workload_object(self, namespace, obj):
        self.create_namespace_if_not_exists(namespace)
        response = self.k8s_object_applier.apply_object(obj)
        if obj.get('kind', '').lower() == 'deployment':
            spec_replicas = obj.get('spec', {}).get('replicas', 1)
            self.observe_mutation((obj['metadata'].get('namespace', namespace), obj['metadata']['name']),
                                  response, spec_replicas)
        return response

    def delete_workload_object(self, obj):
        response = self.k8s_object_applier.delete_object(obj)
        if obj.get('kind', 'Deployment').lower() == 'deployment':
            key = (obj.get('namespace', obj.get('metadata', {}).get('namespace', 'default')),
                   obj.get('name', obj.get('metadata', {}).get('name')))
            getattr(self, 'topology', {}).pop(key, None)
        return response

    def ignore_hpa(self, obj):
        return not self.hpa and obj.get('kind', '').lower() == 'horizontalpodautoscaler'

    @staticmethod
    def unsupported_action(kind):
        raise ValueError(f"Unsupported workload action: {kind}")

    def check_usage(self, stopping=False):
        with self.usage_lock:
            self._check_usage(stopping)

    def usage_done(self, event_ids):
        """True when every event id reached its terminal acknowledgement."""
        with self.usage_lock:
            pending = self.usage_tracker.pending
            return not any(event_id in pending for event_id in event_ids)

    def _check_usage(self, stopping=False):
        progress = self.usage_progress
        tracker = self.usage_tracker
        while True:
            try:
                message = self.usage_status.get_nowait()
            except queue.Empty:
                break
            kind = message['kind']
            if kind == 'started':
                problem = tracker.started(message)
                if problem:
                    progress['errors'].append(dict(message, kind='protocol', message=problem))
            elif kind == 'ack':
                # A duplicate or unknown identity is never counted twice.
                problem = tracker.acknowledge(message)
                if problem:
                    progress['errors'].append(dict(message, kind='protocol', message=problem))
                    continue
                progress['completed' if message['completed'] else 'failed'] += 1
                if not message['completed']:
                    progress['errors'].append(message)
                else:
                    lag = message.get('lag_real', 0)
                    progress['late_events'] += int(lag > 0)
                    progress['total_lag_real'] += lag
                    progress['total_lag_logical'] += message.get('lag_logical', 0)
                    if lag > progress['max_lag_real']:
                        progress['max_lag_real'] = lag
                        progress['max_lag_logical'] = message['lag_logical']
                        progress['worst_event'] = message
            elif kind == 'stopped':
                progress['stopped'] = True
            elif kind == 'concurrency':
                # Requested and observed worker concurrency of the usage process.
                progress['concurrency'] = message['stats']
            elif kind == 'cache':
                # Observation counters only; they do not prove usage progress.
                progress['resource_cache'] = message['stats']
            elif kind == 'fidelity-warning':
                progress['fidelity_warning_count'] = progress.get('fidelity_warning_count', 0) + 1
                warnings = progress.setdefault('fidelity_warnings', [])
                if len(warnings) < 100:
                    warnings.append(message)
            elif kind == 'activity':
                progress.setdefault('api', {})[message['action']] = message['stats']
                self.usage_activity = getattr(self, 'usage_activity', 0) + 1
            else:
                progress['errors'].append(message)
        tracker.check_deadlines()
        progress['pending'] = len(tracker.pending)
        progress.update(tracker.summary())
        if self.usage_manager.pid is not None and self.usage_manager.exitcode is not None:
            if self.usage_manager.exitcode != 0:
                raise RuntimeError(f'UsageManager exited with code {self.usage_manager.exitcode}')
            if not stopping:
                raise RuntimeError('UsageManager stopped before replay finished')

    def drain_usage(self, timeout=UsageManager.TIME_OUT + 30, check=None):
        """Wait for all acknowledgements, without stopping periodic reconciliation.

        Only inactivity is bounded: a healthy backlog may take longer than the
        cleanup deadline. API completions and event acks both prove activity.
        """
        last_progress = time.monotonic()
        previous = None
        while True:
            self.check_usage()
            progress = self.usage_progress
            if not progress['pending']:
                return
            if self.usage_manager.pid is None:
                raise RuntimeError('queued usage has no running worker')
            current = (progress['completed'], progress['failed'], getattr(self, 'usage_activity', 0))
            now = time.monotonic()
            if current != previous:
                previous = current
                last_progress = now
            if now - last_progress >= timeout:
                raise RuntimeError('UsageManager made no progress within real inactivity deadline')
            if check is not None:
                check()
            self.usage_manager.join(timeout=.1)

    def finish_usage(self, timeout=30, allow_progress=False, check=None):
        if self.usage_manager.pid is None:
            return
        if not self.usage_stop_sent:
            self.usage_queue.put('STOP')
            self.usage_stop_sent = True
        deadline = time.monotonic() + timeout
        previous = None
        while self.usage_manager.is_alive():
            self.check_usage(stopping=True)
            if check is not None:
                check()
            current = (self.usage_progress['completed'], self.usage_progress['failed'],
                       getattr(self, 'usage_activity', 0))
            if allow_progress and current != previous:
                deadline = time.monotonic() + timeout
                previous = current
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise RuntimeError('UsageManager did not drain within real cleanup deadline')
            self.usage_manager.join(timeout=min(.1, remaining))
        self.check_usage(stopping=True)
        if self.usage_progress['pending'] or not self.usage_progress['stopped']:
            raise RuntimeError('UsageManager exited without acknowledging all queued events')

    def tear_down(self):
        if self.usage_manager.pid is None:
            return
        try:
            self.finish_usage()
        finally:
            if self.usage_manager.is_alive():
                self.usage_manager.terminate()
                self.usage_manager.join(timeout=5)
            if self.usage_manager.is_alive():
                self.usage_manager.kill()
                self.usage_manager.join(timeout=5)
            # Never wait for a feeder whose consumer has died.
            self.usage_queue.cancel_join_thread()
            self.usage_queue.close()

    def setup_owners(self):
        """Final replica counts for the generator's namespace/deployment label."""
        owners = {}
        for namespace, objects in self.data['setup']['applied_objects'].items():
            for obj in objects:
                if obj.get('kind', '').lower() in {'deployment', 'statefulset'}:
                    owner = obj.get('spec', {}).get('template', {}).get('metadata', {}).get(
                        'labels', {}).get('deployment', obj['metadata']['name'])
                    owners[(namespace, owner)] = obj.get('spec', {}).get('replicas', 1)
        return owners

    def owned_pods(self, pods):
        grouped = {key: [] for key in self.expected_owners}
        for pod in pods:
            key = (pod.metadata.namespace, (pod.metadata.labels or {}).get('deployment'))
            if key in grouped:
                identity = (pod.metadata.namespace, pod.metadata.name)
                if identity in self.pods_before_setup:
                    raise RuntimeError(f'Pre-existing workload pod cannot seed setup: {identity}')
                grouped[key].append(pod)
        return grouped

    def wait_pods_created(self):
        """Pending custom-scheduler pods are valid here; no bind is required yet."""
        deadline = time.monotonic() + self.CREATION_TIMEOUT
        expected = sum(self.expected_owners.values())
        setup_progress(self, 'workloads_converge', expected=expected)
        pending = dict(self.expected_owners)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f'Pods not created within real setup deadline: {pending}')
            grouped = self.owned_pods(self.k8s_api.list_pod_for_all_namespaces(
                _request_timeout=min(10, remaining)).items)
            pending = {key: {'expected': count, 'observed': len(grouped[key])}
                       for key, count in self.expected_owners.items() if len(grouped[key]) != count}
            setup_progress(self, 'workloads_converge', completed=sum(len(pods) for pods in grouped.values()))
            if not pending:
                self.created_pods = {(pod.metadata.namespace, pod.metadata.name): pod.metadata.uid
                                     for pods in grouped.values() for pod in pods}
                return
            excess = {key: value for key, value in pending.items() if value['observed'] > value['expected']}
            if excess:
                raise RuntimeError(f'Excess workload pods: {excess}')
            time.sleep(min(2, max(0, deadline - time.monotonic())))

    def wait_pods_operational(self, mapping_path=PodsMapping.PODS_AND_NODES_PATH):
        """Confirm the same pods on their mapped nodes and KWOK Running/Ready."""
        with open(mapping_path, newline='') as source:
            rows = list(csv.DictReader(source))
        destinations = {(row['namespace'], row['pod']): row['node'] for row in rows}
        if len(destinations) != len(rows) or set(destinations) != set(self.created_pods):
            raise RuntimeError('Initial placement differs from created workload pods')
        deadline = time.monotonic() + self.SCHEDULING_TIMEOUT
        pending = sorted(destinations)
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise TimeoutError(f'Pods not operational within real scheduling deadline: {pending}')
            grouped = self.owned_pods(self.k8s_api.list_pod_for_all_namespaces(
                _request_timeout=min(10, remaining)).items)
            pods = {(pod.metadata.namespace, pod.metadata.name): pod
                    for entries in grouped.values() for pod in entries}
            if set(pods) - set(destinations):
                raise RuntimeError(f'Unexpected workload pods after binds: {sorted(set(pods) - set(destinations))}')
            pending = []
            for key, node in destinations.items():
                pod = pods.get(key)
                if pod is None:
                    pending.append(key)
                    continue
                if pod.metadata.uid != self.created_pods[key]:
                    raise RuntimeError(f'Workload pod replaced during setup: {key}')
                if pod.spec.node_name and pod.spec.node_name != node:
                    raise RuntimeError(f'Incorrect initial destination for {key}: {pod.spec.node_name}, expected {node}')
                ready = pod.status and pod.status.phase == 'Running' and any(
                    condition.type == 'Ready' and condition.status == 'True'
                    for condition in (pod.status.conditions or []))
                if pod.spec.node_name != node or not ready or pod.metadata.deletion_timestamp:
                    pending.append(key)
            setup_progress(self, 'pods_operational', expected=len(destinations), completed=len(destinations) - len(pending))
            if not pending:
                return
            time.sleep(min(2, max(0, deadline - time.monotonic())))

    def namespace_exists(self, namespace):
        """
        Checks if a Kubernetes namespace exists.
        """
        try:
            self.k8s_api.read_namespace(name=namespace)
            return True
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return False
            raise

    def create_namespace_if_not_exists(self, namespace):
        """
        Ensures that a Kubernetes namespace exists. If the namespace does not exist, it creates it.
        """
        if not self.namespace_exists(namespace):
            body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
            self.k8s_api.create_namespace(body=body)
            self.log(f"[INFO] Namespace {namespace} created.")
        else:
            self.log(f"[INFO] Namespace {namespace} already exists.")

    def count_pods_in_namespaces(self, namespaces: set):
        """
        Counts only pods that belong to the given set of namespaces.
        This avoids mismatches caused by hardcoded exclusion lists that may
        not align with the namespaces actually used in the setup.
        """
        pods = self.k8s_api.list_pod_for_all_namespaces()
        return sum(1 for pod in pods.items if pod.metadata.namespace in namespaces)
    
    def set_workload_usage(self, usage_info, cancel=None):
        """Deliver one event with a unique identity; only delivered events count.

        At most ``USAGE_ADMISSION_LIMIT`` events are admitted and not terminal.
        The producer waits for capacity, and puts the event, without holding
        ``usage_lock``, which the acknowledgement consumer needs. The identity
        is registered before the put so an early acknowledgement is never
        unknown; a failed put withdraws it, leaving no pending event.
        """
        with self.usage_admission_lock:
            self.wait_usage_capacity(cancel)
            with self.usage_lock:
                event = dict(usage_info, event_id=self.usage_tracker.next_event_id)
                self.usage_tracker.register(event)
            try:
                self.usage_queue.put(event)
            except BaseException:
                with self.usage_lock:
                    self.usage_tracker.withdraw(event['event_id'])
                    self.usage_progress['pending'] = len(self.usage_tracker.pending)
                raise
            with self.usage_lock:
                self.usage_progress['enqueued'] += 1
                self.usage_progress['pending'] = len(self.usage_tracker.pending)
            return event['event_id']

    def wait_usage_capacity(self, cancel=None, timeout=UsageManager.TIME_OUT + 30, poll=.05):
        """Wait, interruptibly, until fewer than the admission limit are pending.

        Capacity is released only by terminal acknowledgements, never inferred
        from queue sizes. Worker failure or a full admission without progress for
        ``timeout`` real seconds raises instead of waiting forever.
        """
        started = time.monotonic()
        last_progress, previous = started, None
        waited = False
        try:
            while True:
                self.check_usage()
                with self.usage_lock:
                    if len(self.usage_tracker.pending) < self.USAGE_ADMISSION_LIMIT:
                        return
                    current = (self.usage_progress['completed'], self.usage_progress['failed'],
                               getattr(self, 'usage_activity', 0))
                waited = True
                if cancel is not None and cancel.is_set():
                    raise InterruptedError('replay cancelled while waiting for usage capacity')
                if self.usage_manager.pid is None:
                    raise RuntimeError('usage admission is full and has no running worker')
                now = time.monotonic()
                if current != previous:
                    previous, last_progress = current, now
                if now - last_progress >= timeout:
                    raise RuntimeError('UsageManager made no progress while usage admission was full')
                if cancel is not None:
                    cancel.wait(poll)
                else:
                    time.sleep(poll)
        finally:
            if waited:
                duration = time.monotonic() - started
                with self.usage_lock:
                    admission = self.usage_progress.setdefault(
                        'admission', dict(limit=self.USAGE_ADMISSION_LIMIT, waits=0, wait_real=0,
                                          max_wait_real=0))
                    admission['waits'] += 1
                    admission['wait_real'] += duration
                    admission['max_wait_real'] = max(admission['max_wait_real'], duration)

    def scale_workload(self, scale_info):
        name = scale_info['name']
        namespace = scale_info['namespace']
        replicas = scale_info['pods']
        kind = scale_info['kind'].lower()
        if kind in {"deployment", "statefulset"}:
            response = self.k8s_api.patch_namespaced_deployment_scale(
                name, namespace, {"spec": {"replicas": replicas}})
            self.observe_mutation((namespace, name), response, replicas)
            return response
        else:
            raise ValueError(f"Unsupported scale kind: {kind}")
