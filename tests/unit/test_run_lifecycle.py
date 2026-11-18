import json
import os
from pathlib import Path
import queue
import shutil
import subprocess
import sys
import threading
from unittest.mock import Mock
import zipfile

import pytest
from kubernetes.client.rest import ApiException

import main as entrypoint
import manager as coordinator
from collector import Collector, CollectionError
from experiment_clock import ExperimentClock, ReplayDiagnostics
from run_report import RunReport
from workload.manager import WorkloadManager
from workload.usage.usage_manager import UsageManager
from workload.usage.usage_tracking import APPLIED, UsageTracker


ROOT = Path(__file__).resolve().parents[2]
BASE_ARGS = ['trace', 'nodepool', 'karpenter-off', 'skip-tracer', 'dynamic', 'dynamic', 'hpa-off']


@pytest.fixture(autouse=True)
def no_external_access(monkeypatch):
    monkeypatch.setattr('kubernetes.config.load_kube_config', Mock(side_effect=AssertionError('cluster')))
    monkeypatch.setattr('requests.get', Mock(side_effect=AssertionError('network')))


@pytest.fixture
def lifecycle(tmp_path, monkeypatch):
    now = [100.0]
    report = RunReport(tmp_path / 'output', monotonic=lambda: now[0])
    # All subprocess commands in the coordinator are setup commands, never real.
    monkeypatch.setattr(coordinator.subprocess, 'run', Mock())
    result = coordinator.Manager.__new__(coordinator.Manager)
    result.report = report
    result.output_dir = str(report.output_dir)
    result.speed_factor = 2
    result.clock = ExperimentClock(2, lambda: now[0], lambda seconds: now.__setitem__(0, now[0] + seconds), lambda: 1000)
    result.clock.cancel = threading.Event()
    result.clock.defer_tail = True
    result.collection_stop = threading.Event()
    result.collection_error = None
    result.collection_diagnostics = dict(complete=False, errors=[])
    result.observability = {'workers': {'podPlayStageParallelism': 4}}
    result.service_monitor_manager = Mock()
    result.start_mapping_and_scheduler = Mock()
    result.prepare_observability = Mock(side_effect=lambda: now.__setitem__(0, now[0] + 4))
    result.collector = Collector(15)
    result.collector.warnings = set()
    def component():
        manager = Mock()
        manager.data = {'emulation': [{'timestamp': 20}]}
        manager.replay_diagnostics = ReplayDiagnostics(1)
        def replay(clock):
            assert clock is result.clock
            manager.replay_diagnostics.entry(0, 20, clock.deadline(20), clock.deadline(20), 2, True)
            now[0] = max(now[0], clock.deadline(20))
        manager.emulation.side_effect = replay
        return manager
    result.infrastructure_manager = component()
    result.workload_manager = component()
    result.workload_manager.usage_lock = threading.RLock()
    result.workload_manager.usage_progress = dict(enqueued=3, completed=3, failed=0, pending=0,
        errors=[], stopped=True, late_events=0, max_lag_real=0, max_lag_logical=0,
        total_lag_real=0, total_lag_logical=0, worst_event=None)
    def collect(origin):
        result.collection_stop.wait(2)
        result.collection_diagnostics['complete'] = True
        (report.output_dir / 'metric.csv').write_text('timestamp,value\n1000,1\n')
    result.collect_loop = collect
    return result, report, now


def test_success_reports_separate_durations_progress_and_archive(lifecycle):
    result, report, now = lifecycle
    result.run()
    data = json.loads(report.path.read_text())
    assert data['status'] == 'completed'
    assert data['timing']['preparation_real'] == 4
    assert data['timing']['replay_real'] == 10
    assert data['timing']['tail_real'] == 7.5
    assert data['timing']['finalization_real'] == 7.5
    assert data['timing']['total_real'] == 21.5
    assert data['timing']['effective_factor'] == 2
    assert data['timing']['origin_monotonic'] == 104
    assert data['timing']['origin_unix'] == 1000
    assert data['progress']['workload']['processed_entries'] == 1
    assert data['progress']['usage']['pending'] == 0
    result.workload_manager.finish_usage.assert_called_once()
    with zipfile.ZipFile(result.output_dir + '.zip') as archive:
        assert {'run.json', 'metric.csv'} == set(archive.namelist())
        assert json.loads(archive.read('run.json')) == data


@pytest.mark.parametrize('component', ['infrastructure', 'workload'])
def test_replay_thread_exceptions_reach_report(lifecycle, component):
    result, report, now = lifecycle
    getattr(result, component + '_manager').emulation.side_effect = RuntimeError(component + ' exploded')
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    data = json.loads(report.path.read_text())
    assert data['status'] == 'failed'
    assert any(error['message'] == component + ' exploded' and error['component'] == component
               for error in data['errors'])
    assert data['timing']['effective_factor'] is None
    assert not data['timing']['replay_complete']
    result.workload_manager.tear_down.assert_called_once()


def test_lag_is_warning_without_event_loss(lifecycle):
    result, report, now = lifecycle
    result.infrastructure_manager.replay_diagnostics.entry(0, 20, 114, 116, 2, True)
    result.infrastructure_manager.emulation.side_effect = None
    result.run()
    assert report.data['status'] == 'completed_with_warnings'
    assert report.data['progress']['infrastructure']['processed_entries'] == 1
    assert report.data['lag']['infrastructure']['max_lag_real'] == 2
    assert report.data['lag']['infrastructure']['max_lag_logical'] == 4


@pytest.mark.parametrize('phase', ['preparation', 'collection', 'teardown', 'archive', 'usage'])
def test_failure_stages_preserve_primary_diagnosis(lifecycle, phase):
    result, report, now = lifecycle
    if phase == 'preparation':
        result.workload_manager.setup.side_effect = RuntimeError('primary')
    elif phase == 'collection':
        def collect(origin):
            result.collection_error = CollectionError('primary')
            result.collection_diagnostics['errors'].append('primary')
        result.collect_loop = collect
    elif phase == 'teardown':
        result.workload_manager.tear_down.side_effect = RuntimeError('primary')
    elif phase == 'archive':
        result.collector.archive = Mock(side_effect=OSError('primary'))
    else:
        result.workload_manager.check_usage.side_effect = RuntimeError('UsageManager exited with code 9: primary')
    result.infrastructure_manager.tear_down.side_effect = RuntimeError('secondary cleanup')
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    assert report.data['status'] == 'failed'
    assert any('primary' in error['message'] for error in report.data['errors'])
    assert any('secondary cleanup' in error['message'] for error in report.data['errors'])
    if phase == 'preparation':
        assert report.data['timing']['replay_real'] is None


def test_usage_action_failure_causes_failed_run(lifecycle):
    result, report, now = lifecycle
    result.workload_manager.usage_progress.update(completed=2, failed=1,
        errors=[dict(kind='ack', completed=False, message='CRU denied', resource='app')])
    with pytest.raises(RuntimeError):
        result.run()
    assert report.data['progress']['usage']['failed'] == 1
    assert any(error['message'] == 'CRU denied' for error in report.data['errors'])


def test_optional_missing_metrics_warn(lifecycle):
    result, report, now = lifecycle
    result.collector.warnings.add('optional metric absent: vpa')
    result.run()
    assert report.data['status'] == 'completed_with_warnings'


def test_interrupt_runs_partial_cleanup(lifecycle):
    result, report, now = lifecycle
    result.workload_manager.setup.side_effect = KeyboardInterrupt('interrupted')
    result.workload_manager.tear_down.side_effect = RuntimeError('cleanup')
    with pytest.raises(RuntimeError, match='interrupted'):
        result.run()
    assert report.data['status'] == 'interrupted'
    assert report.data['timing']['origin_unix'] is None
    assert [error['message'] for error in report.data['errors']][:2] == ['interrupted', 'cleanup']


def usage_ack(event_id, outcome=APPLIED, **fields):
    return dict(kind='ack', event_id=event_id, outcome=outcome,
                completed=outcome in ('applied', 'empty', 'target_absent'), **fields)


def usage_parent(messages=(), exitcode=None, events=2, monotonic=None):
    parent = WorkloadManager.__new__(WorkloadManager)
    parent.usage_tracker = UsageTracker(monotonic)
    for event_id in range(1, events + 1):
        parent.usage_tracker.register(dict(namespace='ns', name='app', event_id=event_id))
    parent.usage_lock = threading.RLock()
    parent.usage_admission_lock = threading.Lock()
    parent.usage_status = queue.Queue()
    for message in messages:
        parent.usage_status.put(message)
    parent.usage_queue = Mock()
    parent.usage_stop_sent = False
    parent.usage_manager = Mock(pid=1, exitcode=exitcode)
    parent.usage_manager.is_alive.return_value = exitcode is None
    parent.usage_progress = dict(enqueued=events, completed=0, failed=0, pending=events,
        stopped=False, errors=[], late_events=0, max_lag_real=0, max_lag_logical=0,
        total_lag_real=0, total_lag_logical=0, worst_event=None)
    return parent


def test_worker_exitcode_and_missing_ack_are_failures():
    parent = usage_parent(exitcode=7)
    with pytest.raises(RuntimeError, match='code 7'):
        parent.finish_usage()
    parent = usage_parent(exitcode=0)
    with pytest.raises(RuntimeError, match='acknowledging'):
        parent.finish_usage()
    assert parent.usage_progress['pending'] == 2


def test_worker_acknowledges_actual_reconciliation_and_fifo_stop():
    events, statuses = queue.Queue(), queue.Queue()
    events.put(dict(namespace='ns', name='app', pods={}, event_id=1, replay_timing={
        'scheduled_monotonic': 10, 'speed_factor': 2, 'trace_timestamp': 20}))
    events.put('STOP')
    worker = UsageManager(events, status_queue=statuses)
    worker.handle_set_usage_event = Mock(return_value=APPLIED)
    worker.process_events(monotonic=lambda: 12)
    started, ack, concurrency, stopped = (statuses.get_nowait() for _ in range(4))
    assert concurrency['kind'] == 'concurrency' and concurrency['stats']['event_workers'] == 1
    assert started['kind'] == 'started' and started['event_id'] == 1
    assert ack['completed'] and ack['lag_real'] == 2 and ack['lag_logical'] == 4
    assert ack['event_id'] == 1 and ack['outcome'] == APPLIED
    assert stopped == {'kind': 'stopped'}
    failed = usage_ack(2, 'failed', message='second event failed')
    parent = usage_parent([started, ack, failed, stopped], exitcode=0)
    parent.finish_usage()
    assert parent.usage_progress['completed'] == 1
    assert parent.usage_progress['failed'] == 1
    assert parent.usage_progress['pending'] == 0


def test_usage_cru_error_is_failed_ack_and_does_not_drop_next_event():
    events, statuses = queue.Queue(), queue.Queue()
    events.put(dict(namespace='ns', name='bad', event_id=1))
    events.put(dict(namespace='ns', name='good', event_id=2))
    events.put('STOP')
    worker = UsageManager(events, status_queue=statuses)
    worker.k8s_api = Mock()
    worker.k8s_api.create_infrastructure_object.side_effect = ApiException(status=403)
    worker.handle_set_usage_event = Mock(side_effect=[RuntimeError('CRU failed'), APPLIED])
    with pytest.raises(RuntimeError, match='CRU example'):
        worker.apply_cru('example', {})
    activity = statuses.get_nowait()
    assert activity['kind'] == 'activity'
    assert activity['stats']['failures'] == 1
    worker.process_events(monotonic=lambda: 12)
    acks = [message for message in iter(statuses.get_nowait, {'kind': 'stopped'})
            if message['kind'] == 'ack']
    assert [(ack['event_id'], ack['completed'], ack['outcome']) for ack in acks] == [
        (1, False, 'failed'), (2, True, APPLIED)]


def test_worker_fatal_initialization_is_sent_to_parent(monkeypatch):
    statuses = queue.Queue()
    worker = UsageManager(queue.Queue(), status_queue=statuses)
    monkeypatch.setattr('workload.usage.usage_manager.K8SAPI', Mock(side_effect=RuntimeError('init failure')))
    with pytest.raises(RuntimeError, match='init failure'):
        worker.run()
    assert statuses.get_nowait() == dict(kind='fatal', message='init failure')


def test_worker_timeout_kills_child_without_queue_join():
    parent = usage_parent()
    parent.finish_usage = Mock(side_effect=RuntimeError('deadline'))
    with pytest.raises(RuntimeError, match='deadline'):
        parent.tear_down()
    parent.usage_manager.terminate.assert_called_once()
    parent.usage_manager.kill.assert_called_once()
    parent.usage_queue.cancel_join_thread.assert_called_once()


def test_tracer_failure_is_finalized_before_manager_starts(tmp_path, monkeypatch):
    monkeypatch.setattr(entrypoint.Main, 'apply_nodepool', Mock())
    monkeypatch.setattr(entrypoint.Main, 'run_tracer', Mock(side_effect=RuntimeError('bad tracer')))
    manager = Mock()
    monkeypatch.setattr(entrypoint.Main, 'run_manager', manager)
    output = tmp_path / 'out'
    assert entrypoint.main(BASE_ARGS + ['--output-dir', str(output)]) == 1
    data = json.loads((output / 'run.json').read_text())
    assert data['status'] == 'failed'
    assert data['errors'][0]['message'] == 'bad tracer'
    manager.assert_not_called()


def test_setup_failure_before_main_is_reported_by_real_shell(tmp_path):
    (tmp_path / 'src').mkdir()
    (tmp_path / 'kwok-karpenter-install').mkdir()
    for name in ('run_report.py', 'experiment_clock.py'):
        shutil.copy(ROOT / 'src' / name, tmp_path / 'src' / name)
    shutil.copy(ROOT / 'execute-emulation.sh', tmp_path / 'execute-emulation.sh')
    setup = tmp_path / 'kwok-karpenter-install/setup.sh'
    setup.write_text('#!/bin/bash\nexit 23\n')
    setup.chmod(0o755)
    output = tmp_path / 'output with spaces'
    env = dict(os.environ, PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'])
    result = subprocess.run(['bash', 'execute-emulation.sh', '--sim', '--data-path', 'unused',
                             '--output-dir', str(output)], cwd=tmp_path, env=env,
                            capture_output=True, text=True, timeout=10)
    assert result.returncode == 23, result.stdout + result.stderr
    data = json.loads((output / 'run.json').read_text())
    assert data['status'] == 'failed'
    assert data['phases']['external_setup']['result'] == 'failed'
    assert data['phases']['external_setup']['failed'] == 1
    assert data['phases']['external_setup']['duration_real'] >= 0
    assert data['errors'][0]['stage'] == 'setup'
    assert '23' in data['errors'][0]['message']
    assert data['timing']['replay_real'] is None


def test_uncaught_collector_thread_exception_is_captured(lifecycle):
    result, report, now = lifecycle
    result.collect_loop = Mock(side_effect=RuntimeError('collector exploded'))
    with pytest.raises(RuntimeError):
        result.run()
    assert report.data['status'] == 'failed'
    assert any(error['message'] == 'collector exploded' and error['stage'] == 'collection'
               for error in report.data['errors'])


def test_failed_application_has_context_and_no_completed_factor(lifecycle):
    result, report, now = lifecycle
    def replay(clock):
        diagnostics = result.workload_manager.replay_diagnostics
        diagnostics.apply(clock, clock.deadline(20),
            {'action': 'apply', 'object': {'kind': 'Deployment', 'metadata': {'name': 'app'},
                                         'spec': {'secret': 'must not enter report'}}},
            Mock(side_effect=RuntimeError('forbidden')))
        diagnostics.entry(0, 20, clock.deadline(20), clock.deadline(20), 2, True)
    result.workload_manager.emulation.side_effect = replay
    with pytest.raises(RuntimeError):
        result.run()
    assert report.data['timing']['effective_factor'] is None
    assert not report.data['timing']['replay_complete']
    assert report.data['progress']['workload']['failed_actions'] == 1
    assert 'must not enter report' not in report.path.read_text()
    assert any(error.get('action', {}).get('name') == 'app' for error in report.data['errors'])


def test_live_snapshot_does_not_duplicate_final_errors(lifecycle):
    result, report, now = lifecycle
    result.infrastructure_manager.replay_diagnostics.entry(0, 20, 110, 111, 2, True)
    result.clock.start()
    result.snapshot(final=False)
    result.snapshot(final=False)
    assert report.data['progress']['infrastructure']['processed_entries'] == 1
    assert report.data['warning_count'] == 0
    assert report.data['timing']['progress_logical'] == 20
    result.snapshot()
    assert report.data['warning_count'] == 1


def test_archive_compression_is_included_in_finalization(lifecycle, monkeypatch):
    result, report, now = lifecycle
    write = zipfile.ZipFile.write
    def slow_write(archive, filename, *args, **kwargs):
        if str(filename).endswith('.csv'):
            now[0] += 3
        return write(archive, filename, *args, **kwargs)
    monkeypatch.setattr(zipfile.ZipFile, 'write', slow_write)
    result.run()
    assert report.data['timing']['finalization_real'] == 10.5
    assert report.data['timing']['total_real'] == 24.5
    with zipfile.ZipFile(result.output_dir + '.zip') as archive:
        assert json.loads(archive.read('run.json')) == report.data


def test_service_monitor_failures_reported_but_delete_404_is_expected():
    from infrastructure.service_monitor.service_monitor_manager import ServiceMonitorManager
    monitor = ServiceMonitorManager.__new__(ServiceMonitorManager)
    monitor.k8s_api = Mock()
    monitor.issue_handler = Mock()
    monitor.k8s_api.delete_namespaced_custom_object.side_effect = ApiException(status=404)
    monitor._on_node_deleted('gone')
    monitor.issue_handler.assert_not_called()
    monitor.k8s_api.delete_namespaced_custom_object.side_effect = ApiException(status=403)
    monitor._on_node_deleted('forbidden')
    assert monitor.issue_handler.call_args.kwargs['resource'] == 'forbidden'


@pytest.mark.parametrize('phase', ['teardown', 'archive'])
def test_cleanup_interruption_changes_status(lifecycle, phase):
    result, report, now = lifecycle
    if phase == 'teardown':
        result.workload_manager.tear_down.side_effect = KeyboardInterrupt('stop during cleanup')
    else:
        result.collector.archive = Mock(side_effect=KeyboardInterrupt('stop during archive'))
    with pytest.raises(RuntimeError, match='interrupted'):
        result.run()
    assert json.loads(report.path.read_text())['status'] == 'interrupted'


def test_shell_signal_waits_for_python_report_owner(tmp_path):
    import signal
    import time
    (tmp_path / 'src').mkdir()
    (tmp_path / 'kwok-karpenter-install').mkdir()
    for name in ('run_report.py', 'experiment_clock.py'):
        shutil.copy(ROOT / 'src' / name, tmp_path / 'src' / name)
    shutil.copy(ROOT / 'execute-emulation.sh', tmp_path / 'execute-emulation.sh')
    setup = tmp_path / 'kwok-karpenter-install/setup.sh'
    setup.write_text('#!/bin/bash\nexit 0\n')
    setup.chmod(0o755)
    # A real isolated subprocess verifies signal forwarding and writer ownership;
    # the application and all cluster commands are replaced.
    (tmp_path / 'src/main.py').write_text('''import signal, sys
from run_report import RunReport
report = RunReport(sys.argv[sys.argv.index('--output-dir') + 1], resume=True)
report.stage('replay')
def stop(signum, frame):
    report.issue('specific Python interruption', component='replay')
    report.finish(interrupted=True)
    (report.output_dir / 'owner-finished').touch()
    raise SystemExit(130)
signal.signal(signal.SIGTERM, stop)
(report.output_dir / 'owner-ready').touch()
while True:
    signal.pause()
''')
    output = tmp_path / 'output'
    env = dict(os.environ, PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'])
    process = subprocess.Popen(['bash', 'execute-emulation.sh', '--sim', '--data-path', 'unused',
                                '--output-dir', str(output)], cwd=tmp_path, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 5
        while not (output / 'owner-ready').exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.01)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 143, stdout + stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    data = json.loads((output / 'run.json').read_text())
    assert data['status'] == 'interrupted'
    assert data['error_count'] == 1
    assert data['errors'][0]['message'] == 'specific Python interruption'
    assert (output / 'owner-finished').exists()


def test_usage_drain_deadline_does_not_wait_on_dead_queue():
    parent = usage_parent()
    with pytest.raises(RuntimeError, match='real cleanup deadline'):
        parent.finish_usage(timeout=0)
    parent.usage_queue.put.assert_called_once_with('STOP')
    assert parent.usage_progress['pending'] == 2


def test_lifecycle_phase_times_and_archive_snapshot(lifecycle, monkeypatch):
    result, report, now = lifecycle
    write = zipfile.ZipFile.write
    def slow_write(archive, filename, *args, **kwargs):
        if str(filename).endswith('.csv'):
            now[0] += 3
        return write(archive, filename, *args, **kwargs)
    monkeypatch.setattr(zipfile.ZipFile, 'write', slow_write)
    result.run()
    phases = report.data['phases']
    assert phases['preparation']['duration_real'] == 4
    assert phases['metrics_ready']['duration_real'] == 4
    assert phases['metrics_ready']['parent'] == 'preparation'
    assert phases['initial_usage_drain']['completed'] == 3
    assert phases['final_collection']['duration_real'] == 7.5
    assert phases['archive']['duration_real'] == 3
    assert phases['archive']['result'] == 'recorded_before_close'
    assert report.data['timing']['effective_factor'] == 2
    with zipfile.ZipFile(result.output_dir + '.zip') as archive:
        assert json.loads(archive.read('run.json')) == report.data


def test_unstarted_collection_absent_after_setup_interruption(lifecycle):
    result, report, now = lifecycle
    def stop():
        now[0] += 2
        raise KeyboardInterrupt('stop')
    result.workload_manager.setup.side_effect = stop
    with pytest.raises(RuntimeError, match='interrupted'):
        result.run()
    assert report.data['phases']['workloads']['result'] == 'interrupted'
    assert report.data['phases']['workloads']['duration_real'] == 2
    assert 'final_collection' not in report.data['phases']
    assert 'metrics_ready' not in report.data['phases']


def test_setup_creation_timeout_stops_before_usage(tmp_path):
    report = RunReport(tmp_path / 'out')
    parent = WorkloadManager.__new__(WorkloadManager)
    parent.report = report
    parent.hpa = False
    parent.BEFORE_EMULATION_KINDS = {'horizontalpodautoscaler'}
    parent.usage_manager = Mock()
    parent.k8s_api = Mock()
    parent.k8s_api.list_pod_for_all_namespaces.return_value.items = []
    parent.k8s_object_applier = Mock()
    parent.create_namespace_if_not_exists = Mock()
    parent.set_workload_usage = Mock()
    parent.CREATION_TIMEOUT = 0
    parent.data = {'setup': {'applied_objects': {'ns': [
        {'kind': 'Deployment', 'metadata': {'namespace': 'ns', 'name': 'app'}, 'spec': {'replicas': 14}},
        {'kind': 'HorizontalPodAutoscaler'}]},
        'workload_actions': [{'action': 'set-usage'}]}}
    with pytest.raises(TimeoutError, match='app'):
        parent.setup()
    phases = report.data['phases']
    assert phases['workloads_apply']['expected'] == 1
    assert phases['workloads_apply']['completed'] == 1
    assert phases['workloads_apply']['concurrency'] == 1
    assert phases['workloads_converge']['result'] == 'failed'
    assert phases['workloads_converge']['expected'] == 14
    assert 'initial_usage' not in phases
    parent.set_workload_usage.assert_not_called()
    parent.create_namespace_if_not_exists.assert_called_once_with('ns')


def test_node_setup_failure_retains_applied_count(tmp_path):
    from infrastructure.manager import InfrastructureManager
    parent = InfrastructureManager.__new__(InfrastructureManager)
    parent.report = RunReport(tmp_path / 'out')
    parent.data = {'setup': [{'metadata': {'name': 'a'}}, {'metadata': {'name': 'b'}}]}
    parent.k8s_object_applier = Mock()
    parent.k8s_object_applier.apply_object.side_effect = [None, RuntimeError('denied')]
    with pytest.raises(RuntimeError, match='denied'):
        parent.setup()
    phase = parent.report.data['phases']['nodes_apply']
    assert phase['expected'] == 2
    assert phase['completed'] == 1
    assert phase['failed'] == 1
    assert 'nodes_converge' not in parent.report.data['phases']


def test_shell_setup_interrupt_imports_partial_child_phase(tmp_path):
    import signal
    import time
    (tmp_path / 'src').mkdir()
    (tmp_path / 'kwok-karpenter-install').mkdir()
    for name in ('run_report.py', 'experiment_clock.py'):
        shutil.copy(ROOT / 'src' / name, tmp_path / 'src' / name)
    shutil.copy(ROOT / 'execute-emulation.sh', tmp_path / 'execute-emulation.sh')
    setup = tmp_path / 'kwok-karpenter-install/setup.sh'
    setup.write_text('''#!/bin/bash
python3 ../src/run_report.py measurement --measurements "$KLUE_SETUP_MEASUREMENTS" --phase install_tools --event begin
touch ready
while true; do sleep .05; done
''')
    setup.chmod(0o755)
    output = tmp_path / 'output'
    env = dict(os.environ, PATH=str(Path(sys.executable).parent) + os.pathsep + os.environ['PATH'])
    process = subprocess.Popen(['bash', 'execute-emulation.sh', '--sim', '--data-path', 'unused',
                                '--output-dir', str(output)], cwd=tmp_path, env=env,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    try:
        deadline = time.monotonic() + 5
        while not (setup.parent / 'ready').exists():
            assert process.poll() is None
            assert time.monotonic() < deadline
            time.sleep(.01)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=5)
        assert process.returncode == 143, stdout + stderr
        data = json.loads((output / 'run.json').read_text())
        assert data['status'] == 'interrupted'
        for name in ('external_setup', 'install_tools'):
            assert data['phases'][name]['result'] == 'interrupted'
            assert data['phases'][name]['duration_real'] >= 0
        assert 'preparation' not in data['phases']
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate(timeout=5)


@pytest.mark.parametrize('failure', ['mapping', 'scheduler'])
def test_initial_placement_failure_never_starts_replay(lifecycle, monkeypatch, failure):
    result, report, _ = lifecycle
    result.start_mapping_and_scheduler = coordinator.Manager.start_mapping_and_scheduler.__get__(result)
    result.pods_mapping = Mock()
    popen = Mock()
    monkeypatch.setattr(coordinator.subprocess, 'Popen', popen)
    if failure == 'mapping':
        result.pods_mapping.run.side_effect = ValueError('incomplete allocation')
    else:
        process = Mock(pid=12345, args=['scheduler'])
        process.wait.return_value = 23
        popen.return_value = process
        monkeypatch.setattr(coordinator.os, 'killpg', Mock())
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    assert result.clock.origin_monotonic is None
    assert result.clock.origin_unix is None
    result.infrastructure_manager.emulation.assert_not_called()
    result.workload_manager.emulation.assert_not_called()
    result.prepare_observability.assert_not_called()
    result.workload_manager.tear_down.assert_called_once()
    assert report.data['status'] == 'failed'
    assert report.data['phases']['preparation']['result'] == 'failed'
    if failure == 'mapping':
        popen.assert_not_called()


@pytest.mark.parametrize('barrier', ['creation', 'operational', 'usage', 'metrics'])
def test_setup_barrier_failure_keeps_origin_unset_and_cleans_up(lifecycle, barrier):
    result, report, _ = lifecycle
    if barrier == 'creation':
        result.workload_manager.setup.side_effect = TimeoutError('missing creation owner ns/app')
    elif barrier == 'operational':
        result.workload_manager.wait_pods_operational.side_effect = TimeoutError('pod ns/p not Ready')
    elif barrier == 'usage':
        result.workload_manager.usage_progress.update(failed=1, completed=2)
    else:
        result.prepare_observability.side_effect = TimeoutError('metrics freshness')
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    assert result.clock.origin_monotonic is None
    assert result.clock.origin_unix is None
    result.workload_manager.emulation.assert_not_called()
    result.infrastructure_manager.emulation.assert_not_called()
    result.workload_manager.tear_down.assert_called_once()
    result.service_monitor_manager.stop.assert_called_once()


def test_origin_waits_for_operational_usage_and_metrics_confirmations(lifecycle):
    result, _, _ = lifecycle
    confirmations = []
    def confirm(kind):
        assert result.clock.origin_monotonic is None
        assert result.clock.origin_unix is None
        confirmations.append(kind)
    result.workload_manager.wait_pods_operational.side_effect = lambda: confirm('pods')
    result.workload_manager.drain_usage.side_effect = lambda **kwargs: confirm('usage') if result.clock.origin_monotonic is None else None
    result.prepare_observability.side_effect = lambda: confirm('metrics')
    result.run()
    assert confirmations == ['pods', 'metrics', 'usage']
    assert result.clock.origin_monotonic is not None


def final_drain(result, now, seconds, error=None):
    def drain(**kwargs):
        if result.clock.origin_monotonic is None:
            return
        now[0] += seconds
        if error is not None:
            raise error
    result.workload_manager.drain_usage.side_effect = drain


def test_producers_and_final_usage_wait_are_measured_separately(lifecycle):
    result, report, now = lifecycle
    final_drain(result, now, 3)
    tracker = UsageTracker(lambda: now[0])
    result.workload_manager.usage_progress.update(tracker.summary())
    result.run()
    timing = json.loads(report.path.read_text())['timing']
    assert timing['producers_real'] == 10
    assert timing['usage_drain_real'] == 3
    # replay_real stays inclusive of the final usage wait.
    assert timing['replay_real'] == 13
    data = json.loads(report.path.read_text())
    assert data['lag']['usage']['completion_lag']['max_real'] == 0
    assert data['progress']['usage']['outcomes']['applied'] == 0
    assert data['progress']['usage']['acknowledgements']['pending'] == 0


def test_interruption_during_final_usage_wait_keeps_partial_measurements(lifecycle):
    result, report, now = lifecycle
    final_drain(result, now, 2, KeyboardInterrupt('stop requested'))
    with pytest.raises(RuntimeError, match='interrupted'):
        result.run()
    data = json.loads(report.path.read_text())
    assert data['status'] == 'interrupted'
    assert data['timing']['producers_real'] == 10
    assert data['timing']['usage_drain_real'] == 2
    assert data['timing']['replay_real'] == 12
    assert not data['timing']['replay_complete']
    result.workload_manager.tear_down.assert_called_once()


def test_absent_usage_targets_are_reported_without_counting_as_applied(lifecycle):
    result, report, now = lifecycle
    result.workload_manager.usage_progress.update(outcomes=dict(
        applied=2, empty=0, target_absent=1, failed=0, cancelled=0))
    result.run()
    assert report.data['status'] == 'completed_with_warnings'
    assert any('no target workload' in warning['message'] for warning in report.data['warnings'])
    assert report.data['progress']['usage']['outcomes']['target_absent'] == 1


def test_protocol_error_fails_the_run(lifecycle):
    result, report, now = lifecycle
    result.workload_manager.usage_progress['errors'].append(dict(
        kind='protocol', message='usage acknowledgement protocol error (duplicate): event 1', event_id=1))
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    assert any('protocol error' in error['message'] and error['event_id'] == 1
               for error in report.data['errors'])


def coordinated(result, now, failure=None):
    """Replace the two producers by a coordinator reporting usage barriers."""
    from replay_coordinator import ReplayCoordinator
    coordinator = ReplayCoordinator(result.infrastructure_manager, result.workload_manager)
    def run(clock):
        coordinator.stats['barrier_wait_real'] = 3
        for manager in (result.infrastructure_manager, result.workload_manager):
            manager.replay_diagnostics.entry(0, 20, clock.deadline(20), clock.deadline(20), 2, True)
        now[0] = max(now[0], clock.deadline(20))
        if failure is not None:
            result.workload_manager.usage_progress['fidelity'] = dict(
                limit_logical=30, violation=failure.details)
            raise failure
    coordinator.run = run
    result.replay_coordinator = coordinator
    result.workload_manager.usage_progress['fidelity'] = dict(limit_logical=30, violation=None)
    return coordinator


def test_coordinated_replay_reports_barriers_and_temporal_validity(lifecycle):
    result, report, now = lifecycle
    coordinated(result, now)
    result.run()
    data = json.loads(report.path.read_text())
    assert data['status'] == 'completed'
    assert data['timing']['replay_complete'] is True
    assert data['timing']['usage_barrier_real'] == 3
    assert data['fidelity'] == dict(limit_logical=30, violation=None, temporal_valid=True)
    assert data['progress']['coordination']['limit_logical'] == 30
    for manager in (result.infrastructure_manager, result.workload_manager):
        manager.emulation.assert_not_called()


def test_fidelity_violation_fails_run_with_primary_error_and_finishes_cleanup(lifecycle):
    from workload.usage.usage_tracking import UsageTracker
    result, report, now = lifecycle
    tracker = UsageTracker(lambda: 200, limit_logical=30)
    tracker.register(dict(namespace='ns', name='app', event_id=1, replay_timing=dict(
        scheduled_monotonic=100, speed_factor=2, trace_timestamp=20)))
    tracker.check_deadlines()
    coordinated(result, now, failure=tracker.violation_error())
    with pytest.raises(RuntimeError, match='failed'):
        result.run()
    data = json.loads(report.path.read_text())
    assert data['status'] == 'failed'
    assert data['timing']['replay_complete'] is False
    assert data['timing']['effective_factor'] is None
    primary = data['errors'][0]
    assert primary['message'].startswith('temporal fidelity violated')
    assert primary['component'] == 'replay'
    assert (primary['event_id'], primary['trace_timestamp'], primary['limit_logical']) == (1, 20, 30)
    assert primary['delay_logical'] == 200
    assert data['fidelity']['temporal_valid'] is False
    assert data['fidelity']['violation']['event_id'] == 1
    result.workload_manager.tear_down.assert_called_once()
    assert (Path(result.output_dir + '.zip')).exists()


def test_cancelled_usage_is_never_a_complete_replay(lifecycle):
    result, report, now = lifecycle
    coordinated(result, now)
    result.workload_manager.usage_progress['outcomes'] = dict(applied=2, cancelled=1)
    result.run()
    assert report.data['timing']['replay_complete'] is False
    assert report.data['fidelity']['temporal_valid'] is False
