import copy
import json
import threading
from unittest.mock import Mock

import pytest
from kubernetes.client.rest import ApiException

from experiment_clock import ReplayDiagnostics
from infrastructure.manager import InfrastructureManager
from workload.manager import WorkloadManager
from workload.usage.usage_manager import UsageManager
from util.k8s_object_applier import KubernetesObjectApplier
from test_experiment_clock import FakeTime


@pytest.fixture(autouse=True)
def no_cluster(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('unit tests must not access Kubernetes')
    monkeypatch.setattr('kubernetes.config.load_kube_config', forbidden)


def manager(cls, entries, factor=1, phase='dynamic'):
    result = cls.__new__(cls)
    result.data = {'emulation': entries}
    result.speed_factor = factor
    result.emulation_phase = phase
    result.replay_diagnostics = ReplayDiagnostics(len(entries))
    result.k8s_api = Mock()
    result.k8s_object_applier = Mock()
    result.hpa = False
    result.create_namespace_if_not_exists = Mock()
    result.usage_lock = threading.RLock()
    result.usage_queue = Mock()
    result.check_usage = Mock()
    result.usage_progress = dict(enqueued=0, pending=0)
    return result


def node(name):
    return {'kind': 'Node', 'metadata': {'name': name}}


def deployment(name='app'):
    return {'kind': 'Deployment', 'metadata': {'name': name, 'namespace': 'ns'}}


@pytest.mark.parametrize('factor', [1, 2, 5, 1.5])
def test_managers_share_origin_even_with_different_start_times(factor):
    time = FakeTime()
    clock = time.clock(factor)
    infra = manager(InfrastructureManager, [{'timestamp': 10, 'applied_objects': [node('n')]}], factor)
    workload = manager(WorkloadManager, [{'timestamp': 10, 'applied_objects': {'ns': [deployment()]}}], factor)
    observed = []
    infra.k8s_object_applier.apply_node.side_effect = lambda *args: observed.append(time.now)
    workload.k8s_object_applier.apply_object.side_effect = lambda *args: observed.append(time.now)
    infra.emulation(clock)
    # Model a thread that only receives CPU after the other replay completes.
    workload.emulation(clock)
    assert observed == pytest.approx([100 + 10 / factor, 100 + 25 / factor])
    action = workload.replay_diagnostics.snapshot()['actions'][0]
    assert action['scheduled'] == 100 + 10 / factor
    assert workload.replay_diagnostics.snapshot()['max_lag_real'] == pytest.approx(15 / factor)
    assert clock.origin_monotonic == 100


@pytest.mark.parametrize('cls', [InfrastructureManager, WorkloadManager])
@pytest.mark.parametrize('factor', [1, 2, 5, 1.5])
@pytest.mark.parametrize('timestamps', [[], [0, 10, 10, 30]])
def test_static_modes_keep_duration_without_applying(cls, factor, timestamps):
    time = FakeTime()
    entries = [{'timestamp': ts} for ts in timestamps]
    instance = manager(cls, entries, factor, 'static')
    instance.emulation(time.clock(factor))
    assert time.now == pytest.approx(100 + ((timestamps[-1] if timestamps else 0) + 15) / factor)
    instance.k8s_api.assert_not_called()
    assert not instance.k8s_api.mock_calls
    assert not instance.k8s_object_applier.mock_calls
    stats = instance.replay_diagnostics.snapshot()
    assert stats['expected_entries'] == stats['processed_entries'] == len(entries)
    assert stats['not_applicable_entries'] == len(entries)
    assert stats['attempted_actions'] == 0


def test_workload_ties_failures_and_queue_metadata_preserve_inputs():
    time = FakeTime()
    entries = [
        {'timestamp': 4, 'applied_objects': {'ns': [deployment()]}},
        {'timestamp': 4, 'workload_actions': [{'action': 'set-usage', 'name': 'app', 'namespace': 'ns',
                                            'pods': {'p': {'cpu': .4, 'memory': 100}}}]},
        {'timestamp': 8, 'workload_actions': [{'action': 'scale', 'kind': 'deployment',
                                            'name': 'app', 'namespace': 'ns', 'pods': 2}]},
        {'timestamp': 9, 'deleted_objects': [deployment()]},
    ]
    original = copy.deepcopy(entries)
    workload = manager(WorkloadManager, entries, 2)
    def slow_failure(*args):
        time.wait(4)
        raise RuntimeError('patch failed')
    workload.k8s_object_applier.apply_object.side_effect = slow_failure
    workload.k8s_api.patch_namespaced_deployment_scale.side_effect = RuntimeError('scale failed')
    workload.emulation(time.clock(2))
    assert entries == original
    event = workload.usage_queue.put.call_args.args[0]
    assert event['pods'] == entries[1]['workload_actions'][0]['pods']
    assert event['replay_timing']['scheduled_monotonic'] == 102
    assert event['replay_timing']['origin_unix'] == 1700000000
    json.dumps(event)
    stats = workload.replay_diagnostics.snapshot()
    assert stats['attempted_actions'] == 4
    assert stats['completed_actions'] == stats['failed_actions'] == 2
    assert stats['late_events'] == 3
    assert time.now == 113.5  # Last event overdue at 106, then 7.5s tail.
    assert [a['context']['action'] for a in stats['actions']] == ['apply-object', 'queue-usage', 'scale', 'delete-object']


def test_infra_slow_actions_do_not_shift_deadlines_or_drop_events():
    time = FakeTime()
    instance = manager(InfrastructureManager, [
        {'timestamp': 2, 'applied_objects': [node('a'), node('b')]},
        {'timestamp': 3, 'deleted_objects': ['a']},
        {'timestamp': 20, 'applied_objects': [node('c')]},
    ], 2)
    seen = []
    def slow_apply(obj, name):
        seen.append((name, time.now))
        time.wait(3)
    instance.k8s_object_applier.apply_node.side_effect = slow_apply
    instance.k8s_api.delete_infrastructure_object.side_effect = RuntimeError('delete failed')
    instance.emulation(time.clock(2))
    assert seen == [('a', 101), ('b', 104), ('c', 110)]
    stats = instance.replay_diagnostics.snapshot()
    assert stats['failed_actions'] == 1
    assert stats['completed_actions'] == 3
    assert stats['processed_entries'] == 3
    assert time.now == 120.5


def test_hpa_omission_is_explicit_in_setup_and_replay():
    hpa = {'kind': 'HorizontalPodAutoscaler', 'metadata': {'name': 'h'}}
    workload = manager(WorkloadManager, [{'timestamp': 0,
        'applied_objects': {'ns': [hpa]}, 'deleted_objects': [hpa]}])
    workload.data['setup'] = {'applied_objects': {'ns': [hpa]}}
    workload.before_emulation()
    workload.emulation(FakeTime().clock())
    assert not workload.k8s_object_applier.mock_calls
    workload.create_namespace_if_not_exists.assert_not_called()
    stats = workload.replay_diagnostics.snapshot()
    assert stats['not_applicable_actions'] == 2
    assert stats['failed_actions'] == stats['attempted_actions'] == 0


def test_hpa_still_applied_at_one_when_enabled():
    hpa = {'kind': 'HorizontalPodAutoscaler', 'metadata': {'name': 'h'}}
    workload = manager(WorkloadManager, [])
    workload.hpa = True
    workload.BEFORE_EMULATION_KINDS = {'horizontalpodautoscaler'}
    workload.data['setup'] = {'applied_objects': {'ns': [hpa]}}
    workload.before_emulation()
    workload.k8s_object_applier.apply_object.assert_called_once_with(hpa)


def test_usage_interval_configured_before_process_start(tmp_path, monkeypatch):
    path = tmp_path / 'workload.json'
    path.write_text(json.dumps({'emulation': []}))
    monkeypatch.setattr('workload.manager.K8SAPI', Mock())
    monkeypatch.setattr('workload.manager.multiprocessing.Queue', Mock())
    instance = WorkloadManager(path, 'dynamic', False, 5)
    assert instance.usage_manager.reconcile_interval_seconds == 1
    assert instance.usage_manager.TIME_OUT == 200
    assert UsageManager(Mock()).reconcile_interval_seconds == 5


@pytest.mark.parametrize('kind', ['deployment', 'horizontalpodautoscaler', 'node'])
def test_applier_propagates_non_404_failure(kind):
    api = Mock()
    error = ApiException(status=503)
    api.read_namespaced_deployment.side_effect = error
    api.read_namespaced_horizontal_pod_autoscaler.side_effect = error
    api.patch_infrastructure_object.side_effect = error
    applier = KubernetesObjectApplier(api, hpa=True)
    with pytest.raises(ApiException):
        applier.apply_object({'kind': kind, 'metadata': {'name': 'x'}})


def test_coordinator_prepares_everything_before_fixing_common_origin(monkeypatch, tmp_path):
    import manager as coordinator
    from experiment_clock import ExperimentClock
    time = FakeTime()
    clock = ExperimentClock(2, time.monotonic, time.wait, lambda: 1700000000)
    monkeypatch.setattr(coordinator, 'ExperimentClock', lambda factor: clock)
    components = {}
    def component(name):
        obj = Mock()
        obj.usage_lock = __import__('threading').RLock()
        obj.data = {'emulation': []}
        obj.usage_progress = dict(enqueued=0, completed=0, failed=0, pending=0, errors=[],
            late_events=0, total_lag_real=0, total_lag_logical=0)
        obj.warnings = []
        obj.replay_diagnostics = ReplayDiagnostics(0)
        def prepare():
            assert clock.origin_monotonic is None
            time.wait(3)
        for method in ('before_setup', 'setup', 'before_emulation', 'start', 'run'):
            getattr(obj, method).side_effect = prepare
        components[name] = obj
        return obj
    for name in ('InfrastructureManager', 'WorkloadManager', 'ServiceMonitorManager', 'PodsMapping', 'Collector'):
        monkeypatch.setattr(coordinator, name, Mock(return_value=component(name)))
    commands = []
    def command(args, **kwargs):
        assert clock.origin_monotonic is None
        commands.append(args)
        time.wait(4)
    monkeypatch.setattr(coordinator.subprocess, 'check_output', lambda *a, **k: 'revision')
    monkeypatch.setattr(coordinator.subprocess, 'run', command)
    def popen(args, **kwargs):
        command(args, **kwargs)
        return Mock(wait=Mock(return_value=0))
    monkeypatch.setattr(coordinator.subprocess, 'Popen', popen)
    class Thread:
        def __init__(self, target, name, args=(), **kwargs):
            self.target, self.name, self.args = target, name, args
        def start(self):
            if self.name != 'CollectorThread':
                assert clock.origin_monotonic == time.now
                self.target(*self.args)
        def join(self, **kwargs):
            pass
        def is_alive(self):
            return False
    monkeypatch.setattr(coordinator.threading, 'Thread', Thread)
    from run_report import RunReport
    report = RunReport(tmp_path / 'out', monotonic=time.monotonic)
    for name in ('infrastructure_description.json', 'workload_description.json', 'pods_allocation.csv'):
        (tmp_path / name).write_text('{}')
    coordinator.PodsMapping.PODS_ALLOCATION_PATH = str(tmp_path / 'pods_allocation.csv')
    instance = coordinator.Manager(data_path=tmp_path, karpenter=False, hpa=False, speed_factor=2, report=report)
    instance.collection_diagnostics['complete'] = True
    def prepare_observability():
        assert clock.origin_monotonic is None
        time.wait(2)
    instance.prepare_observability = Mock(side_effect=prepare_observability)
    instance.run()
    instance.prepare_observability.assert_called_once()
    assert commands[-1] == ['bash', 'src/port-forward.sh']
    assert clock.origin_monotonic > 100
    for name in ('InfrastructureManager', 'WorkloadManager'):
        components[name].emulation.assert_called_once_with(clock)
    assert clock.origin_unix == 1700000000


def test_setup_omits_hpa_without_forwarding_to_applier():
    workload = manager(WorkloadManager, [])
    hpa = {'kind': 'HorizontalPodAutoscaler', 'metadata': {'name': 'h'}}
    obj = deployment()
    workload.BEFORE_EMULATION_KINDS = {'horizontalpodautoscaler'}
    workload.data['setup'] = {'applied_objects': {'ns': [hpa, obj]}, 'workload_actions': []}
    workload.usage_manager = Mock()
    workload.count_pods_in_namespaces = Mock(return_value=0)
    workload.wait_pods_ready = Mock()
    workload.setup()
    workload.k8s_object_applier.apply_object.assert_called_once_with(obj)


def test_usage_process_uses_serialized_interval(monkeypatch):
    import workload.usage.usage_manager as usage_module
    queue = Mock()
    queue.get.return_value = 'STOP'
    monkeypatch.setattr(usage_module, 'K8SAPI', Mock())
    usage = UsageManager(queue, reconcile_interval_seconds=2.5)
    monkeypatch.setattr(usage_module.time, 'monotonic', lambda: 100)
    usage.run()
    queue.get.assert_called_once_with(timeout=2.5)
    usage_module.K8SAPI.assert_called_once_with(timeout=200)


@pytest.mark.parametrize('kind', ['Deployment', 'Node', 'HorizontalPodAutoscaler', 'NodeClaim'])
def test_applier_404_creates_object(kind):
    api = Mock()
    error = ApiException(status=404)
    api.read_namespaced_deployment.side_effect = error
    api.read_namespaced_horizontal_pod_autoscaler.side_effect = error
    api.patch_infrastructure_object.side_effect = error
    applier = KubernetesObjectApplier(api, hpa=True)
    applier.apply_object({'kind': kind, 'metadata': {'name': 'x'}})
    creates = [call for call in api.mock_calls if call[0].startswith('create_')]
    assert len(creates) == 1


def test_usage_delivery_failure_is_not_counted_as_success():
    workload = manager(WorkloadManager, [{
        'timestamp': 0, 'workload_actions': [{'action': 'set-usage', 'name': 'x', 'namespace': 'ns', 'pods': {}}]
    }])
    workload.usage_queue.put.side_effect = RuntimeError('queue failed')
    workload.emulation(FakeTime().clock())
    stats = workload.replay_diagnostics.snapshot()
    assert stats['failed_actions'] == 1
    assert stats['completed_actions'] == 0
    assert stats['actions'][0]['error'] == 'queue failed'
