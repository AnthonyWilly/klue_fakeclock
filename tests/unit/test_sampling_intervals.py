import copy
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from kubernetes.client.rest import ApiException

from experiment_clock import validate_speed_factor
from sampling import SamplingIntervals, configure_ksm, duration, parse_duration
from util.k8s_objects.servicemonitor_generator import ServiceMonitorGenerator
from workload.usage.usage_manager import UsageManager
from infrastructure.service_monitor.service_monitor_manager import ServiceMonitorManager


@pytest.mark.parametrize('factor,kwok,query,reconcile', [(1, 15, 30, 5), (2, 7.5, 15, 2.5), (5, 3, 6, 1), (1.5, 10, 20, 5/1.5), (8, 1.875, 3.75, .625), (15000/7, .007, .014, 5/(15000/7))])
def test_intervals_and_timeout(factor, kwok, query, reconcile):
    intervals = SamplingIntervals(factor)
    assert (intervals.kwok, intervals.query, intervals.reconcile) == (kwok, query, reconcile)
    endpoint = ServiceMonitorGenerator(intervals.kwok).generate_service_monitor('n')['spec']['endpoints'][0]
    assert parse_duration(endpoint['interval']) == kwok
    assert parse_duration(endpoint['scrapeTimeout']) <= kwok
    assert parse_duration(duration(query)) == query


@pytest.mark.parametrize('factor', [7, 5001, float('inf')])
def test_unrepresentable_factor_rejected(factor):
    with pytest.raises(ValueError):
        validate_speed_factor(factor)


def test_ksm_overlay_preserves_self_endpoint_authentication_and_input():
    monitor = {'spec': {'endpoints': [dict(port='https-main', interval='30s', scrapeTimeout='30s', tlsConfig={'x': 1}),
                                     dict(port='https-self', interval='30s')], 'selector': {'app': 'ksm'}}}
    before = copy.deepcopy(monitor)
    result = configure_ksm(monitor, 3.75)
    assert result['spec']['endpoints'][0] == dict(port='https-main', interval='3750ms', scrapeTimeout='3750ms', tlsConfig={'x': 1})
    assert result['spec']['endpoints'][1] == before['spec']['endpoints'][1]
    assert monitor == before
    with pytest.raises(ValueError):
        configure_ksm({'spec': {'endpoints': []}}, 3)


def test_continuous_queue_does_not_starve_periodic_reconciliation():
    now = [100.0]
    seen, ticks = [], []
    events = iter({'id': index} for index in range(12))
    queue = Mock()
    def get(timeout):
        event = next(events, 'STOP')
        now[0] += .4
        return event
    queue.get.side_effect = get
    usage = UsageManager(queue, 1)
    usage.handle_set_usage_event = lambda event: seen.append(event)
    usage.reconcile_tracked_workloads = lambda: ticks.append(now[0])
    usage.process_events(lambda: now[0])
    assert seen == [{'id': index} for index in range(12)]
    assert ticks == pytest.approx([101.2, 102, 103.2, 104])
    assert all(0 <= call.kwargs['timeout'] <= 1 for call in queue.get.call_args_list)


def test_idle_queue_and_slow_reconciliation_keep_absolute_grid():
    import queue as queues
    now = [0.0]
    ticks = []
    queue = Mock()
    def get(timeout):
        now[0] += timeout
        if len(ticks) == 2:
            return 'STOP'
        raise queues.Empty
    queue.get.side_effect = get
    usage = UsageManager(queue, 1)
    def reconcile():
        ticks.append(now[0])
        now[0] += 1.2
    usage.reconcile_tracked_workloads = reconcile
    usage.process_events(lambda: now[0])
    assert ticks == [1, 3]


def test_watch_relist_410_retains_scaled_interval(monkeypatch):
    import infrastructure.service_monitor.service_monitor_manager as module
    monkeypatch.setattr(module, 'K8SAPI', Mock())
    manager = ServiceMonitorManager(7.5)
    manager._stop_event.wait = Mock()
    def node(name):
        return NS(metadata=NS(name=name, resource_version='2', annotations={'kwok.x-k8s.io/node': 'fake'}))
    manager.k8s_api.v1.list_node.side_effect = [
        NS(items=[node('a')], metadata=NS(resource_version='1')),
        NS(items=[node('a'), node('b')], metadata=NS(resource_version='3'))]
    watch = Mock()
    def stream(*args, **kwargs):
        if kwargs['resource_version'] == '1':
            raise ApiException(status=410)
        manager._stop_event.set()
        return iter([])
    watch.stream.side_effect = stream
    monkeypatch.setattr(module.watch, 'Watch', lambda: watch)
    manager._watch_loop()
    # The real applier uses custom API create/patch; inspect generated monitors directly.
    assert manager.k8s_api.v1.list_node.call_count == 2
    assert watch.stream.call_args.kwargs['resource_version'] == '3'
    calls = manager.k8s_api.create_namespaced_custom_object.call_args_list + manager.k8s_api.patch_namespaced_custom_object.call_args_list
    assert len(calls) == 3
    for call in calls:
        body = call.kwargs.get('body') or call.args[-1]
        assert body['spec']['endpoints'][0]['interval'] == '7500ms'


def test_observability_overlay_readiness_and_effective_capacity_before_origin():
    import yaml
    from manager import Manager
    manager = Manager.__new__(Manager)
    manager.intervals = SamplingIntervals(2)
    manager.workers_override = {'podPlayStageParallelism': 8}
    manager.service_monitor_manager = Mock()
    manager.service_monitor_manager.prepare.return_value = ['n']
    api = manager.service_monitor_manager.k8s_api
    api.get_namespaced_custom_object.return_value = {'spec': {'endpoints': [dict(port='https-main'), dict(port='https-self', interval='30s')]}}
    api.v1.read_namespaced_config_map.return_value = NS(data={'kwok.yaml': yaml.safe_dump(dict(
        apiVersion='config.kwok.x-k8s.io/v1alpha1', kind='KwokConfiguration', options=dict(
            podPlayStageParallelism=8, nodePlayStageParallelism=4, nodeLeaseParallelism=4)))})
    api.apps_v1.read_namespaced_deployment.return_value = NS(spec=NS(template=NS(spec=NS(containers=[
        NS(name='kwok-controller', image='registry.k8s.io/kwok/kwok:v0.8.0')]))))
    manager.workload_manager = Mock()
    manager.workload_manager.usage_manager.reconcile_interval_seconds = 2.5
    manager.collector = Mock()
    manager.collector.step = 15
    manager.collector.wait_ready.return_value = dict(kwok=7.5, kube_state_metrics=15)
    manager.prepare_observability()
    patch = api.patch_namespaced_custom_object.call_args.args[-1]
    assert patch['spec']['endpoints'] == [dict(port='https-main', interval='15s', scrapeTimeout='10s'), dict(port='https-self', interval='30s')]
    assert manager.observability['workers'] == dict(podPlayStageParallelism=8, nodePlayStageParallelism=4, nodeLeaseParallelism=4)
    assert manager.observability['images']['kwok-controller'].endswith(':v0.8.0')
    manager.collector.wait_ready.assert_called_once_with(manager.intervals, ['n'])
    manager.workers_override['podPlayStageParallelism'] = 9
    with pytest.raises(ValueError, match='does not match'):
        manager.prepare_observability()



def test_replay_unix_origin_is_representable_without_whole_second_truncation():
    from experiment_clock import ExperimentClock
    clock = ExperimentClock(unix_time=lambda: 1700000000.1259).start()
    assert clock.origin_unix == 1700000000.125
