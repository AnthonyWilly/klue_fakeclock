import queue
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from test_run_lifecycle import lifecycle, usage_parent
from workload.usage.usage_manager import UsageManager


def pod(name, workload):
    return NS(metadata=NS(name=name, namespace='ns', labels={'deployment': workload},
                          deletion_timestamp=None), status=NS(phase='Running'),
              spec=NS(containers=[]))


def test_periodic_scan_uses_one_snapshot_for_1000_workloads():
    worker = UsageManager(queue.Queue())
    worker.k8s_api = Mock()
    pods = [pod(f'p{i}', f'app{i}') for i in range(1000)]
    worker.k8s_api.v1.list_pod_for_all_namespaces.return_value = NS(items=pods)
    worker.last_usage_actions = {('ns', f'app{i}'): {} for i in range(1000)}
    worker.last_emulated_pods = {('ns', f'app{i}'): {('ns', f'p{i}')} for i in range(1000)}
    worker.reconcile_workload = Mock()
    worker.reconcile_tracked_workloads()
    worker.k8s_api.v1.list_pod_for_all_namespaces.assert_called_once_with(
        label_selector='deployment', _request_timeout=200)
    worker.k8s_api.v1.list_namespaced_pod.assert_not_called()
    worker.k8s_api.apps_v1.list_deployment_for_all_namespaces.assert_not_called()
    worker.reconcile_workload.assert_not_called()
    assert worker.api_stats['list-pods']['calls'] == 1


def test_periodic_snapshot_handles_new_pods_and_deleted_workloads():
    worker = UsageManager(queue.Queue())
    worker.k8s_api = Mock()
    worker.last_usage_actions = {('ns', name): {} for name in ('new', 'gone', 'scaled-zero')}
    added = pod('new-pod', 'new')
    worker.k8s_api.v1.list_pod_for_all_namespaces.return_value = NS(items=[added])
    worker.k8s_api.apps_v1.list_deployment_for_all_namespaces.return_value = NS(items=[
        NS(metadata=NS(namespace='ns', name=n)) for n in ('new', 'scaled-zero')])
    worker.reconcile_workload = Mock()
    worker.cleanup_workload = Mock()
    worker.reconcile_tracked_workloads()
    worker.cleanup_workload.assert_called_once_with(('ns', 'gone'))
    assert [c.args[0] for c in worker.reconcile_workload.call_args_list] == [
        ('ns', 'new'), ('ns', 'scaled-zero')]


def test_failed_assignment_does_not_commit_pod_set_and_can_retry():
    worker = UsageManager(queue.Queue())
    worker.last_usage_actions[('ns', 'app')] = {}
    worker.assignment_engine = Mock()
    worker.apply_assignment = Mock(side_effect=[RuntimeError('denied'), None])
    pods = NS(items=[pod('p', 'app')])
    with pytest.raises(RuntimeError):
        worker.reconcile_workload(('ns', 'app'), {('ns', 'p')}, pods)
    assert ('ns', 'app') not in worker.last_emulated_pods
    worker.reconcile_workload(('ns', 'app'), {('ns', 'p')}, pods)
    assert worker.last_emulated_pods[('ns', 'app')] == {('ns', 'p')}


def test_drain_allows_40_seconds_of_healthy_fifo_progress(monkeypatch):
    parent = usage_parent()
    parent.usage_progress.update(enqueued=40, pending=40)
    now = [0]
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    def step(timeout):
        now[0] += 1
        parent.usage_status.put(dict(kind='ack', completed=True))
    parent.usage_manager.join.side_effect = step
    parent.drain_usage(timeout=5)
    assert now[0] == 40
    assert parent.usage_progress['pending'] == 0
    parent.usage_queue.put.assert_not_called()  # Worker stays alive for the tail.


def test_api_activity_keeps_a_slow_action_alive(monkeypatch):
    parent = usage_parent()
    now = [0]
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    def step(timeout):
        now[0] += 1
        parent.usage_status.put(dict(kind='activity', action='patch-usage', stats={'calls': now[0]}))
        if now[0] == 20:
            parent.usage_status.put(dict(kind='ack', completed=True))
            parent.usage_status.put(dict(kind='ack', completed=True))
    parent.usage_manager.join.side_effect = step
    parent.drain_usage(timeout=3)
    assert parent.usage_progress['api']['patch-usage']['calls'] == 20


def test_drain_stalled_or_dead_worker_fails_without_hanging(monkeypatch):
    parent = usage_parent()
    now = [0]
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    parent.usage_manager.join.side_effect = lambda timeout: now.__setitem__(0, now[0] + 1)
    with pytest.raises(RuntimeError, match='inactivity'):
        parent.drain_usage(timeout=3)
    parent.usage_manager.exitcode = 9
    with pytest.raises(RuntimeError, match='code 9'):
        parent.drain_usage()


def test_normal_stop_waits_for_periodic_activity_but_cleanup_stays_bounded(monkeypatch):
    parent = usage_parent()
    now = [0]
    alive = [True]
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    parent.usage_manager.is_alive.side_effect = lambda: alive[0]
    def step(timeout):
        now[0] += 1
        parent.usage_status.put(dict(kind='activity', action='delete-usage', stats={'calls': now[0]}))
        if now[0] == 40:
            parent.usage_status.put(dict(kind='ack', completed=True))
            parent.usage_status.put(dict(kind='ack', completed=True))
            parent.usage_status.put(dict(kind='stopped'))
            parent.usage_manager.exitcode = 0
            alive[0] = False
    parent.usage_manager.join.side_effect = step
    parent.finish_usage(timeout=3, allow_progress=True)
    assert now[0] == 40
    assert parent.usage_progress['stopped']


def test_shutdown_checks_interruptions():
    parent = usage_parent()
    with pytest.raises(KeyboardInterrupt):
        parent.finish_usage(allow_progress=True,
            check=Mock(side_effect=KeyboardInterrupt('stop requested')))


def test_drain_and_stop_surround_the_observable_tail(lifecycle):
    manager, report, now = lifecycle
    order = []
    manager.workload_manager.drain_usage.side_effect = lambda **kw: order.append('drain')
    manager.workload_manager.finish_usage.side_effect = lambda **kw: order.append('stop')
    wait = manager.clock.wait
    def tail(seconds):
        order.append('tail')
        wait(seconds)
    manager.clock.wait = tail
    manager.run()
    assert order == ['drain', 'drain', 'tail', 'stop']


def test_pod_writes_are_parallel_but_each_event_has_a_completion_barrier():
    import threading
    from concurrent.futures import ThreadPoolExecutor
    from util.k8s_objects.cluster_resource_usage_generator import ClusterResourceUsageGenerator
    worker = UsageManager(queue.Queue())
    worker.cru_generator = ClusterResourceUsageGenerator()
    barrier = threading.Barrier(4)
    finished = []
    def apply(name, body):
        barrier.wait(timeout=5)
        finished.append(name)
        if name.endswith('p0'):
            raise RuntimeError('one pod failed')
    worker.apply_cru = apply
    worker.delete_stale_crus = Mock()
    assignment = {('ns', f'p{i}'): dict(cpu=1, memory=100) for i in range(4)}
    with ThreadPoolExecutor(max_workers=4) as executor:
        worker.usage_executor = executor
        with pytest.raises(RuntimeError, match='one pod failed'):
            worker.apply_assignment(('ns', 'app'), assignment)
        assert len(finished) == 4
        worker.delete_stale_crus.assert_not_called()
