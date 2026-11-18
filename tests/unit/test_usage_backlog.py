import queue
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from test_run_lifecycle import lifecycle, usage_ack, usage_parent
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
    now = [0]
    parent = usage_parent(events=40, monotonic=lambda: now[0])
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    def step(timeout):
        now[0] += 1
        parent.usage_status.put(usage_ack(now[0]))
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
            parent.usage_status.put(usage_ack(1))
            parent.usage_status.put(usage_ack(2))
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
            parent.usage_status.put(usage_ack(1))
            parent.usage_status.put(usage_ack(2))
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


def clock_sequence(*values):
    times = iter(values)
    return lambda: next(times)


def run_event(event, handler, times):
    statuses = queue.Queue()
    worker = UsageManager(queue.Queue(), status_queue=statuses)
    worker.handle_set_usage_event = handler
    worker.process_event(event, clock_sequence(*times))
    return [statuses.get_nowait() for _ in range(statuses.qsize())]


def replay_event(event_id, factor, scheduled=100, name='app'):
    return dict(namespace='ns', name=name, pods={}, event_id=event_id, replay_timing=dict(
        scheduled_monotonic=scheduled, trace_timestamp=40, speed_factor=factor))


@pytest.mark.parametrize('factor', [1, 2])
def test_completion_lag_includes_execution_without_changing_start_lag(factor):
    from workload.usage.usage_tracking import APPLIED, UsageTracker
    tracker = UsageTracker(lambda: 100)
    event = replay_event(1, factor)
    tracker.register(event)
    # Started 2 s after the deadline, then took 3 s.
    messages = run_event(event, Mock(return_value=APPLIED), (102, 105))
    started, ack = messages
    assert tracker.started(started) is None
    assert ack['lag_real'] == 2 and ack['lag_logical'] == 2 * factor
    assert tracker.acknowledge(ack) is None
    summary = tracker.summary()
    assert summary['completion_lag']['max_real'] == 5
    assert summary['completion_lag']['max_logical'] == 5 * factor
    assert summary['completion_lag']['late'] == 1
    assert summary['completion_lag']['worst']['event_id'] == 1
    assert summary['execution']['max_real'] == 3
    assert summary['outcomes']['applied'] == 1
    assert summary['acknowledgements']['max_active'] == 1
    assert summary['acknowledgements']['pending'] == 0


def test_setup_event_has_no_logical_completion_lag_but_counts_execution():
    from workload.usage.usage_tracking import APPLIED, UsageTracker
    now = [10]
    tracker = UsageTracker(lambda: now[0])
    event = dict(namespace='ns', name='app', pods={}, event_id=1)
    tracker.register(event)
    now[0] = 14
    oldest = tracker.summary()['acknowledgements']['oldest_pending']
    assert oldest['age_real'] == 4 and oldest['age_logical'] is None
    for message in run_event(event, Mock(return_value=APPLIED), (14, 15)):
        tracker.started(message) if message['kind'] == 'started' else tracker.acknowledge(message)
    summary = tracker.summary()
    assert summary['completion_lag']['replay_events'] == 0
    assert summary['execution']['count'] == 1
    assert summary['acknowledgements']['max_oldest_pending_age_real'] == 4


def worker_with_pods(pods, deployment_exists=True):
    worker = UsageManager(queue.Queue())
    worker.k8s_api = Mock()
    worker.k8s_api.v1.list_namespaced_pod.return_value = NS(items=pods)
    worker.deployment_exists = Mock(return_value=deployment_exists)
    worker.assignment_engine = Mock()
    worker.cleanup_workload = Mock()
    return worker


def test_reconciliation_outcomes_distinguish_applied_empty_and_absent_target():
    from workload.usage.usage_tracking import APPLIED, EMPTY, TARGET_ABSENT
    event = dict(namespace='ns', name='app', pods={'real': dict(cpu=.1, memory=10)})
    worker = worker_with_pods([pod('p', 'app')])
    worker.assignment_engine.resolve.return_value = {('ns', 'p'): dict(cpu=.1, memory=10)}
    worker.apply_assignment = Mock()
    assert worker.handle_set_usage_event(event) == APPLIED

    worker = worker_with_pods([])
    worker.assignment_engine.resolve.return_value = {}
    worker.apply_assignment = Mock()
    assert worker.handle_set_usage_event(event) == EMPTY
    worker.apply_assignment.assert_called_once_with(('ns', 'app'), {})

    worker = worker_with_pods([], deployment_exists=False)
    worker.apply_assignment = Mock()
    # Cleanup of a missing target is not an application of its usage.
    assert worker.handle_set_usage_event(event) == TARGET_ABSENT
    worker.cleanup_workload.assert_called_once_with(('ns', 'app'))
    worker.apply_assignment.assert_not_called()


def test_partial_pod_failure_is_a_failed_terminal_outcome():
    from concurrent.futures import ThreadPoolExecutor
    from util.k8s_objects.cluster_resource_usage_generator import ClusterResourceUsageGenerator
    from workload.usage.usage_tracking import UsageTracker
    worker = worker_with_pods([pod('p0', 'app'), pod('p1', 'app')])
    worker.cru_generator = ClusterResourceUsageGenerator()
    worker.assignment_engine.resolve.return_value = {
        ('ns', 'p0'): dict(cpu=.1, memory=10), ('ns', 'p1'): dict(cpu=.1, memory=10)}
    def apply(name, body):
        if name.endswith('p1'):
            raise RuntimeError('p1 denied')
    worker.apply_cru = apply
    worker.status_queue = queue.Queue()
    tracker = UsageTracker(lambda: 0)
    event = replay_event(1, 2)
    tracker.register(event)
    with ThreadPoolExecutor(max_workers=2) as executor:
        worker.usage_executor = executor
        worker.process_event(event, clock_sequence(101, 102))
    messages = [worker.status_queue.get_nowait() for _ in range(worker.status_queue.qsize())]
    [ack] = [message for message in messages if message['kind'] == 'ack']
    assert ack['outcome'] == 'failed' and not ack['completed'] and 'p1 denied' in ack['message']
    assert ('ns', 'app') not in worker.last_emulated_pods
    tracker.acknowledge(ack)
    assert tracker.summary()['outcomes']['failed'] == 1


def test_interrupted_event_reports_cancellation_once_and_propagates():
    statuses = queue.Queue()
    worker = UsageManager(queue.Queue(), status_queue=statuses)
    worker.handle_set_usage_event = Mock(side_effect=KeyboardInterrupt)
    with pytest.raises(KeyboardInterrupt):
        worker.process_event(replay_event(7, 2), clock_sequence(101, 104))
    started, ack = statuses.get_nowait(), statuses.get_nowait()
    assert statuses.empty()
    assert (ack['event_id'], ack['outcome'], ack['completed']) == (7, 'cancelled', False)


def test_duplicate_and_unknown_acks_are_protocol_errors_not_double_counts():
    parent = usage_parent([usage_ack(1), usage_ack(1), usage_ack(99), usage_ack(None)])
    parent.check_usage()
    progress = parent.usage_progress
    assert progress['completed'] == 1 and progress['failed'] == 0 and progress['pending'] == 1
    assert progress['acknowledgements']['protocol_errors'] == 3
    assert [error['kind'] for error in progress['errors']] == ['protocol'] * 3
    assert 'duplicate' in progress['errors'][0]['message']
    assert 'unknown' in progress['errors'][1]['message']


def test_inconsistent_outcome_is_rejected():
    parent = usage_parent([dict(usage_ack(1), completed=False), usage_ack(2, 'bogus')])
    parent.check_usage()
    assert parent.usage_progress['completed'] == parent.usage_progress['failed'] == 0
    assert parent.usage_progress['acknowledgements']['protocol_errors'] == 2


def test_out_of_order_acks_keep_contiguous_watermark_and_gaps():
    parent = usage_parent([usage_ack(1), usage_ack(3), usage_ack(5, 'empty')], events=5)
    parent.check_usage()
    acks = parent.usage_progress['acknowledgements']
    # The highest identity does not prove that earlier events finished.
    assert (acks['acknowledged_through'], acks['highest_acknowledged'], acks['gaps']) == (1, 5, 2)
    assert acks['out_of_order'] == 2 and acks['pending'] == 2
    assert acks['oldest_pending']['event_id'] == 2
    parent.usage_status.put(usage_ack(2))
    parent.usage_status.put(usage_ack(4, 'target_absent'))
    parent.check_usage()
    acks = parent.usage_progress['acknowledgements']
    assert (acks['acknowledged_through'], acks['gaps'], acks['pending']) == (5, 0, 0)
    assert acks['oldest_pending'] is None
    assert parent.usage_progress['outcomes'] == dict(
        applied=3, empty=1, target_absent=1, failed=0, cancelled=0)
    assert parent.usage_progress['completed'] == 5


def test_delivery_assigns_sequential_ids_only_after_successful_put():
    parent = usage_parent(events=0)
    parent.usage_manager.pid = None
    delivered = []
    def put(event):
        if event['name'] == 'b':
            raise RuntimeError('queue failed')
        delivered.append(event['event_id'])
    parent.usage_queue.put.side_effect = put
    parent.set_workload_usage(dict(namespace='ns', name='a'))
    with pytest.raises(RuntimeError, match='queue failed'):
        parent.set_workload_usage(dict(namespace='ns', name='b'))
    parent.set_workload_usage(dict(namespace='ns', name='c'))
    assert delivered == [1, 2]
    assert parent.usage_progress['enqueued'] == 2 and parent.usage_progress['pending'] == 2
    assert sorted(parent.usage_tracker.pending) == [1, 2]
    assert parent.usage_tracker.pending[2]['resource'] == 'c'
