"""Per-workload ordering, bounded concurrency and admission of usage events.

Barriers, gates and counters prove overlap, ordering and limits; nothing here
measures wall-clock performance.
"""
import queue
import threading
from collections import defaultdict
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from test_run_lifecycle import usage_ack, usage_parent
from test_usage_resource_cache import caches, deployment, pod
from util.k8s_objects.cluster_resource_usage_generator import ClusterResourceUsageGenerator
from workload.usage.event_dispatcher import EventDispatcher, FairExecutor
from workload.usage.usage_assignment import UsageAssignmentEngine
from workload.usage.usage_manager import UsageManager
from workload.usage.usage_tracking import APPLIED, CANCELLED

WAIT = 5


def drain(statuses):
    return [statuses.get_nowait() for _ in range(statuses.qsize())]


def usage_threads():
    return [t for t in threading.enumerate()
            if t.name.startswith(('usage-event', 'usage-write')) and t.is_alive()]


def event(name, event_id, pods=None, namespace='ns'):
    return dict(namespace=namespace, name=name, event_id=event_id,
                pods=pods or {'real': dict(cpu=.1, memory=1)})


# ------------------------------------------------------------------ dispatcher


def test_independent_workloads_overlap_and_one_workload_stays_serial():
    dispatcher = EventDispatcher(2)
    # Both first tasks must be running at once to pass the barrier.
    barrier = threading.Barrier(2, timeout=WAIT)
    lock = threading.Lock()
    running, order, observed = defaultdict(int), [], []

    def task(key, index, meet=False):
        def run(claim):
            with lock:
                running[key] += 1
                observed.append(running[key])
            if meet:
                barrier.wait()
            with lock:
                order.append((key, index))
                running[key] -= 1
        return run

    for index in range(6):
        dispatcher.submit('a', task('a', index, meet=index == 0))
    dispatcher.submit('b', task('b', 0, meet=True))
    for index in range(1, 4):
        dispatcher.submit('b', task('b', index))
    assert dispatcher.wait_idle(WAIT)
    assert dispatcher.close(WAIT)
    assert max(observed) == 1
    assert [i for key, i in order if key == 'a'] == list(range(6))
    assert [i for key, i in order if key == 'b'] == list(range(4))
    assert dispatcher.stats()['max_active'] == 2


def test_round_robin_lets_other_workloads_run_behind_a_long_backlog():
    dispatcher = EventDispatcher(1)
    gate, order = threading.Event(), []
    dispatcher.submit('big', lambda claim: (gate.wait(WAIT), order.append('big-0')))
    for index in range(1, 50):
        dispatcher.submit('big', lambda claim, i=index: order.append(f'big-{i}'))
    dispatcher.submit('small', lambda claim: order.append('small'))
    gate.set()
    assert dispatcher.wait_idle(WAIT) and dispatcher.close(WAIT)
    assert order.index('small') == 1
    assert len(order) == 51


def test_escaped_task_failure_is_reported_by_the_dispatcher():
    dispatcher = EventDispatcher(1)
    dispatcher.submit('a', Mock(side_effect=SystemExit('worker died')))
    assert dispatcher.wait_idle(WAIT)
    with pytest.raises(RuntimeError, match='worker died'):
        dispatcher.check()
    assert dispatcher.close(WAIT)


def test_fair_executor_bounds_calls_and_serves_groups_round_robin():
    executor = FairExecutor(1)
    gate, order = threading.Event(), []
    first = executor.submit(lambda: (gate.wait(WAIT), order.append('big-0')), group='big')
    for index in range(1, 50):
        executor.submit(order.append, f'big-{index}', group='big')
    small = executor.submit(order.append, 'small', group='small')
    gate.set()
    small.result(WAIT)
    first.result(WAIT)
    assert order.index('small') <= 2
    assert executor.shutdown(WAIT)
    assert executor.stats()['max_running'] == 1


# ------------------------------------------------------------- usage worker


def worker_with_dispatcher(workers=2, **kwargs):
    statuses = queue.Queue()
    worker = UsageManager(queue.Queue(), status_queue=statuses, event_workers=workers, **kwargs)
    worker.dispatcher = EventDispatcher(workers)
    return worker, statuses


def test_periodic_reconciliation_is_queued_behind_events_of_its_workload():
    worker, statuses = worker_with_dispatcher(workers=1)
    worker.resource_cache = Mock()
    worker.resource_cache.take_dirty.return_value = {('ns', 'a'), ('ns', 'b')}
    worker.last_usage_actions = {('ns', 'a'): {}, ('ns', 'b'): {}}
    gate, order = threading.Event(), []
    def handle(event):
        if event['event_id'] == 1:
            gate.wait(WAIT)
        order.append(event['event_id'])
        return APPLIED
    worker.handle_set_usage_event = handle
    worker.reconcile_workload = Mock()
    worker.observe_workload = lambda key: NS(keys=set(), pods=NS(items=[]), signature=key)
    worker.submit_event(event('a', 1), lambda: 0)
    worker.submit_event(event('a', 2), lambda: 0)
    worker.reconcile_tracked_workloads()
    # Still queued: a second tick does not queue the same workload again.
    worker.reconcile_tracked_workloads()
    worker.reconcile_workload.side_effect = lambda key, *args: order.append(key)
    gate.set()
    assert worker.dispatcher.wait_idle(WAIT) and worker.dispatcher.close(WAIT)
    # Without starvation: b runs as soon as the first event of a releases the worker.
    assert order == [1, ('ns', 'b'), 2, ('ns', 'a')]
    assert not worker.queued_reconciliations


def test_usage_of_a_workload_is_recorded_when_its_event_starts():
    worker, _ = worker_with_dispatcher(workers=2)
    gate, seen = threading.Event(), []
    def reconcile(workload_key):
        seen.append(worker.last_usage_actions[workload_key])
        if len(seen) == 1:
            gate.wait(WAIT)
        return APPLIED
    worker.reconcile_workload = reconcile
    worker.submit_event(event('a', 1, {'r': dict(cpu=1, memory=1)}), lambda: 0)
    worker.submit_event(event('a', 2, {'r': dict(cpu=2, memory=2)}), lambda: 0)
    gate.set()
    assert worker.dispatcher.wait_idle(WAIT) and worker.dispatcher.close(WAIT)
    assert [usage[('ns', 'r')]['cpu'] for usage in seen] == [1, 2]


def run_events(worker, events):
    for item in events:
        worker.queue.put(item)
    worker.queue.put('STOP')
    worker.process_events()
    return drain(worker.status_queue)


def replay_assignments(caches, workers):
    """Run interleaved events of several workloads with the real engine."""
    pods = [pod(f'w{w}-e{i}', f'w{w}') for w in range(6) for i in range(w % 3 + 1)]
    cache = caches(pods=pods, deployments=[deployment(f'w{w}') for w in range(6)])
    worker = UsageManager(queue.Queue(), reconcile_interval_seconds=3600,
                          status_queue=queue.Queue(), event_workers=workers)
    worker.resource_cache = cache
    worker.assignment_engine = UsageAssignmentEngine()
    worker.cru_generator = ClusterResourceUsageGenerator()
    worker.usage_executor = FairExecutor(UsageManager.API_CONCURRENCY)
    writes, lock = defaultdict(list), threading.Lock()
    def apply(name, body):
        with lock:
            writes[name].append(body['spec'])
    worker.apply_cru = apply
    worker.delete_cru = Mock()
    events, event_id = [], 0
    for round_ in range(4):
        for w in range(6):
            event_id += 1
            real = {f'r{i}': dict(cpu=round(.1 * (i + 1) * (round_ + w + 1), 3), memory=100 * (i + w + 1))
                    for i in range((w + round_) % 4 + 1)}
            events.append(event(f'w{w}', event_id, real))
    messages = run_events(worker, events)
    assert worker.usage_executor.shutdown(WAIT)
    acks = [m for m in messages if m['kind'] == 'ack']
    assert sorted(m['event_id'] for m in acks) == list(range(1, event_id + 1))
    assert {m['outcome'] for m in acks} == {APPLIED}
    return dict(writes), worker.assignment_engine.state


def test_assignments_are_identical_across_serial_and_concurrent_runs(caches):
    serial = replay_assignments(caches, workers=1)
    for _ in range(3):
        assert replay_assignments(caches, workers=4) == serial


def test_all_cru_mutations_share_a_global_limit_of_four():
    worker, statuses = worker_with_dispatcher(workers=3)
    worker.cru_generator = ClusterResourceUsageGenerator()
    worker.usage_executor = FairExecutor(UsageManager.API_CONCURRENCY)
    lock = threading.Lock()
    state = dict(running=0, max=0)
    in_flight = defaultdict(int)
    reached = threading.Barrier(4, timeout=WAIT)
    def mutation(workload, name):
        with lock:
            state['running'] += 1
            state['max'] = max(state['max'], state['running'])
            in_flight[workload] += 1
        try:
            if name in {f'usage-ns-big-p{i}' for i in range(4)}:
                reached.wait()  # the first four writes must overlap
        finally:
            with lock:
                state['running'] -= 1
                in_flight[workload] -= 1
    worker.apply_cru = lambda name, body: mutation(name.split('-')[2], name)
    worker.delete_cru = lambda name: mutation(name.split('-')[2], name)
    worker.managed_crus_by_workload[('ns', 'gone')] = {f'usage-ns-gone-{i}' for i in range(20)}
    worker.assignment_engine = Mock()
    worker.managed_crus_by_workload[('ns', 'small')] = {'usage-ns-small-old'}

    acks = []
    class Statuses(queue.Queue):
        def put(self, message, *args, **kwargs):
            if message.get('kind') == 'ack':
                # Every write of an event finished before its acknowledgement.
                with lock:
                    assert in_flight[message['resource']] == 0
                acks.append(message)
            super().put(message, *args, **kwargs)
    worker.status_queue = Statuses()

    def handle(item):
        key = (item['namespace'], item['name'])
        if item['name'] == 'gone':
            worker.cleanup_workload(key)
            return APPLIED
        size = 200 if item['name'] == 'big' else 2
        worker.apply_assignment(key, {('ns', f'{item["name"]}-p{i}'): dict(cpu=.1, memory=1)
                                      for i in range(size)})
        return APPLIED
    worker.handle_set_usage_event = handle
    for index, name in enumerate(('big', 'small', 'gone'), 1):
        worker.submit_event(event(name, index), lambda: 0)
    assert worker.dispatcher.wait_idle(WAIT) and worker.dispatcher.close(WAIT)
    assert worker.usage_executor.shutdown(WAIT)
    assert state['max'] == 4 and state['running'] == 0
    assert worker.usage_executor.stats()['max_running'] == 4
    assert sorted(a['event_id'] for a in acks) == [1, 2, 3]
    assert worker.managed_crus_by_workload[('ns', 'small')] == {'usage-ns-small-p0', 'usage-ns-small-p1'}
    assert ('ns', 'gone') not in worker.managed_crus_by_workload


def test_failed_write_of_one_workload_does_not_affect_another(caches):
    cache = caches(pods=[pod('ok-p', 'ok'), pod('bad-p', 'bad')],
                   deployments=[deployment('ok'), deployment('bad')])
    worker, statuses = worker_with_dispatcher(workers=2)
    worker.resource_cache = cache
    worker.k8s_api = Mock()
    worker.assignment_engine = UsageAssignmentEngine()
    worker.cru_generator = ClusterResourceUsageGenerator()
    worker.usage_executor = FairExecutor(UsageManager.API_CONCURRENCY)
    def apply(name, body):
        if 'bad' in name:
            raise RuntimeError('denied')
    worker.apply_cru = apply
    worker.submit_event(event('bad', 1), lambda: 0)
    worker.submit_event(event('ok', 2), lambda: 0)
    assert worker.dispatcher.wait_idle(WAIT) and worker.dispatcher.close(WAIT)
    assert worker.usage_executor.shutdown(WAIT)
    acks = {m['event_id']: m for m in drain(statuses) if m['kind'] == 'ack'}
    assert acks[1]['outcome'] == 'failed' and acks[2]['outcome'] == APPLIED
    assert worker.last_emulated_pods == {('ns', 'ok'): {('ns', 'ok-p')}}
    assert worker.retry_workloads == {('ns', 'bad')}


def test_stop_finishes_accepted_backlog_before_confirming_stopped():
    statuses, events = queue.Queue(), queue.Queue()
    worker = UsageManager(events, reconcile_interval_seconds=3600, status_queue=statuses, event_workers=2)
    gate = threading.Event()
    def handle(item):
        gate.wait(WAIT)
        return APPLIED
    worker.handle_set_usage_event = handle
    for index in range(1, 31):
        events.put(event(f'w{index % 4}', index))
    events.put('STOP')
    before = usage_threads()
    runner = threading.Thread(target=worker.process_events)
    runner.start()
    # STOP was received, yet nothing is confirmed while accepted work remains.
    assert not runner.join(.2) and runner.is_alive()
    assert events.empty()
    assert 'stopped' not in [m['kind'] for m in drain(statuses)]
    gate.set()
    runner.join(WAIT)
    assert not runner.is_alive()
    messages = drain(statuses)
    kinds = [m['kind'] for m in messages]
    assert kinds[-1] == 'stopped'
    assert sorted(m['event_id'] for m in messages if m['kind'] == 'ack') == list(range(1, 31))
    assert all(m['outcome'] == APPLIED for m in messages if m['kind'] == 'ack')
    concurrency = [m for m in messages if m['kind'] == 'concurrency'][-1]['stats']
    assert concurrency['event_workers'] == 2 and concurrency['max_active_events'] == 2
    assert usage_threads() == before


class InterruptingQueue:
    """Deliver the given items, then interrupt the receiver once ``ready`` is set."""

    def __init__(self, items, ready):
        self.items = list(items)
        self.ready = ready

    def get(self, timeout=None):
        if self.items:
            return self.items.pop(0)
        assert self.ready.wait(WAIT)
        raise KeyboardInterrupt('stop requested')


def test_interruption_cancels_unfinished_events_once_even_with_a_hung_thread():
    statuses = queue.Queue()
    hung, released = threading.Event(), threading.Event()
    worker = UsageManager(InterruptingQueue([event('a', 1), event('a', 2), event('b', 3)], hung),
                          reconcile_interval_seconds=3600, status_queue=statuses, event_workers=2)
    worker.CANCEL_TIMEOUT = .2
    def handle(item):
        if item['name'] == 'a':
            hung.set()
            released.wait(WAIT)  # a call that ignores cancellation
        return APPLIED
    worker.handle_set_usage_event = handle
    with pytest.raises(KeyboardInterrupt):
        worker.process_events()
    assert hung.is_set()
    released.set()
    for thread in usage_threads():
        thread.join(WAIT)
    assert not usage_threads()
    acks = [m for m in drain(statuses) if m['kind'] == 'ack']
    by_id = defaultdict(list)
    for ack in acks:
        by_id[ack['event_id']].append(ack['outcome'])
    # The hung event and the one queued behind it are cancelled exactly once;
    # the late completion of the hung call is discarded, never reported applied.
    assert by_id[1] == [CANCELLED] and by_id[2] == [CANCELLED]
    assert len(by_id[3]) == 1
    assert all(not ack['completed'] for ack in acks if ack['outcome'] == CANCELLED)


def test_worker_threads_stop_with_the_worker(monkeypatch):
    from test_usage_resource_cache import make_cache
    statuses, events = queue.Queue(), queue.Queue()
    events.put(event('a', 1))
    events.put('STOP')
    worker = UsageManager(events, status_queue=statuses, event_workers=3)
    monkeypatch.setattr('workload.usage.usage_manager.K8SAPI', Mock())
    monkeypatch.setattr('workload.usage.usage_manager.ResourceCache',
                        lambda *args, **kwargs: make_cache(start=False))
    worker.handle_set_usage_event = Mock(return_value=APPLIED)
    before = usage_threads()
    worker.run()
    assert usage_threads() == before
    assert drain(statuses)[-1] == dict(kind='stopped')


# --------------------------------------------------------- parent admission


def full_parent(limit=1024):
    parent = usage_parent(events=limit)
    delivered = []
    parent.usage_queue.put.side_effect = delivered.append
    return parent, delivered


def producer(parent, **kwargs):
    outcome = {}
    def run():
        try:
            parent.set_workload_usage(dict(namespace='ns', name='late'), **kwargs)
            outcome['ok'] = True
        except BaseException as error:
            outcome['error'] = error
    thread = threading.Thread(target=run)
    thread.start()
    return thread, outcome


def test_saturated_admission_waits_without_dropping_and_resumes_on_any_ack():
    parent, delivered = full_parent()
    assert len(parent.usage_tracker.pending) == parent.USAGE_ADMISSION_LIMIT == 1024
    thread, outcome = producer(parent)
    thread.join(.2)
    assert thread.is_alive() and not delivered
    # The consumer of acknowledgements is never blocked by the waiting producer.
    assert parent.usage_lock.acquire(timeout=WAIT)
    parent.usage_lock.release()
    # An out-of-order acknowledgement releases capacity.
    parent.usage_status.put(usage_ack(700))
    thread.join(WAIT)
    assert outcome == {'ok': True}
    assert [e['event_id'] for e in delivered] == [1025]
    assert len(parent.usage_tracker.pending) == 1024
    assert parent.usage_progress['admission']['waits'] == 1


def test_producer_waiting_for_capacity_is_interruptible():
    parent, delivered = full_parent()
    cancel = threading.Event()
    thread, outcome = producer(parent, cancel=cancel)
    thread.join(.1)
    assert thread.is_alive()
    cancel.set()
    thread.join(WAIT)
    assert isinstance(outcome['error'], InterruptedError)
    assert not delivered and parent.usage_tracker.next_event_id == 1025
    assert len(parent.usage_tracker.pending) == 1024


def test_full_admission_fails_on_stalled_or_dead_worker():
    parent, _ = full_parent()
    with pytest.raises(RuntimeError, match='no progress'):
        parent.wait_usage_capacity(timeout=.05, poll=.01)
    parent.usage_manager.exitcode = 9
    with pytest.raises(RuntimeError, match='code 9'):
        parent.wait_usage_capacity(timeout=WAIT, poll=.01)


def test_failed_delivery_releases_its_reservation():
    parent, delivered = full_parent(limit=1023)
    parent.usage_queue.put.side_effect = [RuntimeError('queue failed'), None]
    with pytest.raises(RuntimeError, match='queue failed'):
        parent.set_workload_usage(dict(namespace='ns', name='a'))
    assert len(parent.usage_tracker.pending) == 1023 and parent.usage_progress['pending'] == 1023
    parent.set_workload_usage(dict(namespace='ns', name='b'))
    assert parent.usage_tracker.pending[1024]['resource'] == 'b'
    # The withdrawn identity can never be acknowledged as an orphan.
    parent.usage_status.put(usage_ack(1025))
    parent.check_usage()
    assert parent.usage_progress['acknowledgements']['protocol_errors'] == 1


def test_concurrency_report_reaches_parent_progress():
    parent = usage_parent([dict(kind='concurrency', stats=dict(event_workers=2, max_active_events=2))])
    parent.check_usage()
    assert parent.usage_progress['concurrency'] == dict(event_workers=2, max_active_events=2)
    assert not parent.usage_progress['errors']
