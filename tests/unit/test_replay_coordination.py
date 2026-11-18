"""Coordinated replay: instant barriers, topology fences and the usage limit.

Parent-side tests drive real Infrastructure/WorkloadManager phases with a fake
clock and a fake usage worker that acknowledges events as fake time passes.
Worker-side tests use the fake LIST/WATCH cache. Nothing reaches a cluster and
no assertion depends on wall-clock speed.
"""
import queue
import threading
import time as real_time
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from experiment_clock import ReplayDiagnostics
from infrastructure.manager import InfrastructureManager
from replay_coordinator import ReplayCoordinator
from workload.manager import WorkloadManager
from workload.usage.event_dispatcher import FairExecutor
from workload.usage.usage_tracking import (APPLIED, FAILED, TARGET_ABSENT, ReplayFidelityError,
                                          UsageTracker)
from test_experiment_clock import FakeTime
from test_usage_resource_cache import caches, deployment as cached_deployment, event as watch_event, \
    pod as cached_pod, usage_worker


@pytest.fixture(autouse=True)
def no_cluster(monkeypatch):
    def forbidden(*args, **kwargs):
        raise AssertionError('unit tests must not access Kubernetes')
    monkeypatch.setattr('kubernetes.config.load_kube_config', forbidden)


class FakeWorker:
    """Acknowledge events after ``duration(event)`` fake seconds.

    ``workers`` events run at once, FIFO per workload, like the dispatcher.
    A duration of None never completes; ``outcome(event)`` may return a
    terminal message override.
    """

    def __init__(self, time, status, log, duration=lambda event: 1, workers=2, outcome=None):
        self.time, self.status, self.log = time, status, log
        self.duration, self.workers, self.outcome = duration, workers, outcome
        self.waiting = []
        self.running = {}
        self.received = []
        self.busy = set()

    def put(self, event):
        self.received.append(event)
        self.log.append(('usage', event['name'], event['event_id'], self.time.now))
        self.waiting.append(event)
        self.start(self.time.now)

    def start(self, now):
        for event in list(self.waiting):
            if len(self.running) >= self.workers:
                return
            key = (event['namespace'], event['name'])
            if key in self.busy:
                continue
            self.waiting.remove(event)
            self.busy.add(key)
            duration = self.duration(event)
            finish = None if duration is None else now + duration
            self.running[event['event_id']] = (event, now, finish)
            self.status.put(dict(kind='started', event_id=event['event_id'], started_monotonic=now))

    def advance(self, now):
        progressed = True
        while progressed:
            progressed = False
            for event_id, (event, started, finish) in sorted(
                    self.running.items(), key=lambda item: (item[1][2] is None, item[1][2] or 0)):
                if finish is None or finish > now:
                    continue
                del self.running[event_id]
                self.busy.discard((event['namespace'], event['name']))
                message = dict(kind='ack', event_id=event_id, outcome=APPLIED, completed=True,
                               started_monotonic=started, finished_monotonic=finish)
                if self.outcome is not None:
                    message.update(self.outcome(event) or {})
                self.status.put(message)
                self.log.append(('ack', event_id, finish))
                self.start(finish)
                progressed = True


class HookTime(FakeTime):
    def __init__(self):
        super().__init__()
        self.hooks = []

    def wait(self, seconds):
        super().wait(seconds)
        for hook in self.hooks:
            hook(self.now)


def deployment(name, replicas=1, namespace='ns'):
    return {'kind': 'Deployment', 'metadata': {'name': name, 'namespace': namespace},
            'spec': {'replicas': replicas}}


def usage(name, namespace='ns'):
    return {'action': 'set-usage', 'name': name, 'namespace': namespace,
            'pods': {'real': {'cpu': .1, 'memory': 1}}}


def scale(name, pods, namespace='ns'):
    return {'action': 'scale', 'kind': 'Deployment', 'name': name, 'namespace': namespace, 'pods': pods}


def build(infra_entries, workload_entries, factor=2, phases=('dynamic', 'dynamic'), setup=(),
          **worker):
    time = HookTime()
    log = []
    infra = InfrastructureManager.__new__(InfrastructureManager)
    infra.data = {'emulation': infra_entries}
    infra.emulation_phase = phases[0]
    infra.replay_diagnostics = ReplayDiagnostics(len(infra_entries))
    infra.k8s_api = Mock()
    infra.k8s_object_applier = Mock()
    infra.k8s_object_applier.apply_node.side_effect = lambda obj, name: log.append(('node+', name, time.now))
    infra.k8s_api.delete_infrastructure_object.side_effect = lambda name: log.append(('node-', name, time.now))

    workload = WorkloadManager.__new__(WorkloadManager)
    workload.data = {'emulation': workload_entries,
                     'setup': {'applied_objects': {'ns': [deployment(name) for name in setup]}}}
    workload.emulation_phase = phases[1]
    workload.hpa = False
    workload.replay_diagnostics = ReplayDiagnostics(len(workload_entries))
    workload.k8s_api = Mock()
    workload.k8s_object_applier = Mock()
    workload.create_namespace_if_not_exists = Mock()
    generations = {}
    def apply(obj):
        name = obj['metadata']['name']
        log.append(('apply', name, time.now))
        generations[name] = generations.get(name, 0) + 1
        return NS(metadata=NS(uid=f'uid-{name}', generation=generations[name]),
                  spec=NS(replicas=obj['spec']['replicas']))
    workload.k8s_object_applier.apply_object.side_effect = apply
    def scale_call(name, namespace, body):
        log.append(('scale', name, time.now))
        generations[name] = generations.get(name, 0) + 1
        return NS(metadata=NS(uid=f'uid-{name}', generation=None), spec=NS(replicas=body['spec']['replicas']))
    workload.k8s_api.patch_namespaced_deployment_scale.side_effect = scale_call
    workload.k8s_object_applier.delete_object.side_effect = lambda obj: log.append(('delete', obj['name'], time.now))
    workload.usage_lock = threading.RLock()
    workload.usage_admission_lock = threading.Lock()
    workload.usage_status = queue.Queue()
    workload.usage_manager = Mock(pid=1, exitcode=None)
    workload.usage_tracker = UsageTracker(time.monotonic)
    workload.usage_progress = dict(enqueued=0, completed=0, failed=0, pending=0, stopped=False,
        errors=[], late_events=0, max_lag_real=0, max_lag_logical=0, total_lag_real=0,
        total_lag_logical=0, worst_event=None)
    fake = FakeWorker(time, workload.usage_status, log, **worker)
    workload.usage_queue = NS(put=fake.put)
    time.hooks.append(fake.advance)

    coordinator = ReplayCoordinator(infra, workload)
    workload.coordinate(coordinator.limit_logical)
    clock = time.clock(factor)
    clock.cancel = threading.Event()
    clock.defer_tail = True
    return NS(time=time, log=log, infra=infra, workload=workload, worker=fake,
              coordinator=coordinator, clock=clock)


def names(log, kinds=None):
    return [entry[:2] for entry in log if kinds is None or entry[0] in kinds]


def test_usage_barrier_orders_topology_and_keeps_original_deadlines():
    run = build(
        [{'timestamp': 0, 'applied_objects': [{'kind': 'Node', 'metadata': {'name': 'a'}}]},
         {'timestamp': 10, 'deleted_objects': ['a']}],
        [{'timestamp': 0, 'applied_objects': {'ns': [deployment('app', 2)]},
          'workload_actions': [usage('app')], 'deleted_objects': [{'kind': 'Deployment', 'name': 'old', 'namespace': 'ns'}]},
         {'timestamp': 10, 'workload_actions': [usage('app'), scale('app', 3)],
          'deleted_objects': [{'kind': 'Deployment', 'name': 'app', 'namespace': 'ns'}]}],
        setup=['old'], duration=lambda event: 8 if event['event_id'] == 1 else 1)
    run.coordinator.run(run.clock)
    # Usage of 0 (8 real s, 16 logical, within the limit) holds deletions of 0
    # and the scale of 10, whose deadline (105) is not moved earlier or later.
    assert names(run.log) == [
        ('node+', 'a'), ('apply', 'app'), ('usage', 'app'), ('ack', 1), ('delete', 'old'),
        ('scale', 'app'), ('usage', 'app'), ('ack', 2), ('delete', 'app'), ('node-', 'a')]
    times = {entry[:2]: entry[-1] for entry in run.log}
    assert times[('ack', 1)] == 108
    assert times[('delete', 'old')] == pytest.approx(108, abs=.03)
    assert times[('scale', 'app')] >= 108
    assert times[('delete', 'app')] >= times[('ack', 2)] == pytest.approx(times[('scale', 'app')] + 1, abs=.03)
    first, second = run.worker.received
    assert first['expected_topology'] == dict(present=True, uid='uid-app', generation=1, replicas=2)
    # The scale response confirms replicas; its identity keeps the known generation.
    assert second['expected_topology'] == dict(present=True, uid='uid-app', generation=1, replicas=3)
    assert first['replay_timing']['usage_deadline_monotonic'] == 100 + 30 / 2
    assert second['replay_timing']['scheduled_monotonic'] == 105
    stats = run.workload.replay_diagnostics.snapshot()
    assert stats['processed_entries'] == 2
    assert stats['worst_event']['scheduled'] == 105
    assert stats['max_lag_real'] == pytest.approx(3, abs=.03)
    assert run.coordinator.stats['usage_events'] == 2
    assert run.coordinator.stats['max_barrier_wait_real'] == pytest.approx(8, abs=.03)
    assert run.workload.usage_tracker.violation is None
    assert run.workload.topology.get(('ns', 'app')) is None
    assert run.workload.topology.get(('ns', 'old')) is None


def test_union_of_timestamps_counts_every_entry_in_input_order():
    run = build(
        [{'timestamp': 0, 'applied_objects': [{'kind': 'Node', 'metadata': {'name': 'a'}}]},
         {'timestamp': 5, 'applied_objects': [{'kind': 'Node', 'metadata': {'name': 'b'}}]},
         {'timestamp': 5, 'applied_objects': [{'kind': 'Node', 'metadata': {'name': 'c'}}]},
         {'timestamp': 5}],
        [{'timestamp': 5, 'applied_objects': {'ns': [deployment('x')]}},
         {'timestamp': 7},
         {'timestamp': 7, 'workload_actions': [usage('x'), usage('y')]}])
    run.coordinator.run(run.clock)
    assert names(run.log, {'node+', 'apply', 'usage'}) == [
        ('node+', 'a'), ('node+', 'b'), ('node+', 'c'), ('apply', 'x'), ('usage', 'x'), ('usage', 'y')]
    for manager, count in ((run.infra, 4), (run.workload, 3)):
        stats = manager.replay_diagnostics.snapshot()
        assert stats['expected_entries'] == stats['processed_entries'] == count
        assert stats['not_applicable_entries'] == 0
    assert run.coordinator.stats['instants'] == 3
    # 'y' is not part of the expected topology: its absence is legitimate.
    assert run.worker.received[1]['expected_topology']['present'] is False


@pytest.mark.parametrize('factor', [1, 2, 5])
def test_static_producers_keep_duration_without_actions(factor):
    run = build([{'timestamp': 0}, {'timestamp': 10}],
                [{'timestamp': 10, 'workload_actions': [usage('x')]}, {'timestamp': 30}],
                factor=factor, phases=('static', 'static'))
    run.coordinator.run(run.clock)
    assert run.time.now == pytest.approx(100 + 30 / factor, abs=.03)
    assert run.log == []
    for manager in (run.infra, run.workload):
        stats = manager.replay_diagnostics.snapshot()
        assert stats['processed_entries'] == stats['not_applicable_entries'] == 2
        assert stats['attempted_actions'] == 0


@pytest.mark.parametrize('factor', [1, 2])
def test_exactly_the_limit_is_valid(factor):
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('x')]},
                     {'timestamp': 40, 'deleted_objects': [{'kind': 'Deployment', 'name': 'x', 'namespace': 'ns'}]}],
                factor=factor, setup=['x'], duration=lambda event: 30 / factor)
    run.coordinator.run(run.clock)
    tracker = run.workload.usage_tracker
    assert tracker.violation is None
    assert tracker.completion['max_logical'] == 30
    assert names(run.log)[-1] == ('delete', 'x')


@pytest.mark.parametrize('factor', [1, 2])
def test_completion_beyond_limit_stops_replay_with_primary_diagnosis(factor):
    run = build([{'timestamp': 0}, {'timestamp': 10, 'applied_objects': [{'kind': 'Node', 'metadata': {'name': 'n'}}]}],
                [{'timestamp': 0, 'workload_actions': [usage('x')],
                  'deleted_objects': [{'kind': 'Deployment', 'name': 'x', 'namespace': 'ns'}]},
                 {'timestamp': 10, 'applied_objects': {'ns': [deployment('later')]}}],
                factor=factor, setup=['x'], duration=lambda event: 31 / factor)
    with pytest.raises(ReplayFidelityError, match='temporal fidelity violated') as raised:
        run.coordinator.run(run.clock)
    details = raised.value.details
    assert details['event_id'] == 1 and details['resource'] == 'x' and details['trace_timestamp'] == 0
    assert details['limit_logical'] == 30
    assert 30 < details['delay_logical'] <= 30 + .05 * factor
    assert details['reason'] == 'pending' and details['state'] == 'active'
    # Neither the deletion of the instant nor any future action ran.
    assert names(run.log) == [('usage', 'x')]
    assert run.workload.usage_progress['fidelity']['violation']['event_id'] == 1
    # Later checks (cleanup) do not raise the same violation again.
    run.time.wait(31)
    run.workload.check_usage()
    assert run.workload.usage_tracker.outcomes[APPLIED] == 1


def test_event_never_started_fails_by_age():
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a'), usage('b')]}],
                setup=['a', 'b'], workers=1, duration=lambda event: None)
    with pytest.raises(ReplayFidelityError) as raised:
        run.coordinator.run(run.clock)
    assert raised.value.details['state'] == 'active'
    assert raised.value.details['pending'] == 2
    assert run.time.now == pytest.approx(115, abs=.03)


def test_queued_event_fails_by_age_when_worker_never_starts_it():
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a')]}],
                setup=['a'], workers=0)
    with pytest.raises(ReplayFidelityError) as raised:
        run.coordinator.run(run.clock)
    assert raised.value.details['state'] == 'queued'


def test_missing_target_fails_before_the_limit():
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a')]}], setup=['a'],
                outcome=lambda event: dict(outcome=FAILED, completed=False, fidelity=True,
                                           message='ns/a: observed workload is missing'))
    with pytest.raises(ReplayFidelityError, match='found no expected target') as raised:
        run.coordinator.run(run.clock)
    assert raised.value.details['reason'] == 'topology'
    assert run.time.now == pytest.approx(101, abs=.03)


def test_dead_worker_and_cancellation_end_the_barrier_without_future_actions():
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a')]},
                     {'timestamp': 2, 'applied_objects': {'ns': [deployment('b')]}}],
                setup=['a'], duration=lambda event: None)
    run.time.hooks.append(lambda now: setattr(run.workload.usage_manager, 'exitcode', 9) if now >= 103 else None)
    with pytest.raises(RuntimeError, match='code 9'):
        run.coordinator.run(run.clock)
    assert names(run.log) == [('usage', 'a')]

    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a')]},
                     {'timestamp': 2, 'applied_objects': {'ns': [deployment('b')]}}],
                setup=['a'], duration=lambda event: None)
    run.time.hooks.append(lambda now: run.clock.cancel.set() if now >= 101 else None)
    with pytest.raises(InterruptedError):
        run.coordinator.run(run.clock)
    assert names(run.log) == [('usage', 'a')]
    assert run.time.now < 101.1


@pytest.mark.parametrize('duration, fails', [(.005, False), (.1, True)])
def test_660_workloads_over_31_instants_preserve_every_event(duration, fails):
    """2 workers at 2x: 660 events per 2.5 real s instant need <= 7.5 ms each."""
    workloads = [f'w{index}' for index in range(660)]
    entries = [{'timestamp': 5 * index, 'workload_actions': [usage(name) for name in workloads]}
               for index in range(31)]
    run = build([], entries, setup=workloads, duration=lambda event: duration, workers=2)
    if fails:
        with pytest.raises(ReplayFidelityError) as raised:
            run.coordinator.run(run.clock)
        assert raised.value.details['trace_timestamp'] == 0
        assert len(run.worker.received) == 660  # Nothing of a later instant was admitted.
        return
    run.coordinator.run(run.clock)
    tracker = run.workload.usage_tracker
    assert len(run.worker.received) == tracker.outcomes[APPLIED] == 31 * 660
    assert sorted(event['event_id'] for event in run.worker.received) == list(range(1, 31 * 660 + 1))
    assert tracker.protocol_errors == 0 and not tracker.pending
    assert tracker.max_pending == 660 <= WorkloadManager.USAGE_ADMISSION_LIMIT
    assert tracker.violation is None
    assert tracker.completion['max_logical'] <= 30
    # Stable topology: usage never issues a workload read or list from the parent.
    assert not run.workload.k8s_api.mock_calls
    assert not run.workload.k8s_object_applier.mock_calls
    assert run.coordinator.stats['usage_instants'] == 31


# ------------------------------------------------------------------ worker side

def coordinated_event(name, expected, budget=5, event_id=1):
    return dict(namespace='ns', name=name, pods={'real': dict(cpu=.1, memory=1)}, event_id=event_id,
                expected_topology=dict(dict(present=True, uid=None, generation=None, replicas=None), **expected),
                replay_timing=dict(scheduled_monotonic=real_time.monotonic(), speed_factor=2,
                                   usage_deadline_monotonic=real_time.monotonic() + budget))


def later(action, delay=.05):
    thread = threading.Timer(delay, action)
    thread.start()
    return thread


def test_worker_waits_for_scaled_generation_and_pods_before_applying(caches):
    cache = caches(pods=[cached_pod('a', 'app')], deployments=[cached_deployment('app')])
    worker = usage_worker(cache)
    watches = cache.fakes.watches
    def converge():
        watches.feed(cache.fakes.deployments).put(watch_event('MODIFIED', cached_deployment('app', generation=2)))
        watches.feed(cache.fakes.pods).put(watch_event('ADDED', cached_pod('b', 'app')))
    timer = later(converge)
    outcome = worker.handle_set_usage_event(coordinated_event(
        'app', dict(uid='d:ns/app', generation=2, replicas=2)))
    timer.join()
    assert outcome == APPLIED
    keys = worker.assignment_engine.resolve.call_args.kwargs['emulated_pods']
    assert keys == {('ns', 'a'), ('ns', 'b')}
    worker.k8s_api.read_namespaced_deployment.assert_not_called()
    assert len(cache.fakes.pods.calls) == len(cache.fakes.deployments.calls) == 1


def test_cache_older_than_creation_is_awaited_not_read_as_empty(caches):
    cache = caches()
    worker = usage_worker(cache)
    def created():
        cache.fakes.watches.feed(cache.fakes.pods).put(watch_event('ADDED', cached_pod('a', 'new')))
        cache.fakes.watches.feed(cache.fakes.deployments).put(watch_event('ADDED', cached_deployment('new')))
    timer = later(created)
    outcome = worker.handle_set_usage_event(coordinated_event('new', dict(uid='d:ns/new', replicas=1)))
    timer.join()
    assert outcome == APPLIED
    worker.k8s_api.read_namespaced_deployment.assert_not_called()


def test_uid_mismatch_or_missing_target_fails_after_catch_up(caches):
    cache = caches(deployments=[cached_deployment('app', uid='old')])
    worker = usage_worker(cache)
    worker.CACHE_CATCH_UP_TIMEOUT = .1
    with pytest.raises(ReplayFidelityError, match='uid-mismatch'):
        worker.handle_set_usage_event(coordinated_event('app', dict(uid='new', replicas=0)))
    with pytest.raises(ReplayFidelityError, match='missing'):
        worker.handle_set_usage_event(coordinated_event('gone', dict(replicas=0)))
    worker.apply_assignment.assert_not_called()
    assert ('ns', 'app') in worker.retry_workloads


def test_expected_absence_is_legitimate_but_an_unexpected_workload_is_not(caches):
    cache = caches(deployments=[cached_deployment('present')])
    worker = usage_worker(cache)
    worker.CACHE_CATCH_UP_TIMEOUT = .1
    worker.cleanup_workload = Mock()
    assert worker.handle_set_usage_event(coordinated_event('gone', dict(present=False))) == TARGET_ABSENT
    worker.cleanup_workload.assert_called_once_with(('ns', 'gone'))
    with pytest.raises(ReplayFidelityError, match='unexpected'):
        worker.handle_set_usage_event(coordinated_event('present', dict(present=False)))


def test_convergence_never_observed_fails_at_the_usage_deadline(caches):
    cache = caches(deployments=[cached_deployment('app')])
    worker = usage_worker(cache)
    started = real_time.monotonic()
    with pytest.raises(ReplayFidelityError, match='before the usage deadline'):
        worker.handle_set_usage_event(coordinated_event('app', dict(replicas=3), budget=.2))
    assert real_time.monotonic() - started < 5


def test_worker_reports_fidelity_failures_as_failed_outcome(caches):
    cache = caches()
    worker = usage_worker(cache)
    worker.CACHE_CATCH_UP_TIMEOUT = .05
    worker.status_queue = queue.Queue()
    worker.process_event(coordinated_event('gone', dict(replicas=1)), real_time.monotonic)
    messages = [worker.status_queue.get_nowait() for _ in range(worker.status_queue.qsize())]
    ack = messages[-1]
    assert ack['kind'] == 'ack' and ack['outcome'] == FAILED and ack['fidelity'] is True


def test_expired_event_issues_no_further_write():
    worker = usage_worker(None)
    worker.usage_executor = FairExecutor(1)
    release, calls = threading.Event(), []
    def blocking():
        calls.append('first')
        release.wait(5)
    worker.event_scope.deadline = real_time.monotonic() + .1
    timer = later(release.set, .3)
    try:
        with pytest.raises(ReplayFidelityError, match='usage writes'):
            worker.run_mutations(('ns', 'app'), [(blocking,)] + [(calls.append, index) for index in range(5)])
    finally:
        timer.join()
        worker.event_scope.deadline = None
        assert worker.usage_executor.shutdown(5)
    assert calls == ['first']


# ------------------------------------------------------------------ accounting

def test_tracker_limit_boundary_pending_and_setup_events():
    now = [100.0]
    tracker = UsageTracker(lambda: now[0], limit_logical=30)
    tracker.register(dict(namespace='ns', name='setup', event_id=1))
    timing = dict(scheduled_monotonic=100, speed_factor=2, trace_timestamp=0)
    tracker.register(dict(namespace='ns', name='a', event_id=2, replay_timing=timing))
    tracker.register(dict(namespace='ns', name='b', event_id=3, replay_timing=timing))
    now[0] = 115
    assert tracker.check_deadlines() is None  # Setup has no deadline; 30 logical s is valid.
    tracker.acknowledge(dict(event_id=2, outcome=APPLIED, completed=True, started_monotonic=101,
                             finished_monotonic=115))
    assert tracker.violation is None
    tracker.acknowledge(dict(event_id=3, outcome=APPLIED, completed=True, started_monotonic=101,
                             finished_monotonic=115.001))
    assert tracker.violation['reason'] == 'completion'
    assert tracker.violation['event_id'] == 3
    assert tracker.summary()['fidelity']['limit_logical'] == 30
    now[0] = 10000
    assert tracker.check_deadlines()['event_id'] == 3  # First violation is kept.


def test_replay_without_limit_keeps_legacy_completion():
    tracker = UsageTracker(lambda: 1000)
    tracker.register(dict(namespace='ns', name='a', event_id=1,
                          replay_timing=dict(scheduled_monotonic=0, speed_factor=2)))
    assert tracker.check_deadlines() is None
    tracker.acknowledge(dict(event_id=1, outcome=APPLIED, completed=True, finished_monotonic=1000))
    assert tracker.violation is None


def test_violation_while_waiting_for_admission_capacity_stops_replay(monkeypatch):
    """A full admission wait still raises the violation instead of a failed action."""
    run = build([], [{'timestamp': 0, 'workload_actions': [usage('a'), usage('b'), usage('c')]}],
                setup=['a', 'b', 'c'], duration=lambda event: None)
    monkeypatch.setattr(WorkloadManager, 'USAGE_ADMISSION_LIMIT', 2)
    now = run.time
    monkeypatch.setattr('workload.manager.time.monotonic', now.monotonic)
    run.workload.usage_manager.join = Mock()
    cancel = run.clock.cancel
    # The admission wait polls the cancel event; advance fake time instead.
    monkeypatch.setattr(cancel, 'wait', lambda seconds: now.wait(seconds))
    with pytest.raises(ReplayFidelityError):
        run.coordinator.run(run.clock)
    assert [entry[:2] for entry in run.log] == [('usage', 'a'), ('usage', 'b')]
    actions = run.workload.replay_diagnostics.snapshot()['actions']
    assert actions[-1]['completed'] is False and 'temporal fidelity' in actions[-1]['error']
