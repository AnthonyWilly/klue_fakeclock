import queue
import threading
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest
from kubernetes.client.rest import ApiException

from workload.usage.resource_cache import ResourceCache, ResourceCacheUnavailable
from workload.usage.usage_manager import UsageManager
from workload.usage.usage_tracking import APPLIED, EMPTY, TARGET_ABSENT

WAIT = 5


def pod(name, workload, namespace='ns', uid=None, cpu=None, phase='Running', deleted=False, version='1'):
    limits = {'cpu': cpu} if cpu is not None else None
    return NS(metadata=NS(name=name, namespace=namespace, uid=uid or f'{namespace}/{name}',
                          labels={'deployment': workload}, resource_version=version,
                          deletion_timestamp='now' if deleted else None),
              status=NS(phase=phase, conditions=[]),
              spec=NS(containers=[NS(resources=NS(limits=limits))]))


def deployment(name, namespace='ns', uid=None, generation=1):
    return NS(metadata=NS(name=name, namespace=namespace, uid=uid or f'd:{namespace}/{name}',
                          generation=generation, resource_version='1'))


class FakeList:
    """Paginated LIST; each full LIST moves to the next snapshot, if any."""

    def __init__(self, *snapshots):
        self.snapshots = list(snapshots)
        self.calls = []
        self.failures = 0
        self.listed = False

    def __call__(self, **kwargs):
        self.calls.append(kwargs)
        if self.failures:
            self.failures -= 1
            raise ApiException(status=500, reason='list failed')
        index = int(kwargs.get('_continue') or 0)
        if not index and self.listed and len(self.snapshots) > 1:
            self.snapshots.pop(0)
        self.listed = True
        pages, version = self.snapshots[0]
        token = str(index + 1) if index + 1 < len(pages) else None
        return NS(items=pages[index], metadata=NS(resource_version=version, _continue=token))


class FakeWatches:
    """Watch streams fed by the test: events, exceptions or 'END'."""

    def __init__(self):
        self.feeds = {}
        self.streams = []
        self.lock = threading.Condition()

    def feed(self, func):
        with self.lock:
            return self.feeds.setdefault(id(func), queue.Queue())

    def factory(self):
        watches = self

        class Watch:
            resource_version = None
            func = None

            def __init__(self):
                self.stopped = threading.Event()

            def stop(self):
                self.stopped.set()
                if self.func is not None:
                    watches.feed(self.func).put(None)

            def stream(self, func, **kwargs):
                self.func = func
                self.resource_version = kwargs['resource_version']
                with watches.lock:
                    watches.streams.append((func, kwargs))
                    watches.lock.notify_all()
                feed = watches.feed(func)
                while not self.stopped.is_set():
                    item = feed.get()
                    if item is None or item == 'END':
                        return
                    if isinstance(item, Exception):
                        raise item
                    if item['type'] != 'ERROR':
                        self.resource_version = item.get('version', self.resource_version)
                    yield item
        return Watch()

    def wait_streams(self, func, count):
        with self.lock:
            assert self.lock.wait_for(
                lambda: sum(f is func for f, _ in self.streams) >= count, WAIT)
            return [kwargs for f, kwargs in self.streams if f is func]


def event(kind, obj, version='2'):
    return dict(type=kind, object=obj, raw_object={}, version=version)


def make_cache(pods=(), deployments=(), pod_pages=None, start=True, **options):
    list_pods = FakeList((pod_pages or [list(pods)], 'p1'))
    list_deployments = FakeList(([list(deployments)], 'd1'))
    watches = FakeWatches()
    options.setdefault('backoff', .001)
    cache = ResourceCache(list_pods, list_deployments, watch_factory=watches.factory, **options)
    cache.fakes = NS(pods=list_pods, deployments=list_deployments, watches=watches)
    if start:
        cache.start().wait_ready(WAIT)
    return cache


def wait_for(cache, predicate):
    assert cache.wait_for(predicate, WAIT)


@pytest.fixture
def caches():
    created = []
    def build(*args, **kwargs):
        created.append(make_cache(*args, **kwargs))
        return created[-1]
    yield build
    for cache in created:
        assert cache.stop(WAIT)


def test_paginated_initial_list_is_ready_and_watch_continues_from_its_version(caches):
    cache = caches(pod_pages=[[pod('a', 'app')], [pod('b', 'app')]], deployments=[deployment('app')],
                   page_size=1)
    calls = cache.fakes.pods.calls
    assert [c.get('_continue') for c in calls] == [None, '1']
    assert all(c['limit'] == 1 and c['label_selector'] == 'deployment' for c in calls)
    assert cache.ready and cache.stats()['list_pages'] == 3
    [watch] = cache.fakes.watches.wait_streams(cache.fakes.pods, 1)
    assert watch['resource_version'] == 'p1' and watch['label_selector'] == 'deployment'
    assert cache.snapshot(('ns', 'app')).keys == {('ns', 'a'), ('ns', 'b')}
    cache.fakes.watches.feed(cache.fakes.pods).put(event('ADDED', pod('c', 'app')))
    wait_for(cache, lambda read: ('ns', 'c') in read(('ns', 'app')).keys)
    assert ('ns', 'app') in cache.take_dirty()


def test_scale_to_zero_differs_from_deletion_and_namespaces_are_separate(caches):
    cache = caches(pods=[pod('a', 'app', namespace='one')],
                   deployments=[deployment('app', 'one'), deployment('app', 'two')])
    one, two = cache.snapshot(('one', 'app')), cache.snapshot(('two', 'app'))
    assert one.keys == {('one', 'a')} and two.keys == set()
    assert two.deployment_uid == 'd:two/app'
    cache.fakes.watches.feed(cache.fakes.deployments).put(event('DELETED', deployment('app', 'two')))
    wait_for(cache, lambda read: read(('two', 'app')).deployment_uid is None)
    assert cache.snapshot(('one', 'app')).deployment_uid == 'd:one/app'


def test_only_relevant_pod_changes_mark_the_workload(caches):
    cache = caches(pods=[pod('a', 'app', cpu='1')], deployments=[deployment('app')])
    cache.take_dirty()
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    heartbeat = pod('a', 'app', cpu='1', version='5')
    heartbeat.status.conditions = ['Ready']
    feed.put(event('MODIFIED', heartbeat))
    wait_for(cache, lambda read: cache.stats()['events'] == 1)
    assert cache.take_dirty() == set()
    feed.put(event('MODIFIED', pod('a', 'app', cpu='2')))
    wait_for(cache, lambda read: cache.stats()['events'] == 2)
    assert cache.take_dirty() == {('ns', 'app')}
    assert dict(next(iter(cache.snapshot(('ns', 'app')).signature[1]))[3]) == {'cpu': 2.0}
    feed.put(event('MODIFIED', pod('a', 'app', cpu='2', phase='Succeeded')))
    wait_for(cache, lambda read: not read(('ns', 'app')).keys)
    assert cache.take_dirty() == {('ns', 'app')}


def test_recreated_uid_replaces_state_and_old_deletion_is_ignored(caches):
    cache = caches(pods=[pod('a', 'app', uid='old')], deployments=[deployment('app')])
    before = cache.snapshot(('ns', 'app')).signature
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    feed.put(event('ADDED', pod('a', 'app', uid='new')))
    feed.put(event('DELETED', pod('a', 'app', uid='old')))
    wait_for(cache, lambda read: cache.stats()['events'] == 2)
    view = cache.snapshot(('ns', 'app'))
    assert view.keys == {('ns', 'a')} and view.pods.items[0].metadata.uid == 'new'
    assert view.signature != before
    assert cache.stats()['stale_events'] == 1 and cache.stats()['pods'] == 1


def test_expired_version_relists_and_swaps_atomically(caches):
    cache = caches(pods=[pod('a', 'app')], deployments=[deployment('app')])
    cache.fakes.pods.snapshots.append(([[pod('b', 'app')]], 'p9'))
    cache.take_dirty()
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    feed.put(ApiException(status=410, reason='Gone'))
    streams = cache.fakes.watches.wait_streams(cache.fakes.pods, 2)
    assert streams[-1]['resource_version'] == 'p9'
    assert cache.snapshot(('ns', 'app')).keys == {('ns', 'b')}
    stats = cache.stats()
    assert (stats['relists'], stats['lists']) == (1, 3)
    assert cache.take_dirty() == {('ns', 'app')}


def test_watch_error_event_410_also_relists(caches):
    cache = caches(pods=[pod('a', 'app')])
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    feed.put(dict(type='ERROR', object={}, raw_object={'code': 410, 'message': 'too old'}))
    cache.fakes.watches.wait_streams(cache.fakes.pods, 2)
    assert cache.stats()['relists'] == 1


def test_disconnection_resumes_from_last_version_without_relisting(caches):
    cache = caches(pods=[pod('a', 'app')])
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    feed.put(event('ADDED', pod('b', 'app'), version='p7'))
    feed.put(ConnectionError('reset'))
    streams = cache.fakes.watches.wait_streams(cache.fakes.pods, 2)
    assert streams[-1]['resource_version'] == 'p7'
    feed.put('END')
    streams = cache.fakes.watches.wait_streams(cache.fakes.pods, 3)
    assert streams[-1]['resource_version'] == 'p7'
    stats = cache.stats()
    assert (stats['reconnects'], stats['watch_restarts'], stats['relists'], stats['lists']) == (1, 1, 0, 2)
    assert cache.healthy


def test_unavailable_cache_is_an_error_not_a_stale_answer(caches):
    cache = caches(start=False)
    cache.fakes.pods.failures = 10 ** 6
    cache.start()
    with pytest.raises(ResourceCacheUnavailable, match='not ready'):
        cache.wait_ready(.2)
    with pytest.raises(ResourceCacheUnavailable, match='unhealthy'):
        cache.snapshot(('ns', 'app'), timeout=.1)
    assert cache.stats()['reconnects'] > 0


def test_repeated_watch_failures_make_reads_wait_then_fail(caches):
    cache = caches(pods=[pod('a', 'app')])
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    for _ in range(3):
        feed.put(ConnectionError('down'))
    cache.fakes.watches.wait_streams(cache.fakes.pods, 4)
    with pytest.raises(ResourceCacheUnavailable, match='unhealthy'):
        cache.snapshot(('ns', 'app'), timeout=.1)
    feed.put(event('MODIFIED', pod('a', 'app')))
    assert cache.snapshot(('ns', 'app'), timeout=WAIT).keys == {('ns', 'a')}


def test_concurrent_snapshots_are_coherent(caches):
    cache = caches(deployments=[deployment('app')])
    feed = cache.fakes.watches.feed(cache.fakes.pods)
    errors = []
    def read():
        for _ in range(300):
            view = cache.snapshot(('ns', 'app'))
            if {(n, p) for n, p, *_ in view.signature[1]} != view.keys:
                errors.append(view)
    readers = [threading.Thread(target=read) for _ in range(3)]
    for reader in readers:
        reader.start()
    for i in range(200):
        feed.put(event('ADDED', pod(f'p{i}', 'app')))
    wait_for(cache, lambda read: len(read(('ns', 'app')).keys) == 200)
    for reader in readers:
        reader.join(WAIT)
    assert not errors


def test_stop_without_events_closes_idle_watches(caches):
    cache = make_cache()
    cache.fakes.watches.wait_streams(cache.fakes.pods, 1)
    cache.fakes.watches.wait_streams(cache.fakes.deployments, 1)
    assert cache.stop(WAIT)
    assert not any(r.thread.is_alive() for r in cache.reflectors)
    with pytest.raises(ResourceCacheUnavailable):
        cache.snapshot(('ns', 'app'), timeout=.1)


# ---------------------------------------------------------------- UsageManager


def usage_worker(cache):
    worker = UsageManager(queue.Queue())
    worker.k8s_api = Mock()
    worker.resource_cache = cache
    worker.assignment_engine = Mock()
    worker.assignment_engine.resolve.side_effect = lambda **kw: {
        key: dict(cpu=.1, memory=1) for key in kw['emulated_pods']}
    worker.apply_assignment = Mock()
    return worker


def usage_event(name, event_id=1):
    return dict(namespace='ns', name=name, pods={'real': dict(cpu=.1, memory=1)}, event_id=event_id)


def test_thousands_of_events_and_ticks_issue_no_discovery_requests(caches):
    workloads = 1000
    cache = caches(pods=[pod(f'p{i}', f'app{i}') for i in range(workloads)],
                   deployments=[deployment(f'app{i}') for i in range(workloads)])
    worker = usage_worker(cache)
    for round_ in range(3):
        for i in range(workloads):
            assert worker.handle_set_usage_event(usage_event(f'app{i}')) == APPLIED
        for _ in range(20):
            worker.reconcile_tracked_workloads()
    assert len(cache.fakes.pods.calls) == len(cache.fakes.deployments.calls) == 1
    worker.k8s_api.v1.list_namespaced_pod.assert_not_called()
    worker.k8s_api.read_namespaced_deployment.assert_not_called()
    assert worker.apply_assignment.call_count == 3 * workloads
    # A real topology change still reaches the assignment on the next tick.
    cache.fakes.watches.feed(cache.fakes.pods).put(event('ADDED', pod('extra', 'app7')))
    wait_for(cache, lambda read: len(read(('ns', 'app7')).keys) == 2)
    worker.reconcile_tracked_workloads()
    assert worker.apply_assignment.call_count == 3 * workloads + 1
    assert worker.last_emulated_pods[('ns', 'app7')] == {('ns', 'p7'), ('ns', 'extra')}


def test_scaled_to_zero_is_empty_without_reading_the_deployment(caches):
    cache = caches(deployments=[deployment('app')])
    worker = usage_worker(cache)
    assert worker.handle_set_usage_event(usage_event('app')) == EMPTY
    worker.k8s_api.read_namespaced_deployment.assert_not_called()


def test_absence_is_confirmed_once_until_the_deployment_changes(caches):
    cache = caches()
    worker = usage_worker(cache)
    worker.cleanup_workload = Mock()
    worker.k8s_api.read_namespaced_deployment.side_effect = ApiException(status=404)
    for event_id in range(1, 50):
        assert worker.handle_set_usage_event(usage_event('gone', event_id)) == TARGET_ABSENT
    assert worker.k8s_api.read_namespaced_deployment.call_count == 1
    assert cache.stats()['absence_confirmations'] == 1
    feed = cache.fakes.watches.feed(cache.fakes.deployments)
    feed.put(event('ADDED', deployment('gone')))
    feed.put(event('DELETED', deployment('gone')))
    wait_for(cache, lambda read: read(('ns', 'gone')).deployment_version == 2)
    assert worker.handle_set_usage_event(usage_event('gone')) == TARGET_ABSENT
    assert worker.k8s_api.read_namespaced_deployment.call_count == 2


def test_deployment_not_yet_observed_is_awaited_not_reported_absent(caches):
    cache = caches()
    worker = usage_worker(cache)
    feed = cache.fakes.watches.feed(cache.fakes.deployments)
    def read(**kwargs):
        feed.put(event('ADDED', deployment('new')))
        return deployment('new')
    worker.k8s_api.read_namespaced_deployment.side_effect = read
    assert worker.handle_set_usage_event(usage_event('new')) == EMPTY
    worker.CACHE_CATCH_UP_TIMEOUT = .1
    worker.k8s_api.read_namespaced_deployment.side_effect = None
    with pytest.raises(RuntimeError, match='did not observe'):
        worker.handle_set_usage_event(usage_event('lagging'))
    assert ('ns', 'lagging') in worker.retry_workloads


def test_periodic_failure_of_one_workload_does_not_block_others_and_is_retried(caches):
    cache = caches(pods=[pod('a', 'one'), pod('b', 'two')],
                   deployments=[deployment('one'), deployment('two')])
    worker = usage_worker(cache)
    worker.status_queue = queue.Queue()
    worker.last_usage_actions = {('ns', 'one'): {}, ('ns', 'two'): {}}
    worker.apply_assignment.side_effect = [RuntimeError('denied'), None, None]
    worker.reconcile_tracked_workloads()
    assert worker.last_emulated_pods == {('ns', 'two'): {('ns', 'b')}}
    assert worker.retry_workloads == {('ns', 'one')}
    worker.reconcile_tracked_workloads()
    assert worker.last_emulated_pods[('ns', 'one')] == {('ns', 'a')}
    assert not worker.retry_workloads
    worker.reconcile_tracked_workloads()
    assert worker.apply_assignment.call_count == 3


def test_worker_waits_for_ready_cache_and_stops_it(monkeypatch):
    statuses, events = queue.Queue(), queue.Queue()
    events.put('STOP')
    worker = UsageManager(events, status_queue=statuses)
    built = []
    def factory(*args, **kwargs):
        cache = make_cache(start=False)
        cache.on_update = kwargs['on_update']
        built.append(cache)
        return cache
    monkeypatch.setattr('workload.usage.usage_manager.K8SAPI', Mock())
    monkeypatch.setattr('workload.usage.usage_manager.ResourceCache', factory)
    worker.run()
    [cache] = built
    assert cache.stopping.is_set()
    assert not any(r.thread.is_alive() for r in cache.reflectors)
    messages = [statuses.get_nowait() for _ in range(statuses.qsize())]
    assert messages[-1] == dict(kind='stopped')
    assert messages[-2]['kind'] == 'cache' and messages[-2]['stats']['ready']


def test_worker_reports_unready_cache_as_fatal(monkeypatch):
    statuses = queue.Queue()
    worker = UsageManager(queue.Queue(), status_queue=statuses)
    worker.CACHE_READY_TIMEOUT = .1
    def factory(*args, **kwargs):
        cache = make_cache(start=False)
        cache.fakes.pods.failures = 10 ** 6
        return cache
    monkeypatch.setattr('workload.usage.usage_manager.K8SAPI', Mock())
    monkeypatch.setattr('workload.usage.usage_manager.ResourceCache', factory)
    with pytest.raises(ResourceCacheUnavailable):
        worker.run()
    messages = [statuses.get_nowait() for _ in range(statuses.qsize())]
    assert messages[-1]['kind'] == 'fatal' and 'not ready' in messages[-1]['message']
