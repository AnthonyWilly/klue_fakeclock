"""Observed cache of emulated pods and deployments for usage reconciliation.

One paginated LIST per resource establishes a consistent snapshot; a WATCH
resumed from the listed ``resourceVersion`` keeps it current. Usage
reconciliation then reads workload topology from memory instead of issuing a
LIST or GET per event or per periodic tick.

Coherence rules:

- ``resourceVersion`` is opaque and specific to each resource. It is only
  passed back to the API server, never compared.
- Objects are identified by UID. A DELETED event whose UID differs from the
  object currently indexed under that name is stale and ignored, so a recreated
  object with the same name is never removed by an old deletion.
- A disconnected watch resumes from the last observed version. An expired
  version (410) triggers a fresh LIST whose result replaces the indexes
  atomically. While a resource is relisting or repeatedly failing, the cache is
  unhealthy and readers wait a bounded time or receive an error; they never
  operate indefinitely on stale state.
- Only changes relevant to usage assignment mark a workload as changed: the
  live pod set and UIDs, pod limits and deployment identity. Status heartbeats
  and irrelevant metadata do not.
"""
import threading
import time
from types import SimpleNamespace

from kubernetes import watch as k8s_watch
from kubernetes.client.rest import ApiException
from kubernetes.utils.quantity import parse_quantity

HTTP_GONE = 410
UNHEALTHY_AFTER_FAILURES = 3


class ResourceCacheUnavailable(RuntimeError):
    """The cache cannot currently prove a coherent view of the cluster."""


def is_live_pod(pod):
    if pod.metadata.deletion_timestamp is not None:
        return False
    return pod.status is None or pod.status.phase not in {"Succeeded", "Failed"}


def pod_limits(pod):
    """Summed container limits; a resource without any limit is omitted."""
    cpu_total = memory_total = None
    for container in (pod.spec.containers if pod.spec else None) or []:
        resources = container.resources
        limits = resources.limits if resources and resources.limits else {}
        if limits.get("cpu") is not None:
            cpu_total = (cpu_total or 0) + parse_quantity(str(limits["cpu"]))
        if limits.get("memory") is not None:
            memory_total = (memory_total or 0) + parse_quantity(str(limits["memory"]))
    result = {}
    if cpu_total is not None:
        result["cpu"] = float(cpu_total)
    if memory_total is not None:
        result["memory"] = int(memory_total)
    return result


def workload_label(pod):
    return (pod.metadata.labels or {}).get("deployment")


class _Reflector:
    """LIST then WATCH one resource, feeding the shared cache."""

    def __init__(self, cache, kind, list_fn, action, selector):
        self.cache = cache
        self.kind = kind
        self.list_fn = list_fn
        self.action = action
        self.selector = selector
        self.resource_version = None
        self.synced = False
        self.relisting = True
        self.failures = 0
        self.watch = None
        self.thread = None

    @property
    def healthy(self):
        return self.synced and not self.relisting and self.failures < UNHEALTHY_AFTER_FAILURES

    def list_all(self):
        """Paginated LIST; every page shares the first page's snapshot."""
        items, token, version = [], None, None
        while True:
            kwargs = dict(limit=self.cache.page_size)
            if self.selector:
                kwargs["label_selector"] = self.selector
            if token:
                kwargs["_continue"] = token
            page = self.cache.call(self.action, self.list_fn, **kwargs)
            self.cache.count("list_pages")
            items.extend(page.items or [])
            version = page.metadata.resource_version
            token = getattr(page.metadata, "_continue", None)
            if not token:
                return items, version

    def run(self):
        cache = self.cache
        while not cache.stopping.is_set():
            if self.relisting:
                try:
                    items, version = self.list_all()
                except Exception as error:
                    self.fail(error)
                    continue
                if cache.stopping.is_set():
                    return
                cache.replace(self.kind, items)
                self.resource_version = version
                with cache.condition:
                    self.relisting = False
                    self.synced = True
                    self.failures = 0
                    cache.condition.notify_all()
                cache.count("lists")
                cache.publish()
            try:
                self.watch_once()
            except ApiException as error:
                if error.status == HTTP_GONE:
                    self.expire()
                else:
                    self.fail(error)
            except Exception as error:
                self.fail(error)

    def watch_once(self):
        cache = self.cache
        self.watch = cache.watch_factory()
        kwargs = dict(resource_version=self.resource_version, timeout_seconds=cache.watch_timeout,
                      allow_watch_bookmarks=True, _request_timeout=cache.watch_timeout + 30)
        if self.selector:
            kwargs["label_selector"] = self.selector
        cache.count("watches")
        try:
            for event in self.watch.stream(self.list_fn, **kwargs):
                if cache.stopping.is_set():
                    return
                if self.failures:
                    with cache.condition:
                        self.failures = 0
                        cache.condition.notify_all()
                kind = event["type"]
                if kind == "ERROR":
                    raw = event.get("raw_object") or {}
                    raise ApiException(status=raw.get("code"), reason=raw.get("message"))
                if kind != "BOOKMARK":
                    cache.observe(self.kind, kind, event["object"])
                self.resource_version = getattr(self.watch, "resource_version", None) or self.resource_version
        finally:
            self.watch = None
        if not cache.stopping.is_set():
            # The server ended an idle stream; resume from the same version.
            with cache.condition:
                self.failures = 0
                cache.condition.notify_all()
            cache.count("watch_restarts")

    def expire(self):
        with self.cache.condition:
            self.relisting = True
            self.cache.condition.notify_all()
        self.cache.count("relists")
        self.cache.publish()

    def fail(self, error):
        with self.cache.condition:
            self.failures += 1
            failures = self.failures
            self.cache.last_error = f"{self.kind}: {error}"
            self.cache.condition.notify_all()
        self.cache.count("reconnects")
        self.cache.publish()
        self.cache.stopping.wait(min(self.cache.backoff * 2 ** (failures - 1), self.cache.max_backoff))


class ResourceCache:
    """In-memory, UID-keyed view of emulated pods and deployments."""

    def __init__(self, list_pods, list_deployments, call=None, watch_factory=k8s_watch.Watch,
                 on_update=None, page_size=500, watch_timeout=60, backoff=.5, max_backoff=10,
                 monotonic=time.monotonic):
        self.call = call or (lambda action, function, **kwargs: function(**kwargs))
        self.watch_factory = watch_factory
        self.on_update = on_update
        self.page_size = page_size
        self.watch_timeout = watch_timeout
        self.backoff = backoff
        self.max_backoff = max_backoff
        self.monotonic = monotonic
        self.condition = threading.Condition()
        self.stopping = threading.Event()
        self.last_error = None
        self.counters = dict(lists=0, list_pages=0, relists=0, watches=0, watch_restarts=0,
                             reconnects=0, events=0, stale_events=0, relevant_changes=0,
                             absence_confirmations=0)
        self.started_monotonic = None
        self.ready_real = None
        # Pods: UID -> pod, (namespace, name) -> UID, workload -> {UID: pod}.
        self.pods = {}
        self.pod_names = {}
        self.workload_pods = {}
        # Deployments: (namespace, name) -> deployment.
        self.deployments = {}
        # Incremented whenever a deployment key is observed to change; absence
        # confirmed at an older version is no longer valid.
        self.deployment_versions = {}
        self.confirmed_absent = {}
        self.dirty = set()
        # Incremented on every relevant topology change.
        self.generation = 0
        self.reflectors = (
            _Reflector(self, "pods", list_pods, "list-pods", "deployment"),
            _Reflector(self, "deployments", list_deployments, "list-deployments", None),
        )

    # ---------------------------------------------------------------- lifecycle

    def start(self):
        self.started_monotonic = self.monotonic()
        for reflector in self.reflectors:
            reflector.thread = threading.Thread(target=reflector.run, daemon=True,
                                                name=f"resource-cache-{reflector.kind}")
            reflector.thread.start()
        return self

    @property
    def ready(self):
        return all(reflector.synced for reflector in self.reflectors)

    @property
    def healthy(self):
        return not self.stopping.is_set() and all(r.healthy for r in self.reflectors)

    def wait_ready(self, timeout):
        with self.condition:
            self.condition.wait_for(lambda: self.ready or self.stopping.is_set(), timeout)
            if not self.ready:
                raise ResourceCacheUnavailable(
                    f"resource cache not ready within {timeout} s: {self.last_error}")
            if self.ready_real is None:
                self.ready_real = self.monotonic() - self.started_monotonic
        self.publish()

    def stop(self, timeout=5):
        """Close watches and join threads; return True when none remain."""
        self.stopping.set()
        with self.condition:
            self.condition.notify_all()
        for reflector in self.reflectors:
            current = reflector.watch
            if current is not None:
                current.stop()
        deadline = self.monotonic() + timeout
        for reflector in self.reflectors:
            if reflector.thread is not None:
                reflector.thread.join(max(0, deadline - self.monotonic()))
        return not any(r.thread is not None and r.thread.is_alive() for r in self.reflectors)

    # ---------------------------------------------------------------- readers

    def _wait_healthy(self, timeout):
        if not self.condition.wait_for(lambda: self.healthy or self.stopping.is_set(), timeout):
            raise ResourceCacheUnavailable(
                f"resource cache unhealthy for more than {timeout} s: {self.last_error}")
        if self.stopping.is_set():
            raise ResourceCacheUnavailable("resource cache stopped")

    def snapshot(self, workload_key, timeout=30):
        """Immutable view of one workload from a healthy cache."""
        with self.condition:
            self._wait_healthy(timeout)
            return self._view(workload_key)

    def _view(self, workload_key):
        pods = sorted(self.workload_pods.get(workload_key, {}).values(),
                      key=lambda pod: pod.metadata.name)
        live = [pod for pod in pods if is_live_pod(pod)]
        deployment = self.deployments.get(workload_key)
        return SimpleNamespace(
            key=workload_key,
            pods=SimpleNamespace(items=pods),
            keys={(pod.metadata.namespace, pod.metadata.name) for pod in live},
            signature=self._signature(workload_key),
            deployment_uid=deployment.metadata.uid if deployment else None,
            deployment_generation=deployment.metadata.generation if deployment else None,
            deployment_replicas=getattr(getattr(deployment, "spec", None), "replicas", None),
            deployment_version=self.deployment_versions.get(workload_key, 0),
            absence_confirmed=(deployment is None and self.confirmed_absent.get(workload_key)
                               == self.deployment_versions.get(workload_key, 0)),
            generation=self.generation)

    def confirm_absent(self, view):
        """Record an authoritative absence unless the key changed since ``view``."""
        with self.condition:
            if self.deployment_versions.get(view.key, 0) == view.deployment_version:
                self.confirmed_absent[view.key] = view.deployment_version
                self.counters["absence_confirmations"] += 1

    def wait_for(self, predicate, timeout):
        """Wait until ``predicate(view_function)`` holds on a healthy cache."""
        with self.condition:
            return self.condition.wait_for(
                lambda: self.stopping.is_set() or (self.healthy and predicate(self._view)), timeout
            ) and not self.stopping.is_set()

    def take_dirty(self):
        with self.condition:
            dirty, self.dirty = self.dirty, set()
            return dirty

    def stats(self):
        with self.condition:
            return dict(self.counters, ready=self.ready, healthy=self.healthy,
                        ready_real=self.ready_real, generation=self.generation,
                        pods=len(self.pods), deployments=len(self.deployments),
                        last_error=self.last_error)

    def publish(self):
        if self.on_update is not None:
            self.on_update(self.stats())

    def count(self, name):
        with self.condition:
            self.counters[name] += 1

    # ---------------------------------------------------------------- writers

    def _signature(self, workload_key):
        deployment = self.deployments.get(workload_key)
        pods = frozenset(
            (pod.metadata.namespace, pod.metadata.name, pod.metadata.uid,
             tuple(sorted(pod_limits(pod).items())))
            for pod in self.workload_pods.get(workload_key, {}).values() if is_live_pod(pod))
        return deployment.metadata.uid if deployment else None, pods

    def _changed(self, keys, before):
        for key in keys:
            if before[key] != self._signature(key):
                self.dirty.add(key)
                self.generation += 1
                self.counters["relevant_changes"] += 1

    def _remove_pod(self, uid):
        pod = self.pods.pop(uid)
        name = (pod.metadata.namespace, pod.metadata.name)
        if self.pod_names.get(name) == uid:
            del self.pod_names[name]
        owner = (pod.metadata.namespace, workload_label(pod))
        bucket = self.workload_pods.get(owner, {})
        bucket.pop(uid, None)
        if not bucket:
            self.workload_pods.pop(owner, None)
        return owner

    def _insert_pod(self, pod):
        uid = pod.metadata.uid
        self.pods[uid] = pod
        self.pod_names[(pod.metadata.namespace, pod.metadata.name)] = uid
        owner = (pod.metadata.namespace, workload_label(pod))
        self.workload_pods.setdefault(owner, {})[uid] = pod
        return owner

    def _set_deployment(self, key, deployment):
        current = self.deployments.get(key)
        if deployment is None:
            self.deployments.pop(key, None)
        else:
            self.deployments[key] = deployment
        old_uid = current.metadata.uid if current else None
        new_uid = deployment.metadata.uid if deployment else None
        old_generation = current.metadata.generation if current else None
        new_generation = deployment.metadata.generation if deployment else None
        if (old_uid, old_generation) != (new_uid, new_generation):
            self.deployment_versions[key] = self.deployment_versions.get(key, 0) + 1

    def observe(self, kind, event_type, obj):
        with self.condition:
            self.counters["events"] += 1
            if kind == "pods":
                self._observe_pod(event_type, obj)
            else:
                self._observe_deployment(event_type, obj)
            self.condition.notify_all()

    def _observe_pod(self, event_type, pod):
        uid = pod.metadata.uid
        name = (pod.metadata.namespace, pod.metadata.name)
        current_uid = self.pod_names.get(name)
        if event_type == "DELETED":
            if uid not in self.pods:
                # Unknown UID: an older incarnation already replaced or removed.
                self.counters["stale_events"] += 1
                return
            owner = (pod.metadata.namespace, workload_label(self.pods[uid]))
            before = {owner: self._signature(owner)}
            self._remove_pod(uid)
            self._changed(before, before)
            return
        affected = {(pod.metadata.namespace, workload_label(pod))}
        replaced = [old for old in (uid, current_uid) if old in self.pods]
        for old in replaced:
            affected.add((self.pods[old].metadata.namespace, workload_label(self.pods[old])))
        before = {key: self._signature(key) for key in affected}
        for old in set(replaced):
            self._remove_pod(old)
        self._insert_pod(pod)
        self._changed(affected, before)

    def _observe_deployment(self, event_type, deployment):
        key = (deployment.metadata.namespace, deployment.metadata.name)
        current = self.deployments.get(key)
        if event_type == "DELETED" and (current is None or current.metadata.uid != deployment.metadata.uid):
            self.counters["stale_events"] += 1
            return
        before = {key: self._signature(key)}
        self._set_deployment(key, None if event_type == "DELETED" else deployment)
        self._changed(before, before)

    def replace(self, kind, items):
        """Swap one resource's indexes for a fresh LIST, marking real changes."""
        with self.condition:
            if kind == "pods":
                affected = set(self.workload_pods) | {
                    (pod.metadata.namespace, workload_label(pod)) for pod in items}
                before = {key: self._signature(key) for key in affected}
                self.pods, self.pod_names, self.workload_pods = {}, {}, {}
                for pod in items:
                    self._insert_pod(pod)
            else:
                listed = {(obj.metadata.namespace, obj.metadata.name): obj for obj in items}
                affected = set(self.deployments) | set(listed)
                before = {key: self._signature(key) for key in affected}
                for key in affected:
                    self._set_deployment(key, listed.get(key))
            self._changed(affected, before)
            self.condition.notify_all()
