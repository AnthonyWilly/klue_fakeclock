import multiprocessing
import queue
import time
import math
from contextlib import nullcontext
import threading

from kubernetes.client.rest import ApiException
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_objects.cluster_resource_usage_generator import ClusterResourceUsageGenerator
from workload.usage.event_dispatcher import EventDispatcher, FairExecutor
from workload.usage.resource_cache import ResourceCache, is_live_pod, pod_limits
from workload.usage.usage_assignment import UsageAssignmentEngine
from workload.usage.usage_tracking import APPLIED, CANCELLED, EMPTY, FAILED, TARGET_ABSENT


class UsageManager(multiprocessing.Process):
    """
    Background process responsible for managing KWOK ClusterResourceUsage objects.

    - Receives set-usage events with real pod-level usage snapshots.
    - Tracks workloads by (namespace, name).
    - Observes emulated pods and deployments through a LIST/WATCH cache.
    - Reconciles when:
        1. a new set-usage event arrives; or
        2. the cache reports a relevant change of a tracked workload.
    - Delegates usage distribution to UsageAssignmentEngine.
    - Applies the returned assignment as one ClusterResourceUsage per emulated pod.

    Events and reconciliations of one (namespace, workload) run in FIFO order,
    one at a time; independent workloads run on ``event_workers`` threads. All
    ClusterResourceUsage mutations share one pool of ``API_CONCURRENCY`` calls.
    """

    TIME_OUT = 200
    API_CONCURRENCY = 4
    RECONCILE_INTERVAL_SECONDS = 5
    CACHE_READY_TIMEOUT = 120
    CACHE_HEALTH_TIMEOUT = 30
    CACHE_STOP_TIMEOUT = 5
    # Real time a deployment confirmed by the API may take to reach the cache.
    CACHE_CATCH_UP_TIMEOUT = 10
    # Real time accepted events get to finish after a failure or interruption.
    CANCEL_TIMEOUT = 5
    WRITER_STOP_TIMEOUT = 5

    CRU_GROUP = "kwok.x-k8s.io"
    CRU_VERSION = "v1alpha1"
    CRU_PLURAL = "clusterresourceusages"

    def __init__(self, queue, reconcile_interval_seconds=RECONCILE_INTERVAL_SECONDS, status_queue=None,
                 event_workers=1):
        super().__init__()
        if not math.isfinite(reconcile_interval_seconds) or reconcile_interval_seconds <= 0:
            raise ValueError("reconciliation interval must be positive and finite")
        if event_workers < 1:
            raise ValueError("usage worker needs at least one event worker")
        self.queue = queue
        self.event_workers = event_workers
        self.status_queue = status_queue
        self.reconcile_interval_seconds = reconcile_interval_seconds

        # (namespace, workload_name) -> {
        #   (namespace, real_pod_name): {"cpu": float, "memory": float}
        # }
        self.last_usage_actions = {}

        # (namespace, workload_name) -> {(namespace, emulated_pod_name), ...}
        self.last_emulated_pods = {}

        # (namespace, workload_name) -> set(cru_name)
        self.managed_crus_by_workload = {}

        # (namespace, workload_name) -> cache signature committed with the
        # last successful assignment.
        self.last_signatures = {}

        # Workloads whose reconciliation failed and must be examined again.
        self.retry_workloads = set()

        # Workloads with a periodic reconciliation queued and not yet started.
        self.queued_reconciliations = set()

        # set(cru_name)
        self.existing_crus = set()
        # Guards the containers above. Each workload's entries are only changed
        # by its own serialized task; the lock protects the shared containers.
        self.state_lock = threading.RLock()
        self.resource_cache = None
        self.api_stats = {}
        self.api_lock = None
        self.usage_executor = None
        self.dispatcher = None

    def log(self, message):
        print(f"[USAGE MANAGER] {message}", flush=True)

    def run(self):
        """
        Main execution loop.

        Process every event and reconcile on monotonic periodic deadlines,
        including when the queue remains continuously occupied.
        """

        try:
            self.k8s_api = K8SAPI(timeout=self.TIME_OUT)
            self.assignment_engine = UsageAssignmentEngine()
            self.cru_generator = ClusterResourceUsageGenerator()
            self.api_lock = threading.Lock()
            self.resource_cache = ResourceCache(
                self.k8s_api.v1.list_pod_for_all_namespaces,
                self.k8s_api.apps_v1.list_deployment_for_all_namespaces,
                call=self.api_call, on_update=self.emit_cache)
            try:
                # Events wait in the queue until the cache can answer them.
                self.resource_cache.start().wait_ready(self.CACHE_READY_TIMEOUT)
                # One bounded writer for every task; tasks wait for their own writes.
                self.usage_executor = FairExecutor(self.API_CONCURRENCY)
                try:
                    self.process_events()
                finally:
                    if not self.usage_executor.shutdown(self.WRITER_STOP_TIMEOUT):
                        self.log("[WARN] usage writers did not stop in time")
            finally:
                if not self.resource_cache.stop(self.CACHE_STOP_TIMEOUT):
                    self.log("[WARN] resource cache threads did not stop in time")
        except BaseException as error:
            self.emit(kind='fatal', message=str(error))
            raise

    def api_call(self, action, function, **kwargs):
        """Bound requests in real time and expose activity while draining backlog."""
        started = time.monotonic()
        failed = False
        try:
            return function(_request_timeout=self.TIME_OUT, **kwargs)
        except Exception:
            failed = True
            raise
        finally:
            duration = time.monotonic() - started
            with self.api_lock or nullcontext():
                stats = self.api_stats.setdefault(action, dict(calls=0, failures=0, total_real=0, max_real=0))
                stats['calls'] += 1
                stats['failures'] += int(failed)
                stats['total_real'] += duration
                stats['max_real'] = max(stats['max_real'], duration)
                self.emit(kind='activity', action=action, stats=dict(stats))

    def emit(self, **message):
        if self.status_queue is not None:
            self.status_queue.put(message)

    def emit_cache(self, stats=None):
        if self.resource_cache is not None:
            self.emit(kind='cache', stats=stats or self.resource_cache.stats())

    def emit_concurrency(self):
        dispatcher, writer = self.dispatcher, self.usage_executor
        stats = dict(event_workers=self.event_workers, mutation_workers=self.API_CONCURRENCY)
        if dispatcher is not None:
            events = dispatcher.stats()
            stats.update(max_active_events=events['max_active'], buffered_events=events['buffered'],
                         max_buffered_events=events['max_buffered'],
                         cancelled_events=events['cancelled'])
        if writer is not None:
            stats['max_active_mutations'] = writer.stats()['max_running']
        self.emit(kind='concurrency', stats=stats)

    def process_events(self, monotonic=None):
        """Receive events and keep the periodic grid while workers process them.

        Events of one workload keep FIFO order with each other and with its
        periodic reconciliations; independent workloads run concurrently. A
        busy queue cannot starve reconciliation. STOP ends admission; it is
        confirmed only after every accepted event reached its terminal outcome.
        On failure or interruption, events without an outcome are reported
        cancelled within ``CANCEL_TIMEOUT`` real seconds.
        """
        monotonic = monotonic or time.monotonic
        self.dispatcher = EventDispatcher(self.event_workers)
        try:
            self._receive(monotonic)
        except BaseException:
            if not self.dispatcher.cancel(self.CANCEL_TIMEOUT):
                self.log("[WARN] usage event threads did not stop in time")
            self.emit_concurrency()
            raise
        finally:
            self.dispatcher = None

    def _receive(self, monotonic):
        dispatcher = self.dispatcher
        deadline = monotonic() + self.reconcile_interval_seconds
        while True:
            dispatcher.check()
            now = monotonic()
            if now >= deadline:
                try:
                    self.reconcile_tracked_workloads()
                except Exception as error:
                    self.emit(kind='error', message=str(error), action='periodic-reconciliation')
                self.emit_concurrency()
                # Skip expired ticks, never shift the periodic grid to event arrival.
                now = monotonic()
                deadline += (int((now - deadline) // self.reconcile_interval_seconds) + 1) * self.reconcile_interval_seconds
            try:
                event = self.queue.get(timeout=max(0, deadline - monotonic()))
            except queue.Empty:
                continue
            if event == "STOP":
                self.log("Stopping UsageManager")
                # The parent bounds a stalled stop; the worker never drops accepted work.
                while not dispatcher.wait_idle(1):
                    dispatcher.check()
                dispatcher.check()
                if not dispatcher.close(self.CANCEL_TIMEOUT):
                    raise RuntimeError("usage event threads did not stop after their work finished")
                self.emit_concurrency()
                self.emit_cache()
                self.emit(kind='stopped')
                break
            self.submit_event(event, monotonic)

    def submit_event(self, event, monotonic):
        key = (event.get('namespace'), event.get('name'))
        self.dispatcher.submit(
            key, lambda claim: self.process_event(event, monotonic, claim),
            on_cancel=lambda: self.cancel_event(event, monotonic))

    @staticmethod
    def event_context(event):
        timing = event.get('replay_timing') or {}
        return dict(event_id=event.get('event_id'), namespace=event.get('namespace'),
                    resource=event.get('name'), trace_timestamp=timing.get('trace_timestamp'))

    def cancel_event(self, event, monotonic):
        """Terminal outcome of an event abandoned before it reported one."""
        self.emit(kind='ack', outcome=CANCELLED, completed=False, finished_monotonic=monotonic(),
                  message='usage event cancelled before completion', **self.event_context(event))

    def process_event(self, event, monotonic, claim=None):
        """Emit one start and exactly one terminal outcome for an accepted event.

        ``lag_real`` keeps its meaning: delay until the worker started the
        event. Completion lag is derived from ``finished_monotonic``. When
        ``claim`` is given, the outcome is reported only if this call wins it;
        an event already reported cancelled never reports another outcome.
        """
        timing = event.get('replay_timing') or {}
        started = monotonic()
        lag = max(0, started - timing.get('scheduled_monotonic', started))
        context = self.event_context(event)
        self.emit(kind='started', event_id=context['event_id'], started_monotonic=started)
        def acknowledge(outcome, **fields):
            finished = monotonic()
            if claim is None or claim():
                self.emit(kind='ack', outcome=outcome, completed=outcome not in (FAILED, CANCELLED),
                          started_monotonic=started, finished_monotonic=finished, **fields, **context)
        try:
            outcome = self.handle_set_usage_event(event)
        except Exception as error:
            acknowledge(FAILED, message=str(error))
        except BaseException:
            acknowledge(CANCELLED, message='usage event cancelled')
            raise
        else:
            acknowledge(outcome, lag_real=lag, lag_logical=lag * timing.get('speed_factor', 1))
        self.log(f'Usage reconciliation lag_real={lag}: {context}')

    # --------------------------------------------------
    # Event handling
    # --------------------------------------------------

    def handle_set_usage_event(self, event):
        """
        Store the latest set-usage event for a workload and reconcile that workload.
        Return the reconciliation outcome.
        """

        namespace = event["namespace"]
        name = event["name"]
        workload_key = (namespace, name)

        usage_action = {}
        for pod_name, usage in event.get("pods").items():
            usage_action[(namespace, pod_name)] = {
                "cpu": usage.get("cpu"),
                "memory": int(usage.get("memory"))
            }

        # Recorded when the event starts: later events of this workload are
        # still queued behind it and cannot replace the usage it applies.
        with self.state_lock:
            self.last_usage_actions[workload_key] = usage_action

        try:
            return self.reconcile_workload(workload_key=workload_key)
        except Exception:
            # Periodic reconciliation retries a workload whose event failed.
            with self.state_lock:
                self.retry_workloads.add(workload_key)
            raise

    # --------------------------------------------------
    # Reconciliation
    # --------------------------------------------------

    def reconcile_tracked_workloads(self):
        """
        Reapply tracked workloads whose observed topology changed.

        Only workloads marked by the cache, or whose previous reconciliation
        failed, are examined; no request is issued for unchanged workloads.
        With a dispatcher, each one is queued behind the events of its workload;
        a workload with a reconciliation still queued is not queued again,
        because that one will read the cache when it starts.
        """

        if self.resource_cache is None:
            return
        self.emit_cache()
        with self.state_lock:
            changed = self.resource_cache.take_dirty() | self.retry_workloads
            self.retry_workloads = set()
            keys = [key for key in self.last_usage_actions if key in changed]
        dispatcher = self.dispatcher
        for workload_key in keys:
            if dispatcher is None:
                self.reconcile_tracked_workload(workload_key)
                continue
            with self.state_lock:
                if workload_key in self.queued_reconciliations:
                    continue
                self.queued_reconciliations.add(workload_key)
            dispatcher.submit(workload_key, lambda claim, key=workload_key: self.reconcile_tracked_workload(key))

    def reconcile_tracked_workload(self, workload_key):
        """Periodic reconciliation of one workload; failures are retried later."""
        with self.state_lock:
            self.queued_reconciliations.discard(workload_key)
            if workload_key not in self.last_usage_actions:
                return
        # A failure for one workload must not starve later workloads.
        try:
            view = self.observe_workload(workload_key)
            if view is None:
                self.cleanup_workload(workload_key)
                return
            with self.state_lock:
                if self.last_signatures.get(workload_key) == view.signature:
                    return
            self.reconcile_workload(workload_key, view.keys, view.pods, view.signature)
        except Exception as error:
            with self.state_lock:
                self.retry_workloads.add(workload_key)
            self.emit(kind='error', message=str(error), action='periodic-reconciliation',
                      namespace=workload_key[0], resource=workload_key[1])

    def observe_workload(self, workload_key):
        """Cached view of a workload, or None when its deployment is gone.

        Absence is decided by the cache only after one authoritative read
        confirmed it; later observations of that deployment invalidate the
        confirmation. A deployment that exists but has not reached the cache
        yet is awaited for a bounded time instead of being reported absent.
        """

        cache = self.resource_cache
        view = cache.snapshot(workload_key, self.CACHE_HEALTH_TIMEOUT)
        if view.keys or view.deployment_uid is not None:
            return view
        if view.absence_confirmed:
            return None
        namespace, name = workload_key
        if not self.deployment_exists(namespace, name):
            cache.confirm_absent(view)
            return None
        if not cache.wait_for(lambda read: read(workload_key).deployment_uid is not None,
                              self.CACHE_CATCH_UP_TIMEOUT):
            raise RuntimeError(f"Resource cache did not observe deployment {namespace}/{name}")
        return cache.snapshot(workload_key, self.CACHE_HEALTH_TIMEOUT)

    def reconcile_workload(self, workload_key, emulated_pods=None, emulated_pods_objects=None,
                           signature=None):
        """
        Reconcile one workload by asking UsageAssignmentEngine for the final
        pod-level assignment and applying it to the cluster.

        Return APPLIED only after every mutation succeeded, EMPTY when the
        workload exists but has no emulated pod to receive usage, and
        TARGET_ABSENT when the deployment is gone. Removing the usage of a
        missing workload is cleanup, never an application. Errors raise.
        """

        namespace, name = workload_key

        with self.state_lock:
            last_usage_action = self.last_usage_actions.get(workload_key)

        if emulated_pods is None:
            view = self.observe_workload(workload_key)
            if view is None:
                self.cleanup_workload(workload_key)
                return TARGET_ABSENT
            emulated_pods, emulated_pods_objects, signature = view.keys, view.pods, view.signature

        emulated_pods_limit = self.get_pod_limits_map(emulated_pods_objects)

        self.log(
            f"[INFO] Reconciling {namespace}/{name}. "
            f"Real pods: {len(last_usage_action)}. "
            f"Emulated pods: {len(emulated_pods)}."
        )

        assignment = self.assignment_engine.resolve(
            workload_key=workload_key,
            real_pods_usage=last_usage_action,
            emulated_pods_limit=emulated_pods_limit,
            emulated_pods=emulated_pods,
        )

        self.apply_assignment(workload_key, assignment)
        # Commit the observed topology only after every mutation succeeded.
        with self.state_lock:
            self.last_emulated_pods[workload_key] = emulated_pods
            if signature is not None:
                self.last_signatures[workload_key] = signature
        return APPLIED if assignment else EMPTY

    # --------------------------------------------------
    # Apply assignment
    # --------------------------------------------------

    def apply_assignment(self, workload_key, assignment):
        """
        Apply usage assignment to KWOK.
        """

        namespace, workload_name = workload_key

        desired_crus = set()
        pending = []

        for pod_key, usage in assignment.items():
            pod_namespace, pod_name = pod_key

            # Static quantities avoid compiling a new CEL expression per sample.
            cpu = f'{int(round(usage.get("cpu") * 1_000_000))}u'
            memory = str(int(round(usage.get("memory"))))
            cru_name = f"usage-{pod_namespace}-{pod_name}"
            
            desired_crus.add(cru_name)

            body = self.cru_generator.generate_cluster_resource_usage(
                cru_name=cru_name,
                namespace=pod_namespace,
                pod_name=pod_name,
                container=workload_name,
                cpu=cpu,
                memory=memory,
            )

            self.log(
                f"[INFO] Applying usage for {pod_namespace}/{pod_name}: "
                f"cpu={cpu}, memory={memory}"
            )
            pending.append((cru_name, body))

        self.run_mutations(workload_key, [(self.apply_cru, name, body) for name, body in pending])

        self.delete_stale_crus(workload_key, desired_crus)
        with self.state_lock:
            self.managed_crus_by_workload[workload_key] = desired_crus

    def run_mutations(self, workload_key, calls):
        """Run CRU mutations on the shared bounded writer and wait for all of them.

        The first failure is raised only after every call finished, so no write
        of this workload is still in flight when its outcome is reported.
        """
        if self.usage_executor is None:
            for function, *args in calls:
                function(*args)
            return
        futures = [self.usage_executor.submit(function, *args, group=workload_key)
                   for function, *args in calls]
        failure = None
        for future in futures:
            try:
                future.result()
            except Exception as error:
                failure = failure or error
        if failure is not None:
            raise failure

    # --------------------------------------------------
    # K8s API calls
    # --------------------------------------------------

    def apply_cru(self, cru_name, body):
        """
        Create or patch a ClusterResourceUsage object.
        """

        try:
            with self.state_lock:
                exists = cru_name in self.existing_crus
            if exists:
                self.api_call('patch-usage', self.k8s_api.patch_cluster_custom_object,
                    group=self.CRU_GROUP,
                    version=self.CRU_VERSION,
                    plural=self.CRU_PLURAL,
                    name=cru_name,
                    body=body,
                )
            else:
                self.api_call('create-usage', self.k8s_api.create_infrastructure_object,
                    body=body,
                    group=self.CRU_GROUP,
                    version=self.CRU_VERSION,
                    plural=self.CRU_PLURAL,
                )
                with self.state_lock:
                    self.existing_crus.add(cru_name)

        except Exception as e:
            raise RuntimeError(f"Failed to apply usage for CRU {cru_name}: {e}") from e

    def delete_cru(self, cru_name):
        """
        Delete a ClusterResourceUsage object.
        """
        
        try:
            self.api_call('delete-usage', self.k8s_api.delete_cluster_custom_object,
                group=self.CRU_GROUP,
                version=self.CRU_VERSION,
                plural=self.CRU_PLURAL,
                name=cru_name,
            )
            self.log(f"[INFO] Deleted CRU {cru_name}")
            with self.state_lock:
                self.existing_crus.discard(cru_name)

        except ApiException as e:
            if e.status == 404:  # if 404, then CRU already no longer exists
                with self.state_lock:
                    self.existing_crus.discard(cru_name)
            else:
                raise


    def deployment_exists(self, namespace, name):
        """Authoritative read used only to confirm a deployment absent from the cache."""
        try:
            self.api_call('read-deployment', self.k8s_api.read_namespaced_deployment,
                namespace=namespace,
                name=name,
            )
            return True

        except ApiException as e:
            if e.status == 404:
                return False
            
            raise

    # --------------------------------------------------
    # Get emulated pod's useful information
    # --------------------------------------------------

    def is_live_pod(self, pod):
        return is_live_pod(pod)

    def list_alive_pod_keys(self, pods):
        return {(pod.metadata.namespace, pod.metadata.name) for pod in pods.items if is_live_pod(pod)}

    def get_pod_limits_map(self, pods):
        """
        Return current resource limits for the pods of a workload.
        """

        return {(pod.metadata.namespace, pod.metadata.name): pod_limits(pod)
                for pod in pods.items if is_live_pod(pod)}

    # --------------------------------------------------
    # Clean-up
    # --------------------------------------------------

    def delete_stale_crus(self, workload_key, desired_crus):
        """
        Delete CRUs previously managed for this workload that are no longer
        needed after a pod-set change.
        """

        with self.state_lock:
            previous_crus = self.managed_crus_by_workload.get(workload_key, set())
        stale_crus = previous_crus - desired_crus

        self.run_mutations(workload_key, [(self.delete_cru, name) for name in sorted(stale_crus)])


    def cleanup_workload(self, workload_key):
        """
        Remove all related state and CRUs for a workload.
        """
        with self.state_lock:
            managed = sorted(self.managed_crus_by_workload.get(workload_key, set()))
        self.run_mutations(workload_key, [(self.delete_cru, name) for name in managed])

        with self.state_lock:
            self.last_usage_actions.pop(workload_key, None)
            self.last_emulated_pods.pop(workload_key, None)
            self.last_signatures.pop(workload_key, None)
            self.retry_workloads.discard(workload_key)
            self.managed_crus_by_workload.pop(workload_key, None)
        self.assignment_engine.cleanup_workload(workload_key)
