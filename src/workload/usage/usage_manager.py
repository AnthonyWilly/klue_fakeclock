import multiprocessing
import queue
import time
import math
from collections import defaultdict
from types import SimpleNamespace
from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
import threading

from kubernetes.client.rest import ApiException
from kubernetes.utils.quantity import parse_quantity
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_objects.cluster_resource_usage_generator import ClusterResourceUsageGenerator
from workload.usage.usage_assignment import UsageAssignmentEngine
from workload.usage.usage_tracking import APPLIED, CANCELLED, EMPTY, FAILED, TARGET_ABSENT


class UsageManager(multiprocessing.Process):
    """
    Background process responsible for managing KWOK ClusterResourceUsage objects.

    - Receives set-usage events with real pod-level usage snapshots.
    - Tracks workloads by (namespace, name).
    - Periodically checks the exact set of emulated pods for each tracked workload.
    - Reconciles when:
        1. a new set-usage event arrives; or
        2. the emulated pod set changes.
    - Delegates usage distribution to UsageAssignmentEngine.
    - Applies the returned assignment as one ClusterResourceUsage per emulated pod.
    """

    TIME_OUT = 200
    API_CONCURRENCY = 4
    RECONCILE_INTERVAL_SECONDS = 5

    CRU_GROUP = "kwok.x-k8s.io"
    CRU_VERSION = "v1alpha1"
    CRU_PLURAL = "clusterresourceusages"

    def __init__(self, queue, reconcile_interval_seconds=RECONCILE_INTERVAL_SECONDS, status_queue=None):
        super().__init__()
        if not math.isfinite(reconcile_interval_seconds) or reconcile_interval_seconds <= 0:
            raise ValueError("reconciliation interval must be positive and finite")
        self.queue = queue
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

        # set(cru_name)
        self.existing_crus = set()
        self.api_stats = {}
        self.api_lock = None
        self.usage_executor = None

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
            # Only independent pods of the SAME event run concurrently. Wait
            # for every result before acknowledging or consuming the next event.
            with ThreadPoolExecutor(max_workers=self.API_CONCURRENCY) as executor:
                self.usage_executor = executor
                self.process_events()
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

    def process_events(self, monotonic=None):
        """A busy queue cannot starve reconciliation; events retain FIFO order."""
        monotonic = monotonic or time.monotonic
        deadline = monotonic() + self.reconcile_interval_seconds
        while True:
            now = monotonic()
            if now >= deadline:
                try:
                    self.reconcile_tracked_workloads()
                except Exception as error:
                    self.emit(kind='error', message=str(error), action='periodic-reconciliation')
                # Skip expired ticks, never shift the periodic grid to event arrival.
                now = monotonic()
                deadline += (int((now - deadline) // self.reconcile_interval_seconds) + 1) * self.reconcile_interval_seconds
            try:
                event = self.queue.get(timeout=max(0, deadline - monotonic()))
            except queue.Empty:
                continue
            if event == "STOP":
                self.log("Stopping UsageManager")
                self.emit(kind='stopped')
                break
            self.process_event(event, monotonic)

    def process_event(self, event, monotonic):
        """Emit one start and exactly one terminal outcome for an accepted event.

        ``lag_real`` keeps its meaning: delay until the worker started the
        event. Completion lag is derived from ``finished_monotonic``.
        """
        timing = event.get('replay_timing') or {}
        started = monotonic()
        lag = max(0, started - timing.get('scheduled_monotonic', started))
        context = dict(event_id=event.get('event_id'), namespace=event.get('namespace'),
                       resource=event.get('name'), trace_timestamp=timing.get('trace_timestamp'))
        self.emit(kind='started', event_id=context['event_id'], started_monotonic=started)
        def acknowledge(outcome, **fields):
            self.emit(kind='ack', outcome=outcome, completed=outcome not in (FAILED, CANCELLED),
                      started_monotonic=started, finished_monotonic=monotonic(), **fields, **context)
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

        self.last_usage_actions[workload_key] = usage_action

        return self.reconcile_workload(
            workload_key=workload_key
        )

    # --------------------------------------------------
    # Reconciliation
    # --------------------------------------------------

    def reconcile_tracked_workloads(self):
        """
        Periodically check whether the exact emulated pod set changed.
        """

        if not self.last_usage_actions:
            return
        # One coherent snapshot replaces one API request per tracked workload.
        snapshot = self.api_call('list-pods', self.k8s_api.v1.list_pod_for_all_namespaces,
                                 label_selector='deployment')
        grouped = defaultdict(list)
        for pod in snapshot.items:
            labels = pod.metadata.labels or {}
            grouped[(pod.metadata.namespace, labels.get('deployment'))].append(pod)
        empty = [key for key in self.last_usage_actions
                 if not self.list_alive_pod_keys(SimpleNamespace(items=grouped[key]))]
        deployments = None
        if empty:
            listed = self.api_call('list-deployments',
                self.k8s_api.apps_v1.list_deployment_for_all_namespaces)
            deployments = {(obj.metadata.namespace, obj.metadata.name) for obj in listed.items}
        for workload_key in list(self.last_usage_actions):
            pods = SimpleNamespace(items=grouped[workload_key])
            keys = self.list_alive_pod_keys(pods)
            # A failure for one workload must not starve later workloads.
            try:
                if not keys and workload_key not in deployments:
                    self.cleanup_workload(workload_key)
                    continue
                if self.last_emulated_pods.get(workload_key) == keys:
                    continue
                self.reconcile_workload(workload_key, keys, pods)
            except Exception as error:
                self.emit(kind='error', message=str(error), action='periodic-reconciliation',
                          namespace=workload_key[0], resource=workload_key[1])

    def reconcile_workload(self, workload_key, emulated_pods=None, emulated_pods_objects=None):
        """
        Reconcile one workload by asking UsageAssignmentEngine for the final
        pod-level assignment and applying it to the cluster.

        Return APPLIED only after every mutation succeeded, EMPTY when the
        workload exists but has no emulated pod to receive usage, and
        TARGET_ABSENT when the deployment is gone. Removing the usage of a
        missing workload is cleanup, never an application. Errors raise.
        """

        namespace, name = workload_key

        last_usage_action = self.last_usage_actions.get(workload_key)

        if emulated_pods is None:
            emulated_pods_objects = self.list_emulated_pod_objects(namespace, name)
            
            if emulated_pods_objects is None:
                raise RuntimeError(f"Could not list emulated pods for {namespace}/{name}")

            emulated_pods = self.list_alive_pod_keys(emulated_pods_objects)

            if not emulated_pods: ## admissible check to avoid unecessary API calls.
                deployment_status = self.deployment_exists(namespace, name)

                if deployment_status is None:
                    raise RuntimeError(f"Could not check deployment existence for {namespace}/{name}")

                if deployment_status is False:
                    self.cleanup_workload(workload_key)
                    return TARGET_ABSENT

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
        self.last_emulated_pods[workload_key] = emulated_pods
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

        if self.usage_executor is None:
            for name, body in pending:
                self.apply_cru(name, body)
        else:
            futures = [self.usage_executor.submit(self.apply_cru, name, body) for name, body in pending]
            failure = None
            for future in futures:
                try:
                    future.result()
                except Exception as error:
                    failure = failure or error
            if failure is not None:
                raise failure

        self.delete_stale_crus(workload_key, desired_crus)
        self.managed_crus_by_workload[workload_key] = desired_crus

    # --------------------------------------------------
    # K8s API calls
    # --------------------------------------------------

    def apply_cru(self, cru_name, body):
        """
        Create or patch a ClusterResourceUsage object.
        """

        try:
            if cru_name in self.existing_crus:
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
            self.existing_crus.discard(cru_name)

        except ApiException as e:
            if e.status == 404:  # if 404, then CRU already no longer exists
                self.existing_crus.discard(cru_name)
            else:
                raise


    def list_emulated_pod_objects(self, namespace, workload_name):
        """
        Return the exact set of live emulated pods for a workload.
        """

        return self.api_call('list-workload-pods', self.k8s_api.v1.list_namespaced_pod,
            namespace=namespace,
            label_selector=f"deployment={workload_name}",
        )

    def deployment_exists(self, namespace, name):
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
        if pod.metadata.deletion_timestamp is not None:
            return False

        if pod.status.phase in {"Succeeded", "Failed"}:
            return False

        return True
    
    def list_alive_pod_keys(self, pods):
        result = set()
        for pod in pods.items:
            if self.is_live_pod(pod):
                pod_name = pod.metadata.name
                pod_namespace = pod.metadata.namespace

                result.add((pod_namespace, pod_name))
        
        return result

    def get_pod_limits_map(self, pods):
        """
        Return current resource limits for the pods of a workload.
        """

        result = {}

        for pod in pods.items:
            if not self.is_live_pod(pod):
                continue

            namespace = pod.metadata.namespace
            pod_name = pod.metadata.name

            cpu_total = None
            memory_total = None

            for container in pod.spec.containers:
                resources = container.resources
                limits = resources.limits if resources and resources.limits else {}

                cpu_limit_base = limits.get("cpu")
                memory_limit_base = limits.get("memory")

                if cpu_limit_base is not None:
                    cpu_limit = parse_quantity(str(cpu_limit_base))
                    cpu_total = (cpu_total or 0) + cpu_limit

                if memory_limit_base is not None:
                    memory_limit = parse_quantity(str(memory_limit_base))
                    memory_total = (memory_total or 0) + memory_limit

            pod_limits = {}

            if cpu_total is not None:
                pod_limits["cpu"] = float(cpu_total)

            if memory_total is not None:
                pod_limits["memory"] = int(memory_total)

            result[(namespace, pod_name)] = pod_limits

        return result

    # --------------------------------------------------
    # Clean-up
    # --------------------------------------------------

    def delete_stale_crus(self, workload_key, desired_crus):
        """
        Delete CRUs previously managed for this workload that are no longer
        needed after a pod-set change.
        """

        previous_crus = self.managed_crus_by_workload.get(workload_key, set())
        stale_crus = previous_crus - desired_crus

        for cru_name in stale_crus:
            self.delete_cru(cru_name)


    def cleanup_workload(self, workload_key):
        """
        Remove all related state and CRUs for a workload.
        """
        for cru_name in self.managed_crus_by_workload.get(workload_key, set()):
            self.delete_cru(cru_name)

        self.last_usage_actions.pop(workload_key, None)
        self.last_emulated_pods.pop(workload_key, None)
        self.managed_crus_by_workload.pop(workload_key, None)
        self.assignment_engine.cleanup_workload(workload_key)
