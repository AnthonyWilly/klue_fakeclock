"""
This module defines the WorkloadManager class, which serves as a base class for managing workloads in a trace emulation environment.
"""

import json
import time
from experiment_clock import ExperimentClock, ReplayDiagnostics, replay_entries, validate_speed_factor
from kubernetes import client
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_object_applier import KubernetesObjectApplier
import multiprocessing
from workload.usage.usage_manager import UsageManager

class WorkloadManager:
    TIME_OUT = 200
    def __init__(self, data_path, emulation_phase, hpa, speed_factor=1):
        """
        Initializes the Workload Manager class.
        """
        self.speed_factor = validate_speed_factor(speed_factor, hpa=hpa)
        self.BEFORE_EMULATION_KINDS = {
            "horizontalpodautoscaler",
        }

        self.emulation_phase = emulation_phase
        self.hpa = hpa

        self.k8s_api = K8SAPI(timeout=self.TIME_OUT)
        self.k8s_object_applier = KubernetesObjectApplier(self.k8s_api, hpa)

        with open(data_path, 'r', encoding="utf-8") as file:
            self.data = json.load(file)
        
        self.replay_diagnostics = ReplayDiagnostics(len(self.data['emulation']))
        self.usage_queue = multiprocessing.Queue()
        self.usage_manager = UsageManager(self.usage_queue, reconcile_interval_seconds=5 / self.speed_factor)

    def log(self, message):
        """
        Logs a message with a "[WORKLOAD MANAGER]" prefix.
        """
        print(f"[WORKLOAD MANAGER] {message}")

    def before_setup(self):
        pass

    def setup(self):
        """
        Executes the setup process for the broker.

        The setup data is expected to be structured as follows:
        - `setup['nodeclaims']`: A list of node claim objects to be applied.
        - `setup['deployments']`: A dictionary where keys are namespace names and 
          values are lists of pod objects to be applied within those namespaces.

        Logs the node count during the waiting process.
        """
        # start UsageManager process
        self.log("[INFO] Starting UsageManager")
        self.usage_manager.start()

        setup = self.data["setup"]

        applied_objects = setup["applied_objects"]

        workload_actions = setup["workload_actions"]

        target_namespaces = set(applied_objects.keys())
        pods_before_setup = self.count_pods_in_namespaces(target_namespaces)

        self.log(f"[INFO] Deploying setup objects")
        for namespace, objects in applied_objects.items():
            self.create_namespace_if_not_exists(namespace)
            for object in objects:
                if object.get("kind", "").lower() not in self.BEFORE_EMULATION_KINDS:
                    self.k8s_object_applier.apply_object(object)

        self.wait_pods_ready(pods_before_setup)

        self.log(f"[INFO] Applying workload actions on setup")
        for workload_action in workload_actions:
            action = workload_action["action"]
            if action == "set-usage":
                self.set_workload_usage(workload_action)
            elif action == "scale" and not self.hpa:
                self.scale_workload(workload_action)

    def before_emulation(self):
        if not self.hpa:
            return
        setup = self.data["setup"]

        applied_objects = setup["applied_objects"]

        self.log(f"[INFO] Deploying setup HPA objects")
        for namespace, objects in applied_objects.items():
            self.create_namespace_if_not_exists(namespace)
            for object in objects:
                if object.get("kind", "").lower() in self.BEFORE_EMULATION_KINDS:
                    self.k8s_object_applier.apply_object(object)

    def emulation(self, clock=None):
        """Replay workload entries without changing trace timestamps or units."""
        clock = clock or ExperimentClock(self.speed_factor).start()
        diagnostics = self.replay_diagnostics
        for entry, scheduled in replay_entries(
                self.data['emulation'], clock, diagnostics,
                self.emulation_phase == "dynamic"):
            for namespace, objects in entry.get('applied_objects', {}).items():
                for obj in objects:
                    if self.ignore_hpa(obj):
                        diagnostics.skip()
                        continue
                    diagnostics.apply(
                        clock, scheduled, {"action": "apply-object", "object": obj},
                        self.apply_workload_object, namespace, obj)
            for action in entry.get('workload_actions', []):
                kind = action['action']
                if kind == 'set-usage':
                    # Queue metadata is serializable; the child process starts
                    # before the common clock origin exists in the parent.
                    event = dict(action, replay_timing={
                        "scheduled_monotonic": scheduled,
                        "trace_timestamp": entry['timestamp'],
                        "origin_monotonic": clock.origin_monotonic,
                        "origin_unix": clock.origin_unix,
                        "speed_factor": clock.speed_factor,
                    })
                    diagnostics.apply(clock, scheduled, {"action": "queue-usage"},
                                      self.set_workload_usage, event)
                elif kind == 'scale' and not self.hpa:
                    diagnostics.apply(clock, scheduled, action,
                                      self.scale_workload, action)
                elif kind == 'scale':
                    diagnostics.skip()
                else:
                    diagnostics.apply(clock, scheduled, action,
                                      self.unsupported_action, kind)
            for obj in entry.get('deleted_objects', []):
                if self.ignore_hpa(obj):
                    diagnostics.skip()
                    continue
                diagnostics.apply(clock, scheduled,
                                  {"action": "delete-object", "object": obj},
                                  self.k8s_object_applier.delete_object, obj)

    def ignore_hpa(self, obj):
        return not self.hpa and obj.get('kind', '').lower() == 'horizontalpodautoscaler'

    def apply_workload_object(self, namespace, obj):
        self.create_namespace_if_not_exists(namespace)
        self.k8s_object_applier.apply_object(obj)

    @staticmethod
    def unsupported_action(kind):
        raise ValueError(f"Unsupported workload action: {kind}")

    def tear_down(self):
        self.usage_queue.put("STOP")
        self.usage_manager.join(timeout=30)

        if self.usage_manager.is_alive():
            self.log("[WARNING] UsageManager did not stop after 30s. Terminating.")
            self.usage_manager.terminate()
            self.usage_manager.join(timeout=30)

        if self.usage_manager.is_alive():
            self.log("[WARNING] UsageManager did not terminate. Killing.")
            self.usage_manager.kill()
            self.usage_manager.join(timeout=30)

    def wait_pods_ready(self, pods_before_setup: int = 0):
        """
        Waits until the number of currently running pods matches the expected number of pods.

        This method calculates the expected number of pods based on the deployment specifications
        defined in the `self.data` dictionary. It then continuously checks the current number of
        running pods (excluding specific namespaces) and logs the progress until the expected
        number of pods is reached.

        The method uses a 2-second interval between checks to avoid excessive polling.
        """
        expected_pods = 0
            
        for namespace_objects in self.data['setup']['applied_objects'].values():
            for obj in namespace_objects:
                kind = obj.get("kind", "").lower()
                replica_count = obj.get("spec", {}).get("replicas", 0)

                if kind in {"deployment", "statefulset"}:
                    expected_pods += replica_count
                elif replica_count:
                    self.log(f"[WARNING] Object is not a Deployment or StatefulSet but has replicas={replica_count}. Counting it as workload.")
                    expected_pods += replica_count

        target_namespaces = set(self.data['setup']['applied_objects'].keys())
        
        # Almost 7 pods are created per second, the timeout is derived from expected pod count.
        deadline = time.monotonic() + int(expected_pods / 7)
        while True:
            current_pods = self.count_pods_in_namespaces(target_namespaces) - pods_before_setup
            if current_pods >= expected_pods:
                self.log(f"[INFO] All pods created. Current: {current_pods}, Expected: {expected_pods}")
                break
            if time.monotonic() >= deadline:
                self.log(f"[WARNING] Timeout waiting for pods. Current: {current_pods}, Expected: {expected_pods}. Proceeding anyway.")
                break
            self.log(f"[INFO] Current pods: {current_pods}, Expected: {expected_pods}")
            time.sleep(2)

    def namespace_exists(self, namespace):
        """
        Checks if a Kubernetes namespace exists.
        """
        try:
            self.k8s_api.read_namespace(name=namespace)
            return True
        except client.exceptions.ApiException as e:
            if e.status == 404:
                return False
            raise

    def create_namespace_if_not_exists(self, namespace):
        """
        Ensures that a Kubernetes namespace exists. If the namespace does not exist, it creates it.
        """
        if not self.namespace_exists(namespace):
            body = client.V1Namespace(metadata=client.V1ObjectMeta(name=namespace))
            self.k8s_api.create_namespace(body=body)
            self.log(f"[INFO] Namespace {namespace} created.")
        else:
            self.log(f"[INFO] Namespace {namespace} already exists.")

    def count_pods_in_namespaces(self, namespaces: set):
        """
        Counts only pods that belong to the given set of namespaces.
        This avoids mismatches caused by hardcoded exclusion lists that may
        not align with the namespaces actually used in the setup.
        """
        pods = self.k8s_api.list_pod_for_all_namespaces()
        return sum(1 for pod in pods.items if pod.metadata.namespace in namespaces)
    
    def set_workload_usage(self, usage_info):
        self.usage_queue.put(usage_info)

    def scale_workload(self, scale_info):
        name = scale_info['name']
        namespace = scale_info['namespace']
        replicas = scale_info['pods']
        kind = scale_info['kind'].lower()
        if kind in {"deployment", "statefulset"}:
            self.k8s_api.patch_namespaced_deployment_scale(
                name, namespace, {"spec": {"replicas": replicas}})
        else:
            raise ValueError(f"Unsupported scale kind: {kind}")
