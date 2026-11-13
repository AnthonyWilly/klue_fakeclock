"""
This module defines the InfrastructureManager class, which manages the setup, emulation, and teardown of Kubernetes infrastructure
for trace-driven experiments. It handles applying Kubernetes objects, managing node pools, and coordinating emulation phases.
"""

import json
import time
from experiment_clock import ExperimentClock, ReplayDiagnostics, replay_entries, validate_speed_factor
from util.k8s_api.k8s_api import K8SAPI
from util.k8s_object_applier import KubernetesObjectApplier

class InfrastructureManager:
    TIME_OUT = 200
    AMOUNT_OF_REAL_NODES = 1

    def __init__(self, data_path, karpenter, emulation_phase, speed_factor=1):
        """
        Initializes the Manager class.
        """
        self.speed_factor = validate_speed_factor(speed_factor, karpenter=karpenter)
        self.karpenter = karpenter
        self.emulation_phase = emulation_phase

        self.k8s_api = K8SAPI(timeout=self.TIME_OUT)
        self.k8s_object_applier = KubernetesObjectApplier(self.k8s_api)

        with open(data_path, 'r', encoding="utf-8") as file:
            self.data = json.load(file)

        self.replay_diagnostics = ReplayDiagnostics(len(self.data['emulation']))
        self.input_data_node_count = self.count_nodes_in_input_data()
        self.node_count = 0

    def log(self, message):
        """
        Logs a message with a "[INFRASTRUCTURE MANAGER]" prefix.
        """
        print(f"[INFRASTRUCTURE MANAGER] {message}")

    def before_setup(self):
        if self.karpenter == True:
            self.log("[INFO] Expanding nodepools disruption time to 6000 minutes.")
            self.expand_nodepools_disruption_time()

    def setup(self):
        """
        Executes the infrastructure setup process.

        This method applies initial infrastructure configurations from
        `self.data['setup']` to the Kubernetes cluster. These configurations
        are typically expected to be items like custom Node objects or
        NodeClaim-like resources. After applying these objects, the method
        waits until a target number of these managed nodes are observed in
        the cluster.
        """
        setup = self.data['setup']
        for infra_obj in setup:
            node_name = infra_obj["metadata"]["name"]
            self.k8s_object_applier.apply_object(infra_obj)
            self.log(f"[INFO] Node {node_name} applied.")

        while self.input_data_node_count != self.node_count:
            nodes = self.k8s_api.list_node()
            print(f"[INFO] Waiting for nodes to be created. Expected: {self.input_data_node_count}, Current: {len(nodes.items) - self.AMOUNT_OF_REAL_NODES}")
            self.node_count = len(nodes.items) - self.AMOUNT_OF_REAL_NODES
            self.log(f"[INFO] Node count: {self.node_count}")
            time.sleep(2)

    def before_emulation(self):
        """
        Performs pre-emulation setup tasks.

        If Karpenter mode is enabled (self.karpenter is True), this method
        restores the disruption time of nodepools to their original values
        by calling `self.restore_nodepools_disruption_time()`.
        """
        if self.karpenter == True:
            self.log("[INFO] Restoring nodepools disruption time to original values.")
            self.restore_nodepools_disruption_time()

    def emulation(self, clock=None):
        """Replay infrastructure against the coordinator's common origin."""
        clock = clock or ExperimentClock(self.speed_factor).start()
        for entry, scheduled in replay_entries(
                self.data['emulation'], clock, self.replay_diagnostics,
                self.emulation_phase == "dynamic"):
            for node in entry.get('applied_objects', []):
                self.replay_diagnostics.apply(
                    clock, scheduled, {"action": "apply-node", "object": node},
                    self.k8s_object_applier.apply_node, node, node['metadata']['name'])
            for name in entry.get('deleted_objects', []):
                self.replay_diagnostics.apply(
                    clock, scheduled, {"action": "delete-node", "name": name},
                    self.k8s_api.delete_infrastructure_object, name)

    def tear_down(self):
        pass

    def expand_nodepools_disruption_time(self):
        """
        Expands the disruption time for all node pools in the Kubernetes cluster.

        This method retrieves the current disruption times for all node pools and stores
        them in the `self.original_disruption_times` dictionary. It then updates the 
        `consolidateAfter` field of the disruption specification for each node pool to 
        a fixed value of "6000m" (6000 minutes).
        """
        self.original_disruption_times = {
            np["metadata"]["name"]: np["spec"]["disruption"]["consolidateAfter"]
            for np in self.k8s_api.list_cluster_custom_object("karpenter.sh", "v1", "nodepools")["items"]
        }

        for np in self.original_disruption_times:
            patch = {"spec": {"disruption": {"consolidateAfter": "6000m"}}}
            self.k8s_api.patch_cluster_custom_object("karpenter.sh", "v1", "nodepools", np, patch)

    def restore_nodepools_disruption_time(self):
        """
        Restores the disruption time for all node pools to their original values.

        This method iterates through the `original_disruption_times` dictionary, which contains
        the original disruption times for each node pool. For each node pool, it sends a patch
        request to the Kubernetes API to update the `consolidateAfter` field in the disruption
        specification of the node pool.
        """
        for nodepool, time_value in self.original_disruption_times.items():
            patch = {"spec": {"disruption": {"consolidateAfter": time_value}}}
            self.k8s_api.patch_cluster_custom_object("karpenter.sh", "v1", "nodepools", nodepool, patch)

    def count_nodes_in_input_data(self):
        """
        Counts the number of node claims in the setup data.

        This method retrieves the 'setup' dictionary from the instance's data,
        extracts the 'nodes' list from it, and returns the count of items
        in the list. If 'setup' or 'nodes' is not present, it defaults to
        an empty dictionary or list, respectively.
        """
        setup_nodes = self.data.get('setup', [])
        return len(setup_nodes)