"""
This class is responsible for running a trace emulation process. It applies a Kubernetes 
node pool configuration, initializes a tracer to process trace data, and runs a broker 
to handle the emulation steps. The script interacts with Kubernetes and processes trace 
files to simulate cluster behavior.
"""
import argparse
from experiment_clock import validate_speed_factor
import os
import subprocess
from tracer.tracer_karpenter import TracerKarpenter
from tracer.tracer_kwok import TracerKWOKOnly
from manager import Manager

class Main:
    def __init__(self, trace_path, nodepool_path, karpenter, tracer_skip, infrastructure, workload, hpa, speed_factor=1):
        self.speed_factor = validate_speed_factor(speed_factor, karpenter=karpenter, hpa=hpa)
        self.trace_path = trace_path
        self.nodepool_path = nodepool_path
        self.karpenter = karpenter
        self.tracer_skip = tracer_skip
        self.infrastructure = infrastructure
        self.workload = workload
        self.hpa = hpa
    
    def apply_nodepool(self):
        """
        Applies the Kubernetes node pool configuration using kubectl apply.
        
        This method runs the command `kubectl apply -f <nodepool_path>` to apply the 
        specified node pool configuration to the Kubernetes cluster.
        """
        if not self.karpenter:
            return
        subprocess.run(["kubectl", "apply", "-f", self.nodepool_path], check=True,)

    def run_tracer(self):
        tracer = None
        if self.karpenter:
            tracer = TracerKarpenter(
                os.path.join(self.trace_path, "kube_pod_container_resource_requests.csv"),
                os.path.join(self.trace_path, "kube_pod_container_resource_limits.csv"),
                os.path.join(self.trace_path, "container_cpu_usage_seconds_total.csv"),
                os.path.join(self.trace_path, "container_memory_usage_bytes.csv"),
                os.path.join(self.trace_path, "karpenter_pods_state.csv"),
                os.path.join(self.trace_path, "kube_pod_owner.csv"),
                os.path.join(self.trace_path, "kube_replicaset_owner.csv"),
                os.path.join(self.trace_path, "instance_types.json"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_max_replicas.csv"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_min_replicas.csv"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_target_metric.csv"),
            )
        else:
            tracer = TracerKWOKOnly(
                os.path.join(self.trace_path, "kube_pod_container_resource_requests.csv"),
                os.path.join(self.trace_path, "kube_pod_container_resource_limits.csv"),
                os.path.join(self.trace_path, "container_cpu_usage_seconds_total.csv"),
                os.path.join(self.trace_path, "container_memory_usage_bytes.csv"),
                os.path.join(self.trace_path, "kube_pod_owner.csv"),
                os.path.join(self.trace_path, "kube_pod_status_phase.csv"),
                os.path.join(self.trace_path, "kube_replicaset_owner.csv"),
                os.path.join(self.trace_path, "instance_types.json"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_max_replicas.csv"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_min_replicas.csv"),
                os.path.join(self.trace_path, "kube_horizontalpodautoscaler_spec_target_metric.csv"),
            )

        # Executa o tracer.py
        if not self.tracer_skip:
            tracer.run()

    def run_manager(self):
        """
        Initializes and runs the Manager to handle the emulation process.
        This method creates an instance of the Manager class with the provided
        parameters and calls its run method to start the emulation.
        """
        # Executa o broker.py
        manager = Manager(karpenter=self.karpenter, infrastructure=self.infrastructure, workload=self.workload, hpa=self.hpa, speed_factor=self.speed_factor)
        manager.run()

def main(argv=None):
    parser = argparse.ArgumentParser(description="Replay a Kubernetes trace")
    parser.add_argument('trace_path')
    parser.add_argument('nodepool_path')
    parser.add_argument('karpenter', choices=['karpenter-on', 'karpenter-off'])
    parser.add_argument('tracer_skip', choices=['skip-tracer', 'no-skip'])
    parser.add_argument('infrastructure', choices=['static', 'dynamic'])
    parser.add_argument('workload', choices=['static', 'dynamic'])
    parser.add_argument('hpa', choices=['hpa-on', 'hpa-off'])
    parser.add_argument('--speed-factor', default='1')
    args = parser.parse_args(argv)
    try:
        instance = Main(args.trace_path, args.nodepool_path,
                        args.karpenter == 'karpenter-on',
                        args.tracer_skip == 'skip-tracer',
                        args.infrastructure, args.workload, args.hpa == 'hpa-on',
                        args.speed_factor)
    except ValueError as error:
        parser.error(str(error))
    instance.apply_nodepool()
    instance.run_tracer()
    instance.run_manager()


if __name__ == "__main__":
    main()
