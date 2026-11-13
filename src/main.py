"""
This class is responsible for running a trace emulation process. It applies a Kubernetes 
node pool configuration, initializes a tracer to process trace data, and runs a broker 
to handle the emulation steps. The script interacts with Kubernetes and processes trace 
files to simulate cluster behavior.
"""
import argparse
from experiment_clock import validate_speed_factor
from run_report import RunReport
import signal
from kwok_config import read_workers
import os
import subprocess
from tracer.tracer_karpenter import TracerKarpenter
from tracer.tracer_kwok import TracerKWOKOnly
from manager import Manager

class Main:
    def __init__(self, trace_path, nodepool_path, karpenter, tracer_skip, infrastructure, workload, hpa, speed_factor=1, kwok_workers_config=None, output_dir=None, report=None):
        self.report = report or RunReport(output_dir)
        self.report.update(configuration={**self.report.data['configuration'],
            'mode': 'emulation', 'speed_factor': speed_factor, 'karpenter': karpenter,
            'hpa': hpa, 'infrastructure': infrastructure, 'workload': workload})
        self.speed_factor = validate_speed_factor(speed_factor, karpenter=karpenter, hpa=hpa)
        self.kwok_workers_config = kwok_workers_config
        read_workers(kwok_workers_config)
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
            self.report.stage('tracer')
            # Tracers expose the input paths they read; record identities, not data.
            for key, path in vars(tracer).items():
                if key.endswith('_path') and isinstance(path, str) and os.path.isfile(path):
                    self.report.trace(path, 'tracer-input')
            tracer.run()

    def run_manager(self):
        """
        Initializes and runs the Manager to handle the emulation process.
        This method creates an instance of the Manager class with the provided
        parameters and calls its run method to start the emulation.
        """
        # Executa o broker.py
        manager = Manager(karpenter=self.karpenter, infrastructure=self.infrastructure, workload=self.workload, hpa=self.hpa, speed_factor=self.speed_factor, kwok_workers_config=self.kwok_workers_config, report=self.report)
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
    parser.add_argument('--kwok-workers-config')
    parser.add_argument('--speed-factor', default='1')
    parser.add_argument('--output-dir')
    args = parser.parse_args(argv)
    report = RunReport(args.output_dir, resume=bool(os.environ.get('KLUE_RUN_ID')))
    if os.environ.get('KLUE_RUN_ID') and report.data['run_id'] != os.environ['KLUE_RUN_ID']:
        raise ValueError('shell run identity does not match output directory')
    previous_term = signal.getsignal(signal.SIGTERM)
    previous_int = signal.getsignal(signal.SIGINT)
    def interrupted(signum, frame):
        if report.data['stage'] in ('collection', 'teardown', 'archive'):
            report.interruption_requested = True
            report.issue(f'received signal {signum}; completing bounded cleanup', component='main')
            return
        raise KeyboardInterrupt(f'received signal {signum}')
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        instance = Main(args.trace_path, args.nodepool_path,
                        args.karpenter == 'karpenter-on',
                        args.tracer_skip == 'skip-tracer',
                        args.infrastructure, args.workload, args.hpa == 'hpa-on',
                        args.speed_factor, args.kwok_workers_config, report=report)
        instance.apply_nodepool()
        report.stage('tracer')
        instance.run_tracer()
        report.stage('preparation')
        instance.run_manager()
        return 0
    except BaseException as error:
        if report.data['status'] == 'running':
            report.issue(error, component='main')
            report.finish(interrupted=isinstance(error, (KeyboardInterrupt, SystemExit)))
        if isinstance(error, ValueError) and report.data['stage'] == 'validation':
            parser.error(str(error))
        print(f"Emulation {report.data['status']}: {report.path}")
        return 130 if report.data['status'] == 'interrupted' else 1
    finally:
        signal.signal(signal.SIGTERM, previous_term)
        signal.signal(signal.SIGINT, previous_int)


if __name__ == "__main__":
    raise SystemExit(main())
