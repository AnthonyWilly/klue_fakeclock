"""
Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
"""

import subprocess
import time
from datetime import datetime
from experiment_clock import ExperimentClock, validate_speed_factor
from pods_mapping import PodsMapping
from collector import Collector, CollectionCursor, CollectionError
from sampling import SamplingIntervals, configure_ksm
from kwok_config import read_workers, WORKERS
import yaml
from workload.manager import WorkloadManager
from infrastructure.manager import InfrastructureManager
from infrastructure.service_monitor.service_monitor_manager import ServiceMonitorManager
import threading

class Manager:
    """
    Manager class is responsible for managing Kubernetes resources and orchestrating the setup and execution
    of a trace emulation process. It interacts with the Kubernetes API to manage namespaces, deployments,
    nodeclaims, and other resources, while also handling custom logic for scaling and disruption times.
    Attributes:
        input_step (int): Time step interval for processing trace entries.
        data_path (string): The path of the JSON with the objects that will be applied by broker.
    """

    def __init__(self, data_path='/tmp', karpenter=True, infrastructure=None, workload=None, hpa=True, speed_factor=1, kwok_workers_config=None):
        """
        Initializes the Manager class.
        """
        self.speed_factor = validate_speed_factor(speed_factor, karpenter=karpenter, hpa=hpa)
        self.intervals = SamplingIntervals(self.speed_factor)
        self.workers_override = read_workers(kwok_workers_config)
        self.clock = ExperimentClock(self.speed_factor)
        self.collection_stop = threading.Event()
        self.collection_error = None
        self.collection_diagnostics = dict(complete=False, errors=[], requested_intervals=self.intervals.snapshot())
        self.observability = {}
        self.infrastructure = infrastructure
        self.workload = workload

        self.infrastructure_manager = InfrastructureManager(f"{data_path}/infrastructure_description.json", karpenter, infrastructure, speed_factor=self.speed_factor)
        self.workload_manager = WorkloadManager(f"{data_path}/workload_description.json", workload, hpa, speed_factor=self.speed_factor)

        self.pods_mapping = PodsMapping(karpenter)
        self.collector = Collector(step=self.intervals.query, intervals=self.intervals)
        self.service_monitor_manager = ServiceMonitorManager(interval_seconds=self.intervals.kwok)

    def log(self, message):
        """
        Logs a message with a "[BROKER]" prefix.
        """
        print(f"[BROKER] {message}")

    def start_mapping_and_scheduler(self):
        """
        Starts the process of mapping pods and running the scheduler.

        This method performs the following actions:
        1. Executes the `run` method of the `pods_mapping` object to initiate pod mapping.
        2. Runs the `build-scheduler.sh` script using a subprocess call to set up the scheduler.
        """
        self.pods_mapping.run()
        subprocess.run(["bash", "src/build-scheduler.sh"], check=True)

    def prepare_observability(self):
        api = self.service_monitor_manager.k8s_api
        monitor = api.get_namespaced_custom_object(
            'monitoring.coreos.com', 'v1', 'monitoring', 'servicemonitors', 'kube-state-metrics')
        configured = configure_ksm(monitor, self.intervals.ksm)
        api.patch_namespaced_custom_object(
            'monitoring.coreos.com', 'v1', 'monitoring', 'servicemonitors', 'kube-state-metrics',
            {'spec': {'endpoints': configured['spec']['endpoints']}})
        self.initial_nodes = self.service_monitor_manager.prepare()
        config = api.v1.read_namespaced_config_map('kwok', 'kube-system')
        documents = list(yaml.safe_load_all(config.data['kwok.yaml']))
        options = next(doc['options'] for doc in documents if doc.get('kind') == 'KwokConfiguration')
        workers = {key: options.get(key) for key in WORKERS}
        if any(workers[key] != value for key, value in self.workers_override.items()):
            raise ValueError('installed KWOK worker configuration does not match requested overrides; run setup first')
        deployment = api.apps_v1.read_namespaced_deployment('kwok-controller', 'kube-system')
        images = {container.name: container.image for container in deployment.spec.template.spec.containers}
        self.observability = dict(workers=workers, images=images,
            kwok_config_api_version=next(doc['apiVersion'] for doc in documents if doc.get('kind') == 'KwokConfiguration'),
            requested_intervals=self.intervals.snapshot(),
            effective_intervals={**self.collector.wait_ready(self.intervals, self.initial_nodes),
                'query': self.collector.step,
                'reconcile': self.workload_manager.usage_manager.reconcile_interval_seconds})

    def collect_loop(self, start_time):
        """Export confirmed grid points; stop wakes immediately and flushes the tail."""
        cursor = CollectionCursor(start_time, self.intervals.query)
        self.collection_cursor = cursor
        run_id = datetime.now().strftime("%Y-%m-%d-%H:%M:%S.%f")
        self.output_dir = f"output_csv_{run_id}"
        deadline = None
        try:
            while True:
                stopping = self.collection_stop.wait(60 if deadline is None else 1)
                if stopping and deadline is None:
                    deadline = time.monotonic() + 60  # Real ingestion timeout, never scaled.
                    self.collector.request_deadline = deadline
                end = self.collection_end if stopping else time.time()
                try:
                    confirmed = self.collector.targets(self.intervals)
                    while True:
                        window = cursor.window(min(end, confirmed))
                        if window is None:
                            break
                        begin, finish, last = window
                        self.collector.collect(begin, finish, self.output_dir)
                        cursor.commit(last)
                        self.collection_diagnostics['confirmed_through'] = finish
                    if stopping and cursor.window(end) is None:
                        self.collection_diagnostics['complete'] = True
                        break
                except (CollectionError, ValueError, KeyError, TypeError) as error:
                    self.collection_diagnostics['errors'].append(str(error))
                if stopping:
                    if time.monotonic() >= deadline:
                        raise CollectionError('final sampling incomplete after real ingestion timeout')
                    # Event.wait would return immediately once stop is set.
                    time.sleep(1)
        except Exception as error:
            self.collection_error = error
            self.collection_diagnostics['errors'].append(str(error))
        finally:
            try:
                self.collector.archive(self.output_dir)
            except Exception as error:
                self.collection_error = error
                self.collection_diagnostics['errors'].append(str(error))

    def run(self):
        """
        Executes the main workflow of the Broker.

        This method orchestrates the entire lifecycle of the Broker's operation.
        """
        self.log("[INFO] Starting Broker.")

        collector_thread = None

        try:
            self.log("[INFO] Preparing to start emulation.")
            self.infrastructure_manager.before_setup()
            self.workload_manager.before_setup()
            self.service_monitor_manager.start()

            self.log("[INFO] Executing setup of infrastructure and workload.")
            self.infrastructure_manager.setup()

            self.workload_manager.setup()

            self.start_mapping_and_scheduler()

            self.workload_manager.before_emulation()
            self.infrastructure_manager.before_emulation()

            self.log("[INFO] Starting emulation.")
            subprocess.run(["bash", "src/port-forward.sh"], check=True)
            self.prepare_observability()
            self.clock.start()

            # 1. Criar as threads para os métodos de emulação
            infra_emulation_thread = threading.Thread(
                target=self.infrastructure_manager.emulation,
                args=(self.clock,),
                name="InfraEmulationThread"
            )
            workload_emulation_thread = threading.Thread(
                target=self.workload_manager.emulation,
                args=(self.clock,),
                name="WorkloadEmulationThread"
            )

            start_time = self.clock.origin_unix
            self.collection_stop.clear()

            collector_thread = threading.Thread(
                target=self.collect_loop,
                args=(start_time,),
                name="CollectorThread"
            )

            self.log("[INFO] Starting collector thread")
            collector_thread.start()

            self.log("[INFO] Starting emulation thread for InfrastructureManager.")
            infra_emulation_thread.start()
            self.log("[INFO] Starting emulation thread for WorkloadManager.")
            workload_emulation_thread.start()
        
            infra_emulation_thread.join()
            self.log("[INFO] InfrastructureManager emulation thread completed.")
            workload_emulation_thread.join()
            self.log("[INFO] WorkloadManager emulation thread completed.")

        finally:
            self.collection_end = time.time()
            self.collection_stop.set()

            if collector_thread:
                collector_thread.join()
                self.log("[INFO] Collector thread stopped.")
    
            self.service_monitor_manager.stop()

            self.log("[INFO] Emulation completed. Tearing down infrastructure, workload and temp files.")

            try:
                self.workload_manager.tear_down()
            except Exception:
                self.log("[ERROR] Failed to tear down workload. Traceback follows.")
                self.log(f"[ERROR] Failed to tear down workload: {error}")

            try:
                self.infrastructure_manager.tear_down()
            except Exception:
                self.log("[ERROR] Failed to tear down infrastructure. Traceback follows.")
                self.log(f"[ERROR] Failed to tear down infrastructure: {error}")