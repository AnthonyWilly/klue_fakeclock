# 🚀 KLUE – `K`ubernetes `L`ite exec`U`tion `E`nvironment

<img align="right" width="130px" src="assets/logo.png"/>

KLUE is a **emulation tool** that allows developers to **test and validate cloud infrastructure changes** without incurring unnecessary expenses.

It enables seamless **Kubernetes** experimentation, helping teams optimize configurations, improve scalability, and reduce cloud costs.

## 🧩 The KLUE Architecture
![The Code Structure](assets/Diagrama.png)

---
## 🛠 Installation Guide
Execute our emulation tool is a very simple process, but it requires a configured cluster (or the informations to create it automatically). To help you with that, we created some scripts that will help you to install the dependencies and create a cluster, if you want to. If else, just install the dependencies and run the emulation tool passing the cluster context as an argument (you will see more about this on [▶️ Running the KLUE](#-running-the-klue)).

**Note:** We strongly recommend using a Minikube or EKS cluster, but you can also use any other Kubernetes cluster, once you have installed the dependencies to run an emulation.

### 📌 Installing Dependencies
To install the required dependencies for an Minikube or EKS cluster (don't worry about how to create one of these kind of clusters for now), run the following command:
```bash
chmod +x install-dependencies.sh
./install-dependencies.sh
```

If you are using macOS, you can use:
```bash
chmod +x install-dependencies-macos.sh
./install-dependencies-macos.sh
```

### 📌 Prerequisites when emulating with Karpenter
Before running any command, open the `Makefile` on [Karpenter Code](kwok-karpenter-install/karpenter-code/Makefile) and replace `{YourDockerUserName}` with your Docker Hub username:

```makefile
export KWOK_REPO ?= docker.io/{YourDockerUserName}
```

### 📌 Creating an Cluster
To create your cluster, and start your journey with our emulation tool, you just need to run:
```bash
chmod +x create-cluster.sh
./create-cluster.sh
```

**IMPORTANT!!!:** Read [this guide](docs/EKS.md) if you want to use an EKS cluster, you should setup some environment variables before running the script.

---
## 📂 About the Emulation Input
Follow [this guide](docs/INPUT.md) to learn how to prepare the input for KLUE. The input can be a trace collected from a real cluster or a trace you generate yourself, as long as it follows the format defined in our tool's documentation.

## 🚀 Executing the Emulation Tool

### 🔑 Grant Execution Permission
Before running an emulation, grant execution permission to the **execute-emulation.sh** file by running:

```bash
chmod +x execute-emulation.sh
```
### ▶️ Running the KLUE

There are **2 ways** to execute our emulation tool:

1. Starting from a **trace collected from some infrastructure**, using the collected metrics as input. The required format is described in [docs/INPUT.md](docs/INPUT.md).
2. **Creating your own input** by generating a time series that represents the workload and infrastructure behavior you want to emulate. The expected format is also described in [docs/INPUT.md](docs/INPUT.md).

After preparing one of these two input types, our emulation tool supports different execution modes by combining the available flags. Below are the main ways to run the tool, with examples for each scenario. You can combine the flags as needed to fit your use case.

#### 0. **Before Start**
Now that you have decided the kind of cluster you want to use and created it, you can use an existing cluster independing of the kind (EKS, minikube or another).

**OBS:** you can get the cluster context by running `kubectl config current-context`.

#### 1. **Basic Emulation with Existing Cluster**
Use an existing cluster and provide a trace file, it will start an emulation without Karpenter, dynamic infrastructure and workload, and consideer that you don't have and input in the KLUE format:
```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path>
```
**OBS:** The `<trace-path>` should not ends with /, you need to provide a path in the format /path/to/trace

#### 2. **Enable Karpenter (Dynamic Node Management)**
If you want to use Karpenter for dynamic node management, you need to provide the path to the nodepool file:
```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-karpenter --nodepool-path <nodepool-path>
```
**OBS:** The `<trace-path>` should not ends with /, you need to provide a path in the format /path/to/trace

#### 3. **Skip Tracer Step**
Like we said before in [this guide](docs/INPUT.md), you can execute our emulation tool in a lot of scenarious. One of them, is the one which you have one input in the format of our tool.
Add `--skip-tracer` to any command to skip the trace generation step:
```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --skip-tracer
```
**OBS:** The `<trace-path>` should not ends with /, you need to provide a path in the format /path/to/trace

#### 4. **Enable HPA**
If you want the emulation to apply Horizontal Pod Autoscaler objects during workload execution, add the `--use-hpa` flag:
```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-hpa
```
This mode is useful when your input contains HPA-related metrics and you want KLUE to replicate that autoscaling behavior during the emulation.

#### 5. **Static Infrastructure or Workload**
Use static infrastructure and/or workload:
```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --static-infra
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --static-workload
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --static-infra --static-workload
```
**OBS:** The `<trace-path>` should not ends with /, you need to provide a path in the format /path/to/trace

#### Replay speed

Use `--speed-factor F` to scale trace replay deadlines, the final 15-second
logical observation tail, and workload usage reconciliation. The default is
`1`; finite fractional factors are supported when both scrape intervals can be
represented in whole milliseconds. For example, `1.5` and `8` are supported,
whereas `7` is rejected before setup. Acceleration requires emulation
without HPA or Karpenter. Development mode and autoscaling modes accept only
`1`. Factors must be between `1` and `5000` so the shortest logical interval
(5 seconds) remains at least 1 millisecond in real time.

```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --speed-factor 2
```

Infrastructure and workload share an origin established after preparation and
Prometheus port-forward and sampling readiness. Overdue events run in input order without
changing the requested factor or shifting later deadlines. Static modes still
wait through the trace duration. API, readiness and cleanup timeouts remain in
real seconds; preparation and total runtime need not accelerate proportionally.

Initial placement matches pods by namespace and workload owner, validates full
allocation coverage, and confirms each mapped `spec.nodeName` through a shared
list/watch before continuing. Binds use `min(8, ceil(F))` workers (one at 1×),
without changing client QPS or burst. The scheduler has a global 600-real-second
deadline, bounded transient retries and cancellation; an incorrect destination,
missing pod or permanent bind failure stops preparation. Its binary is rebuilt
through Go's build cache on every invocation. Default scheduler manifests are
applied only after all initial destinations are confirmed.

Replay diagnostics are available in memory through each manager's
`replay_diagnostics.snapshot()`: entry counts, skipped actions, call outcomes,
real/logical lag and action timing. A successful usage queue delivery does not
mean reconciliation completed, and a successful API call does not establish
cluster convergence.

Sampling uses logical intervals divided by the requested factor: usage
reconciliation every 5 seconds, KWOK scrape every 15 seconds, and the main
kube-state-metrics scrape and query grid every 30 seconds. For 1×/2×/5×,
KWOK uses 15/7.5/3 real seconds and queries use 30/15/6 real seconds.
Reconciliation runs on monotonic deadlines even while usage events arrive
continuously. Every event is processed in queue order.

Before replay, KLUE overlays the main kube-state-metrics endpoint, reconciles
node ServiceMonitors, checks the loaded Prometheus targets and intervals, and
waits for fresh CPU, memory, requests, limits and owner metrics. The readiness
timeout is 180 real seconds. The kube-state-metrics self endpoint is unchanged.
The Prometheus HTTP API supports a [fractional query step](https://prometheus.io/docs/prometheus/latest/querying/api/#range-queries).

CSV exports keep real Unix timestamps at millisecond precision and unmodified
values: cumulative CPU in core-seconds, memory in bytes, requests and limits.
Batches share one evaluation grid starting at the replay origin. Failed batches
retain their cursor for retry; overlapping series/timestamps are deduplicated,
and new labels extend the file schema. Optional Karpenter/VPA metrics may be
absent. Missing required evaluations, HTTP/JSON/Prometheus errors and stale
source scrapes are exposed as collection errors. Source samples older than
one configured scrape interval plus its scrape timeout are rejected rather
than hidden by Prometheus lookback. Readiness and historical export resolve
that interval and timeout from the series' scrape origin (`job`, `instance`,
and available `endpoint`, `service`, `metrics_path` discriminators), using
Prometheus' effective target configuration with a 1 ms numerical tolerance.
Workload labels such as `pod`, `container` and `namespace` do not identify the
scrape origin. Kubelet/cAdvisor at 30 s/10 s keeps a 40-real-second freshness
limit at every speed factor; KWOK at 2× retains its 7.5 s/7.5 s limit of 15 s.
Unknown origins, invalid configuration and conflicting compatible target policies
fail explicitly. Equivalent targets may share one policy. Optional metrics may
be absent, but present samples must pass the same validation. Observed policies
are retained for historical export after target removal; kubelet targets do not
become prerequisites or contribute to the KWOK/kube-state-metrics watermark.

Stopping wakes collection immediately and flushes every grid point through the
replay's observation tail, including executions shorter than one batch cycle.
Final ingestion has a 60-real-second budget, including HTTP requests. The
coordinator exposes `collection_error`, `collection_diagnostics`, and
`observability` (requested/effective intervals, installed worker counts and
KWOK images) for execution diagnostics. CSV files are archived once after
collection and teardown, with the finalized execution report. These checks establish sampling availability, not fidelity of an
accelerated execution; evaluate that separately on the target machine.

#### Execution results

`--output-dir PATH` selects a new, exclusive directory for raw CSVs,
`emulation.log` and `run.json`. Existing directories and archives are rejected.
Without the flag, KLUE automatically creates
`output_csv_<trace-directory-name>_YYYY-MM-DD-HH:MM:SS` in the current directory.
For example, `--data-path data/full-trace-reduzido-30-minutos/` produces
`output_csv_full-trace-reduzido-30-minutos_2026-11-17-15:30:00`.
Names are sanitized for filesystem use; a numeric suffix avoids collisions with
existing directories or ZIPs. The run ID remains independent of the folder name.
The Python CLI also accepts `--output-dir` as an optional final argument.

The shell creates the report before validation and external setup; Python
continues the same run identity through tracing, preparation, replay,
collection and teardown. Updates use atomic replacement. The version 1 report
contains UTC start/end, Git revision, SHA-256 identities of input files,
requested/effective sampling and KWOK settings, progress and usage acknowledgements,
real/logical lag, and bounded error/warning details with total counts.
Progress snapshots refresh during replay. Full action and lag context remains
in the execution log; Kubernetes object bodies and kubeconfig are not copied
into the report.

The additive `phases` map records monotonic start/end, inclusive `duration_real`,
`result`, `parent`, expected/completed counts, failures, count `unit` and effective
operation `concurrency` where known. Unstarted phases are absent; unknown counts
or concurrency are null. Older version 1 reports without this map remain valid.
Installation returns a local `setup-measurements.jsonl` journal; the shell imports
it only after the synchronous setup process exits. Python owns `run.json` while
replaying, and workers never update the report themselves.

Phases cover external installation, tracing, node/workload application and
convergence, initial usage submission, mapping, scheduler build/binds,
port-forward, metrics readiness, usage acknowledgements, final collection,
teardown and compression. Parent durations include their children; compare
siblings or top-level phases, without summing parents and children. Setup and
bind concurrency are currently 1 at every factor; usage pod-write concurrency
remains separately reported. Application counts measure successful calls,
`initial_usage` counts submissions/scales, and `initial_usage_drain` counts worker
acknowledgements. Convergence counts retain the existing observed node/pod policy;
a pod-count timeout is recorded as `timed_out` even when setup proceeds. Failure
and interruption retain partial counts and elapsed time. These diagnostics do
not change readiness policy or prove correct pod placement.

The archive phase uses `recorded_before_close`: its final snapshot includes CSV
compression but precedes writing `run.json` into the ZIP and closing the archive.
`timing.total_real` has the same internal boundary. Measure wall time around the
complete shell command for end-to-end comparisons through ZIP readiness; no
second archive is created to include the cost of archiving itself.

Statuses are `running`, `completed`, `completed_with_warnings`, `failed`, or
`interrupted`. Late events preserve input order and produce warnings. Failed
applications, lost usage acknowledgements, premature worker termination,
incomplete required collection, teardown and archive errors produce nonzero
exit status. Missing optional metrics may warn. Cleanup errors retain the
original failure. A failed archive leaves its error beside the CSVs in
`run.json`; a successful ZIP includes the finalized report.

Timing separates preparation, replay and finalization, including an explicit
15-logical-second observation tail after the usage queue drains. The effective
factor divides scheduled logical duration by real replay duration and excludes
the tail and preparation/finalization. Incomplete replays have no effective
factor. Unstarted timing fields are null. These are operational diagnostics; the
report makes no fidelity judgment. SIGINT/SIGTERM attempt orderly cleanup;
SIGKILL, machine loss or unavailable storage may leave the initial `running`
report without a final result.

#### Visual comparison

Raw accelerated CSVs use real time. Prepare explicit logical-time views before
passing them to the R scripts; cumulative CPU needs the matching value
transformation to preserve cores. See [the evaluation guide](docs/ACCELERATION.md)
for the preparer, legacy inputs, manual baseline acceptance and controlled
1×/2×/5× and worker experiments.

#### KWOK worker experiments

`--kwok-workers-config PATH` accepts a YAML or JSON mapping containing any of
`podPlayStageParallelism`, `nodePlayStageParallelism`, and
`nodeLeaseParallelism`. Values must be positive uint32 integers. Omitted fields
retain the installed release configuration; without the flag KLUE does not
override worker counts. Unknown fields and invalid values fail before setup.
The overrides are merged into the release's `kwok` ConfigMap
(`config.kwok.x-k8s.io/v1alpha1`, `KwokConfiguration`); setup restarts the
controller to load its mounted config. Lease duration and other options are
preserved. This follows the [KWOK configuration contract](https://kwok.sigs.k8s.io/docs/user/configuration/).

```bash
./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --speed-factor 2 --kwok-workers-config /absolute/path/workers.yaml
```

Use the same KWOK image, trace and machine when comparing worker settings.
Worker counts are an experiment parameter; no increased defaults or speedup
are assumed.

#### 6. **Playground (Development) Mode**
Set up a development environment (no emulation), it's just for test Karpenter actions manually:
```bash
./execute-emulation.sh --dev --use-cluster <cluster-context>
```
You can also combine with `--use-karpenter` if needed (it will install the karpenter in your cluster).

#### 7. **Replicating a Previous Usage Scenario**
If you want to replicate a previous execution scenario, use the same trace directory, cluster context, and optional flags that describe the original behavior. Some common examples are:

- Emulation with existing cluster, Karpenter, static infra, and skip tracer:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-karpenter --nodepool-path <nodepool-path> --static-infra --skip-tracer
  ```
- Emulation with existing cluster, dynamic infra, dynamic workload, and HPA:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-hpa
  ```
- Emulation with existing cluster, Karpenter, static workload, and HPA:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-karpenter --nodepool-path <nodepool-path> --static-workload --use-hpa
  ```

This makes it easier to reproduce the same execution conditions when comparing results across different experiments.

---

#### ℹ️ **Combining Flags**
You can combine the flags above to create different execution modes, for example:
- Emulation with existing cluster, Karpenter, static infra, and skip tracer:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-karpenter --nodepool-path <nodepool-path> --static-infra --skip-tracer
  ```
- Emulation with existing cluster, dynamic infra, and workload:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path>
  ```
- Emulation with existing cluster, HPA enabled:
  ```bash
  ./execute-emulation.sh --sim --use-cluster <cluster-context> --data-path <trace-path> --use-hpa
  ```

#### 8. **Help**
To see all available options and combinations, run:
```bash
./execute-emulation.sh --help
```

> **Note:** Some flags require others (e.g., `--use-karpenter` requires `--nodepool-path`). If you provide invalid or missing combinations, the script will show an error message.

---

## 🧪 Testing
If you want to contribue with our project, we have some simple tests which ensure the correct functioning of the KLUE flow. It will help you to see if have inserted some bug in the code or if you have broken something. To run the tests, just run:
```bash
PYTHONPATH=./src pytest tests/test_klue_workflow.py
```

We have test cases covering the main execution modes of the emulation tool.

**OBS:** The tests may take a while to run, as they involve setting up a Kubernetes cluster and running the emulation tool multiple times. In average, it takes around 20 - 25 minutes to run all tests depending on your network connection.

---
## ⚠️ Important Precautions

When running the emulation multiple times on the same cluster, that are **three main steps** needed for guaranteeing that you will have a correct execution, these are:

- **Removing all nodepools (only when using Karpenter):**
```bash
kubectl delete nodepools --all
```

- **Cleaning the /tmp:**
```bash
rm /tmp/*.csv
rm /tmp/*.json
```

- **Deleting all deployments (depending of your input):**
```bash
kubectl delete deployments -A --all
kubectl delete no --all
```

---
## 👥 Team
- **Kayky Fidelis – Undergraduate Student, Federal University of Campina Grande (UFCG)**
- **Geraldo Sobreira – Undergraduate Student, Federal University of Campina Grande (UFCG)**
- **Matheus Palmeira Leite Rocha – Undergraduate Student, Federal University of Campina Grande (UFCG)**
- **Julia da Silva Leal – Undergraduate Student, Federal University of Campina Grande (UFCG)**  
- **Anthony Willy Miranda Pereira – Undergraduate's Student, Federal University of Campina Grande (UFCG)**
- **Bruno Grangeiro Bonifácio – Master's Student, Federal University of Campina Grande (UFCG)**
- **Giovanni Farias – Federal University of Campina Grande (UFCG)**  
- **Fábio Morais – Federal University of Campina Grande (UFCG)**  
- **Thiago Emmanuel Pereira – Federal University of Campina Grande (UFCG)**  
