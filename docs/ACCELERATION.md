# Evaluating accelerated replay

`--speed-factor F` defaults to 1. It scales deadlines and logical sampling
intervals, with one common origin after preparation. Infrastructure and workload
may be static or dynamic. Acceleration with HPA or Karpenter is rejected before
cluster setup; those modes remain available at 1×. Late events continue in order
with the requested factor and produce lag diagnostics. Preparation, readiness,
HTTP and cleanup timeouts use real seconds. Total execution time is not expected
to improve proportionally to F.

The usage worker processes every event in FIFO order. Within one event, up to
four independent pod usage writes run concurrently; all must finish before the
event is acknowledged. Periodic reconciliation groups a single pod snapshot
by workload, with one deployment snapshot when empty workloads need checking.
Initial usage is drained before the common replay origin. After replay, queued
events are acknowledged before the observable tail, while the worker continues
periodic reconciliation until the tail ends. Healthy backlog and normal shutdown
may exceed 30 real seconds: API completions and acknowledgements reset a 230-second
inactivity deadline. Failure/interruption cleanup keeps its bounded stop and kill
deadlines. No event is coalesced or discarded. `run.json` usage progress includes
API operation counts, failures, total/max real duration and pod-write concurrency;
these diagnostics allow capacity to be measured on the execution machine.

Final collection distinguishes node deletion from missing monitoring. It tracks
initial nodes and successfully completed infrastructure replay actions, checks
the current fake-node set, and requires scrape targets for live nodes. When the
replay has removed every emulated node and the API confirms none remain, KWOK
targets are no longer required; healthy kube-state-metrics scrapes still advance
the export watermark. Removed targets left temporarily in discovery are ignored.
Unexpected node disappearance, missing live targets, stale/missing historical
samples and HTTP errors remain collection errors. `run.json` collection metadata
includes expected/live node counts and `kwok_targets_not_required`. Autoscaler
modes retain their existing target policy.

Freshness uses each series' effective Prometheus scrape interval plus timeout,
with a 1 ms numerical tolerance, both before replay and during historical export.
The collector matches `job` and `instance`, disambiguating by `endpoint`,
`service` and `metrics_path` when available on both series and targets. Workload
labels are excluded from origin matching. Multiple matching targets must agree
on the policy; unknown origins, invalid policies and conflicts fail explicitly.
Kubelet/cAdvisor scrapes are not divided by the speed factor: 30 s/10 s permits
40 real seconds even at 5×, while KWOK at 5× permits only 3 s + 3 s. Kubelet
is catalogued when present without becoming required or entering the watermark.
Removed targets retain their observed policies for historical validation.
Intervals are assumed stable during one execution; arbitrary historical
configuration changes are not reconstructed. Raw values, labels, the query grid
and batch atomicity are unchanged.

Output is automatic: `output_csv_<trace-directory-name>_YYYY-MM-DD-HH:MM:SS`
in the current directory. `--output-dir PATH` is an optional override, and
existing outputs are never overwritten. Preserve
the raw CSVs and `run.json`; their Unix timestamps are real, cumulative CPU is in
physical core-seconds, memory is in bytes, and requests/limits are unscaled.
Logical intervals are 5 s for usage reconciliation, 15 s for KWOK scraping,
and 30 s for kube-state-metrics scraping and the query grid. Real intervals are
these values divided by F (at 5×: 1 s, 3 s, 6 s, 6 s). Factors must also satisfy
the supported whole-millisecond scrape intervals described in the README.

Setup reapplies `kwok-karpenter-install/configuration-files/kube-state-metrics-resources.yaml`
after the kube-prometheus manifests. This CPU overlay gives kube-state-metrics
a 100m request and 1000m limit, and its main HTTPS proxy a 100m request and 500m
limit. It preserves memory settings and the telemetry proxy. The release's 40m
main-proxy limit caused scrape timeouts on a large trace even at 1×. These
settings provide initial headroom; verify scrape health at each speed factor.
Setup also repairs legacy ServiceMonitor apply metadata before reapplying the
release manifests. Sampling intervals are updated with a patch of endpoints,
so server-owned metadata is never saved into the last-applied configuration.
On a fresh cluster, setup waits up to 300 real seconds for the operator to
create the Prometheus pod, then up to 300 real seconds for it to become Ready.
Failures remain visible in the execution log and stop setup with a nonzero code.

## Prepare comparison views locally

The preparer is a separate, explicit command. It does not run during emulation,
contact a cluster, install R, or produce plots:

```bash
python src/prepare_comparison.py --input /results/run-1x --output /results/view-1x
python src/prepare_comparison.py --input /results/run-2x --output /results/view-2x
python src/prepare_comparison.py --input /results/run-5x --output /results/view-5x
```

Each input directory must contain the raw CSVs and its adjacent version 1
`run.json`. Factor and real replay origin come only from that report. The output
must not exist and must be outside the input directory. The report and raw files
are never changed or copied as if they were derived data. A `comparison.json`
manifest records source run identity, revision, report and CSV SHA-256 hashes,
status, factor, origin, retained window, row counts, transformation and warnings.
A directory with this marker cannot be transformed again.

For original/legacy data without a report, explicitly provide factor 1 and the
smallest real Unix timestamp consumed by the R pipeline:

```bash
python src/prepare_comparison.py --input /results/original --output /results/view-original \
  --speed-factor 1 --real-origin <minimum-consumed-unix-timestamp>
```

Do not infer factor from directory names or total duration. Legacy completeness
is unknown; the manifest records the available window rather than inventing
replay boundaries. With `run.json`, the window starts at `timing.origin_unix`
and ends after the recorded `replay_real + tail_real`. An absent tail in a partial
run contributes zero. Preparation and samples beyond that window are removed.
Failed, interrupted or ongoing runs with sufficient timing data can be projected,
but are prominently marked partial. Missing origin or replay duration is an error.
Operational errors and warnings in the source report are also called out.

Every timestamp becomes `(real_timestamp - real_origin) * F`. Labels, including
`exported_namespace` and `exported_pod`, and column order are preserved. No
rounding, interpolation, synthetic origin sample or per-metric shift is applied.
The minimum timestamp after the R consumer's filters must be zero. Otherwise the
preparer fails with an alignment error: the R scripts would subtract another
origin and shift the curves. For legacy data the supplied origin must equal the
minimum consumed timestamp before cropping, too.

Cumulative CPU values are multiplied by F for these explicit counters:
`container_cpu_usage_seconds_total`, `pod_cpu_usage_seconds_total`,
`node_cpu_usage_seconds_total`, and `node_cpu_seconds_total`. Two cores over one
real second at 5× produce a delta of 10 logical core-seconds over five logical
seconds: the derived rate is still two cores. Counter resets and initial samples
remain intact; the R consumer masks negative deltas and the initial undefined
rate. Memory, pod counts, requests, limits and unknown metrics retain their
values. Memory still converts from bytes to GiB by dividing by 1024³.

The preparer requires the R schemas for container CPU, memory working-set (or
usage-bytes as fallback), container resource requests and limits with cpu/memory
resources, pod owner and replicaset owner. Missing files, columns, window samples,
invalid numbers or missing alignment fail before publishing a view. This checks
input compatibility, not the fidelity or completeness of every individual curve.
The derived view is for analysis; use raw physical CSVs for a new tracer input.

## External evaluation on the experiment machine

Run these steps yourself on the target machine, using the same trace, trace
slice, machine capacity, configuration, static/dynamic mode and KWOK version for
all factors. Pin/verify the installed KWOK version: setup fetching latest must
not silently change the controller between experiments. R and the packages used
by the existing scripts are external prerequisites.

1. Run without HPA/Karpenter, choosing distinct output directories:

   ```bash
   ./execute-emulation.sh --sim --use-cluster <context> --data-path <trace> --speed-factor 1 --output-dir /results/run-1x
   ./execute-emulation.sh --sim --use-cluster <context> --data-path <trace> --speed-factor 2 --output-dir /results/run-2x
   ./execute-emulation.sh --sim --use-cluster <context> --data-path <trace> --speed-factor 5 --output-dir /results/run-5x
   ```

   Cover dynamic infrastructure/workload and at least one static combination
   with the same input slice. Append the appropriate `--static-infra` and/or
   `--static-workload` flags consistently to every factor in that combination.

2. Inspect each `run.json`: `status`, `errors`, `warnings`, `progress`, `lag`,
   `collection` and `observability`. Check event counts, usage acknowledgements,
   queue completion, missing query evaluations, stale scrapes, collection errors
   and application/reconciliation lag. Statuses are `running`, `completed`,
   `completed_with_warnings`, `failed` or `interrupted`. A successful API call
   does not prove convergence. A completed run is not automatically faithful.

3. Record `timing.preparation_real`, `replay_real`, `tail_real`,
   `finalization_real`, `total_real`, the requested factor and `effective_factor`.
   The effective factor is scheduled logical duration divided by real replay
   duration, excluding preparation, observation tail and finalization. It is null
   for incomplete replays. Finalization includes the tail; do not add it twice.
   A late but complete run may have a lower effective factor than requested.
   Inspect `phases` for the installation and preparation breakdown, partial
   counts and effective concurrency. Durations are inclusive: do not sum a
   parent with its children. Reports without `phases` remain supported.
   `archive.result=recorded_before_close` marks the internal snapshot before
   the report itself is written and the ZIP closes. For total command time,
   measure externally, for example:

   ```bash
   /usr/bin/time -p ./execute-emulation.sh --sim --use-cluster <context> --data-path <trace> --speed-factor 1 --output-dir /results/timed-1x
   ```

   Use distinct outputs and identical other arguments for the second factor.
   Keep cold installation and already-installed preparation measurements
   separate; the internal `total_real` is not exact time through ZIP readiness.

4. Prepare the original and 1× views and inspect them first:

   ```bash
   Rscript /home/ubuntu/klue/plots-scripts/main_doubleplots.R /results/view-original /results/view-1x /results/plots-original-1x --scenario-a-label=original --scenario-b-label=1x --bucket-size=300
   ```

   Record manual acceptance of the candidate 1× baseline outside `run.json`.
   Only after acceptance compare 1×/2× and 1×/5×:

   ```bash
   Rscript /home/ubuntu/klue/plots-scripts/main_doubleplots.R /results/view-1x /results/view-2x /results/plots-1x-2x --scenario-a-label=1x --scenario-b-label=2x --bucket-size=300
   Rscript /home/ubuntu/klue/plots-scripts/main_doubleplots.R /results/view-1x /results/view-5x /results/plots-1x-5x --scenario-a-label=1x --scenario-b-label=5x --bucket-size=300
   ```

   These paths refer to the existing external R consumer; R scripts are not
   shipped or modified here. Prioritize aggregate pod count, CPU and memory,
   then individual curves. Use 300 logical seconds for all utilization buckets;
   projecting before bucketing makes their boundaries relative to the common
   experiment origin. Never pass accelerated raw CSVs directly as logical time.
   There is no automatic score, numerical fidelity tolerance or acceptance seal.

## Controlled worker experiment

Measure 1×/2×/5× with installed defaults first. Then pass an explicit
`--kwok-workers-config /absolute/path/workers.yaml`, varying one configuration at
a time while keeping machine, input, KWOK image, intervals and mode fixed. The
mapping accepts `podPlayStageParallelism`, `nodePlayStageParallelism` and
`nodeLeaseParallelism`, each a positive uint32 integer; omitted fields retain the
installed values. Without an override, defaults are preserved.

Compare repeated runs for lag, errors, machine resource usage, effective factor,
phase durations and the same visual curves. Record requested and installed
worker counts from `run.json`. Recommend worker values only after measurements;
additional workers are a capacity hypothesis, not a guaranteed speedup.


## Setup capacity and end-to-end acceptance

The factor also selects `min(8, ceil(F))` setup application and bind workers
(one at 1×, two at 1.5×/2×, five at 5×). Independent resources can overlap;
operations on one namespace/kind/name remain serial with isolated object bodies.
Namespaces precede applications, fake nodes precede binds, and all parallel
results are joined. Errors stop preparation. This does not change KWOK workers,
client QPS/burst, usage distribution or FIFO event order; the usage pool still
allows four pod writes within one event and acknowledges completion.

Setup identifies fake nodes explicitly, ignoring real-node counts and rejecting
extra fake nodes. NodeClaims resolve their destination via `status.nodeName`.
Pod creation requires exact per-namespace/workload-owner coverage, using the
`deployment` label produced by the trace generator. Pre-existing workload pods
cannot satisfy creation. Other owners/namespaces cannot fill missing coverage.
Creation accepts Pending pods before custom binds. Afterwards the same pod UIDs
must appear on the mapped nodes, Running with a True Ready condition, before
replay. Setup uses 600-real-second global deadlines for node convergence, pod
creation and post-bind operational readiness, with bounded API requests. They
are not scaled by F or used as target durations; expiry fails with pending
identities. These are initial limits whose sufficiency needs reference-machine
validation. Usage acknowledgements and existing metrics freshness/readiness
remain required before establishing the common origin.

Evaluate total command wall time through the closed ZIP, not just replay or
`timing.total_real` (which is recorded before archive closure). Use at least
three paired 1×/2× runs with alternating order, the same candidate revision,
trace, machine, modes, versions and clean initial state. Separate cold install
from already-installed preparation. Record medians, dispersion and the dominant
setup/replay/finalization stages, backlog, input/image identities, collection
coverage and logical grid counts. The experimental total-time target is
`median(T2) / median(T1) <= 0.55`, with no material candidate-1× regression
against the approved reference. It is not a replay-factor guarantee.

Accept duration and fidelity together: inspect candidate 1× against the approved
reference, then candidate 2× against candidate 1×, using the comparison views
above with a common origin, identical logical buckets and no interpolation.
Check initial transitions, aggregate pods, CPU, memory and individual curves.
Keep raw counters/CSVs unchanged; shorter setup changes their initial history.
There is no numerical fidelity tolerance implied by the 10% timing margin.
A faster setup or passing unit tests alone does not establish the total target.
If API/KWOK capacity or replay backlog prevents it, report the remaining budget
and bottleneck without shortening convergence waits, dropping events or
relaxing collection freshness. Store plots and the manual acceptance separately
from `run.json`.
