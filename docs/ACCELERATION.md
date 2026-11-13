# Evaluating accelerated replay

`--speed-factor F` defaults to 1. It scales deadlines and logical sampling
intervals, with one common origin after preparation. Infrastructure and workload
may be static or dynamic. Acceleration with HPA or Karpenter is rejected before
cluster setup; those modes remain available at 1×. Late events continue in order
with the requested factor and produce lag diagnostics. Preparation, readiness,
HTTP and cleanup timeouts use real seconds. Total execution time is not expected
to improve proportionally to F.

Use `--output-dir PATH` for a new, exclusive output directory per run. Preserve
the raw CSVs and `run.json`; their Unix timestamps are real, cumulative CPU is in
physical core-seconds, memory is in bytes, and requests/limits are unscaled.
Logical intervals are 5 s for usage reconciliation, 15 s for KWOK scraping,
and 30 s for kube-state-metrics scraping and the query grid. Real intervals are
these values divided by F (at 5×: 1 s, 3 s, 6 s, 6 s). Factors must also satisfy
the supported whole-millisecond scrape intervals described in the README.

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
