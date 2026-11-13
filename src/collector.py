"""Export raw Prometheus samples on one replay-anchored evaluation grid."""
import csv
from decimal import Decimal, ROUND_FLOOR
import glob
import math
import os
import time
import zipfile
from datetime import datetime
from sampling import SamplingIntervals, duration, parse_duration
import requests


class CollectionError(RuntimeError):
    pass


# Other metrics (including Karpenter and VPA) are optional in KWOK-only runs.
REQUIRED_METRICS = {
    'container_cpu_usage_seconds_total',
    'kube_pod_container_resource_requests', 'kube_pod_container_resource_limits',
    'kube_pod_owner', 'kube_replicaset_owner',
}
MEMORY_METRICS = {'container_memory_working_set_bytes', 'container_memory_usage_bytes'}


class CollectionCursor:
    """Integer indices avoid drift, skipped windows and off-grid batch starts."""
    def __init__(self, origin, step):
        self.origin = Decimal(str(origin))
        self.step = Decimal(str(step))
        self.next_index = 0

    def timestamp(self, index):
        return float(self.origin + index * self.step)

    def window(self, confirmed_end, max_points=1000):
        last = int(((Decimal(str(confirmed_end)) - self.origin) / self.step).to_integral_value(rounding=ROUND_FLOOR))
        last = min(last, self.next_index + max_points - 1)
        if last < self.next_index:
            return None
        return self.timestamp(self.next_index), self.timestamp(last), last

    def commit(self, last):
        self.next_index = last + 1


class Collector:
    def __init__(self, step, metrics_file='src/metrics.txt', prometheus_host="http://localhost:30222", intervals=None):
        self.intervals = intervals or SamplingIntervals()
        self.request_deadline = None
        self.prometheus_host = prometheus_host
        self.step = float(step)
        if not math.isfinite(self.step) or self.step <= 0:
            raise ValueError('query step must be positive and finite')
        duration(self.step)
        self.step = round(self.step, 3)
        self.metrics_file = metrics_file
        self.errors = []
        self._last_written = {}
        self.effective_intervals = {}

    def log(self, message):
        print(f"[COLLECTOR] {message}")

    def read_metrics(self):
        with open(self.metrics_file, encoding='utf-8') as stream:
            return [line.strip() for line in stream if line.strip()]

    def api(self, endpoint, params=None):
        try:
            timeout = 10
            if self.request_deadline is not None:
                timeout = min(timeout, self.request_deadline - time.monotonic())
                if timeout <= 0:
                    raise CollectionError('real HTTP collection deadline exceeded')
            response = requests.get(f'{self.prometheus_host}/api/v1/{endpoint}', params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            if data.get('status') != 'success' or 'data' not in data:
                raise ValueError(f"Prometheus error: {data.get('error', data)}")
            if data.get('warnings'):
                raise ValueError(f"Prometheus warnings: {data['warnings']}")
            return data['data']
        except (requests.RequestException, ValueError, TypeError, AttributeError) as error:
            raise CollectionError(f'{endpoint}: {error}') from error

    def request_metrics(self, metric, start_time, end_time):
        data = self.api('query_range', dict(query=metric, start=format(start_time, '.3f'),
                        end=format(end_time, '.3f'), step=str(self.step)))
        if not isinstance(data, dict) or data.get('resultType') != 'matrix' or not isinstance(data.get('result'), list):
            raise CollectionError(f'invalid matrix response for {metric}')
        return data['result']

    @staticmethod
    def require_metrics(available):
        missing = REQUIRED_METRICS - available
        if missing or not (MEMORY_METRICS & available):
            raise CollectionError(f'required metrics absent: {sorted(missing)}; memory available: {sorted(MEMORY_METRICS & available)}')

    def targets(self, intervals, nodes=()):
        """Inspect loaded target configuration and completed scrapes, not applied manifests."""
        data = self.api('targets', {'state': 'active'})
        kwok, ksm = [], []
        for target in data['activeTargets']:
            url = target.get('scrapeUrl', '')
            pool = target.get('scrapePool', '')
            if '/metrics/nodes/' in url:
                kwok.append(target)
            elif pool == 'serviceMonitor/monitoring/kube-state-metrics/0':
                ksm.append(target)
        if not ksm or not kwok or any(not any(f'/metrics/nodes/{node}/' in t['scrapeUrl'] for t in kwok) for node in nodes):
            raise CollectionError('required scrape targets are not active')
        for group, interval, key in ((kwok, intervals.kwok, 'kwok'), (ksm, intervals.ksm, 'kube_state_metrics')):
            for target in group:
                if (target.get('health') != 'up' or parse_duration(target['scrapeInterval']) != interval
                        or parse_duration(target['scrapeTimeout']) > interval):
                    raise CollectionError(f'{key} target not ready at requested interval {duration(interval)}')
            self.effective_intervals[key] = interval
        timestamps = []
        for target in kwok + ksm:
            timestamps.append(datetime.fromisoformat(target['lastScrape'].replace('Z', '+00:00')).timestamp())
        return min(timestamps)

    def wait_ready(self, intervals, nodes=(), timeout=180, monotonic=time.monotonic, wait=time.sleep, unix_time=time.time):
        previous_deadline = self.request_deadline
        self.request_deadline = time.monotonic() + timeout
        try:
            deadline = monotonic() + timeout
            while True:
                try:
                    self.targets(intervals, nodes)
                    available = set()
                    for metric in self.read_metrics():
                        if metric in REQUIRED_METRICS | MEMORY_METRICS:
                            data = self.api('query', {'query': f'timestamp({metric})'})
                            interval = intervals.kwok if metric.startswith('container_') else intervals.ksm
                            values = data.get('result', [])
                            if values and all(math.isfinite(float(item['value'][1])) and
                                              unix_time() - float(item['value'][1]) <= interval + min(10, interval) + .001
                                              for item in values):
                                available.add(metric)
                    self.require_metrics(available)
                    return dict(self.effective_intervals)
                except (CollectionError, KeyError, TypeError, ValueError) as error:
                    if monotonic() >= deadline:
                        raise CollectionError(f'observability readiness timed out: {error}') from error
                    wait(min(1, max(0, deadline - monotonic())))
        finally:
            self.request_deadline = previous_deadline

    def validate_source_samples(self, metric, results, start, end):
        """PromQL lookback must not hide a scrape outage behind repeated values."""
        if not results:
            return
        sources = self.request_metrics(f'timestamp({metric})', start, end)
        interval = self.intervals.kwok if metric.startswith('container_') else self.intervals.ksm
        max_age = interval + min(10, interval)  # Allow one real scrape timeout for ingestion.
        observed = {}
        for series in sources:
            labels = tuple(sorted((key, value) for key, value in series['metric'].items() if key != '__name__'))
            for timestamp, source_timestamp in series['values']:
                observed[(labels, round(float(timestamp), 3))] = float(source_timestamp)
        for series in results:
            labels = tuple(sorted((key, value) for key, value in series['metric'].items() if key != '__name__'))
            for timestamp, _ in series['values']:
                source = observed.get((labels, round(float(timestamp), 3)))
                if source is None or not math.isfinite(source) or float(timestamp) - source > max_age + .001:
                    raise CollectionError(f'{metric}: missing or stale scrape at {timestamp}')

    def _write(self, output_dir, metric, results):
        if not results:
            return
        path = os.path.join(output_dir, f'{metric}.csv')
        labels = sorted({key for result in results for key in result['metric']})
        old_labels = []
        if os.path.exists(path):
            with open(path, newline='', encoding='utf-8') as stream:
                old_labels = next(csv.reader(stream))[3:]
        labels = sorted(set(labels) | set(old_labels))
        header = ['name', 'timestamp', 'value'] + labels
        # Schema grows only when a new label appears. Rewrite old rows by name,
        # never append differently ordered columns under an existing header.
        if old_labels != labels and os.path.exists(path):
            with open(path, newline='', encoding='utf-8') as source, open(path + '.tmp', 'w', newline='', encoding='utf-8') as dest:
                reader = csv.DictReader(source)
                writer = csv.DictWriter(dest, header, restval='')
                writer.writeheader()
                writer.writerows(reader)
            os.replace(path + '.tmp', path)
        exists = os.path.exists(path)
        committed = dict(self._last_written)
        with open(path, 'a', newline='', encoding='utf-8') as stream:
            writer = csv.writer(stream)
            if not exists:
                writer.writerow(header)
            for result in results:
                series = tuple(sorted(result['metric'].items()))
                key = (metric, series)
                for timestamp, value in sorted(result['values'], key=lambda pair: float(pair[0])):
                    if float(timestamp) <= committed.get(key, -math.inf):
                        continue
                    writer.writerow([result['metric'].get('__name__', metric), timestamp, value] +
                                    [result['metric'].get(label, '') for label in labels])
                    committed[key] = float(timestamp)
        self._last_written = committed

    def collect(self, start_time, end_time, output_dir):
        """Commit a batch only after all queries and mandatory grid checks succeed."""
        try:
            batches = {metric: self.request_metrics(metric, start_time, end_time) for metric in self.read_metrics()}
            cursor = CollectionCursor(start_time, self.step)
            window = cursor.window(end_time, max_points=11000)
            expected = {round(cursor.timestamp(index), 3) for index in range(window[2] + 1)}
            available_by_time = {timestamp: set() for timestamp in expected}
            for metric, results in batches.items():
                for result in results:
                    for timestamp, value in result['values']:
                        timestamp = round(float(timestamp), 3)
                        if timestamp in available_by_time:
                            available_by_time[timestamp].add(metric)
            for available in available_by_time.values():
                self.require_metrics(available)
            for metric, results in batches.items():
                self.validate_source_samples(metric, results, start_time, end_time)
            os.makedirs(output_dir, exist_ok=True)
            for metric, results in batches.items():
                self._write(output_dir, metric, results)
        except (CollectionError, KeyError, TypeError, ValueError, OSError) as error:
            self.errors.append(dict(start=start_time, end=end_time, error=str(error)))
            raise CollectionError(f'collection {start_time}..{end_time}: {error}') from error

    def archive(self, output_dir):
        """
        Compresses the CSV files in `output_dir` into `<output_dir>.zip`.

        Meant to run once, after the last collection. Prometheus exports are
        highly repetitive (pod and namespace strings repeat on every row), so
        DEFLATE shrinks them several times over; the default ZIP_STORED would
        not compress at all.
        """
        csv_files = sorted(glob.glob(f"{output_dir}/*.csv"))
        if not csv_files:
            self.log(f"[INFO] No CSV files in {output_dir}; skipping archive.")
            return

        with zipfile.ZipFile(f"{output_dir}.zip", "w",
                             compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
            for file in csv_files:
                zf.write(file, arcname=os.path.basename(file))

        self.log(f"[INFO] CSV files zipped to {output_dir}.zip")
