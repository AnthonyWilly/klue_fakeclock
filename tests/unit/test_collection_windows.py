import csv
from datetime import datetime, timezone
from unittest.mock import Mock

import pytest
import requests

from collector import Collector, CollectionCursor, CollectionError, REQUIRED_METRICS, MEMORY_METRICS
from sampling import SamplingIntervals, duration


METRICS = sorted(REQUIRED_METRICS | MEMORY_METRICS)


def make_collector(tmp_path, step=6, intervals=None):
    path = tmp_path / 'metrics.txt'
    path.write_text('\n'.join(METRICS + ['karpenter_pods_state']))
    collector = Collector(step, str(path), intervals=intervals)
    collector.api = Mock(return_value=targets(collector.intervals))
    collector.targets(collector.intervals)
    del collector.api
    return collector


def samples(metric, times, labels=None):
    return [{'metric': {'__name__': metric, 'pod': 'p', 'job': 'kwok-controller' if metric.startswith('container_') else 'kube-state-metrics',
                         'instance': 'exporter', **(labels or {})},
             'values': [[timestamp, '1234.567890'] for timestamp in times]}]


def fake_queries(collector, labels=None):
    def query(metric, begin, end):
        source = metric.startswith('timestamp(')
        if source:
            metric = metric[len('timestamp('):-1]
        cursor = CollectionCursor(begin, collector.step)
        last = cursor.window(end)[2]
        results = [] if metric.startswith('karpenter') else samples(metric, [cursor.timestamp(i) for i in range(last + 1)], labels)
        if source:
            for result in results:
                result['values'] = [[ts, str(ts)] for ts, _ in result['values']]
        return results
    collector.request_metrics = Mock(side_effect=query)


def test_fractional_grid_and_raw_values_schema_union_deduplication(tmp_path):
    collector = make_collector(tmp_path, 3.75)
    output = tmp_path / 'csv'
    fake_queries(collector)
    collector.collect(1000.125, 1003.875, output)
    fake_queries(collector, {'zone': 'a'})
    collector.collect(1003.875, 1007.625, output)
    collector.collect(1003.875, 1007.625, output)
    path = output / 'container_cpu_usage_seconds_total.csv'
    rows = list(csv.DictReader(path.open()))
    # Newly introduced labels identify a different series even at the boundary.
    assert [float(row['timestamp']) for row in rows] == [1000.125, 1003.875, 1003.875, 1007.625]
    assert [row['zone'] for row in rows] == ['', '', 'a', 'a']
    for metric in METRICS:
        assert all(row['value'] == '1234.567890' for row in csv.DictReader((output / f'{metric}.csv').open()))
    assert not (output / 'karpenter_pods_state.csv').exists()


def test_multiple_batches_failure_retry_and_tail_have_no_gaps(tmp_path):
    collector = make_collector(tmp_path, .03)
    fake_queries(collector)
    cursor = CollectionCursor(1000.125, collector.step)
    windows = []
    while window := cursor.window(1000.425, max_points=3):
        begin, end, last = window
        if not windows:
            original = collector.request_metrics.side_effect
            collector.request_metrics.side_effect = CollectionError('HTTP failure')
            with pytest.raises(CollectionError):
                collector.collect(begin, end, tmp_path / 'csv')
            assert cursor.next_index == 0
            collector.request_metrics.side_effect = original
        collector.collect(begin, end, tmp_path / 'csv')
        cursor.commit(last)
        windows.append((begin, end))
    rows = list(csv.DictReader((tmp_path / 'csv/kube_pod_owner.csv').open()))
    assert [float(row['timestamp']) for row in rows] == pytest.approx([1000.125 + i * .03 for i in range(11)])
    assert len(windows) == 4


@pytest.mark.parametrize('failure', ['missing', 'incomplete', 'prometheus', 'json', 'http', 'transport'])
def test_errors_are_exposed_without_committing_batch(tmp_path, monkeypatch, failure):
    collector = make_collector(tmp_path)
    fake_queries(collector)
    original = collector.request_metrics.side_effect
    if failure in ('missing', 'incomplete'):
        def query(metric, begin, end):
            if metric == 'kube_pod_owner':
                return [] if failure == 'missing' else samples(metric, [begin])
            return original(metric, begin, end)
        collector.request_metrics.side_effect = query
    else:
        del collector.request_metrics
        response = Mock()
        response.json.return_value = {'status': 'error', 'error': 'bad query'}
        if failure == 'json':
            response.json.side_effect = ValueError('invalid JSON')
        if failure == 'http':
            response.raise_for_status.side_effect = requests.HTTPError('503')
        get = Mock(return_value=response)
        if failure == 'transport':
            get.side_effect = requests.ConnectionError('disconnected')
        monkeypatch.setattr('collector.requests.get', get)
    with pytest.raises(CollectionError):
        collector.collect(1000.125, 1006.125, tmp_path / 'csv')
    assert len(collector.errors) == 1
    assert not (tmp_path / 'csv').exists()


def test_query_preserves_fractional_parameters_and_real_timeout(tmp_path, monkeypatch):
    collector = make_collector(tmp_path, 3.75)
    get = Mock()
    get.return_value.json.return_value = {'status': 'success', 'data': {'resultType': 'matrix', 'result': []}}
    monkeypatch.setattr('collector.requests.get', get)
    collector.request_metrics('metric', 123.125, 126.875)
    assert get.call_args.kwargs == dict(params=dict(query='metric', start='123.125', end='126.875', step='3.75'), timeout=10)


def targets(intervals, last=1000):
    stamp = datetime.fromtimestamp(last, timezone.utc).isoformat()
    return {'activeTargets': [dict(scrapeUrl='http://kwok/metrics/nodes/n/metrics/resource', scrapePool='serviceMonitor/monitoring/n/0',
                labels=dict(job='kwok-controller', instance='exporter'),
                health='up', scrapeInterval=duration(intervals.kwok), scrapeTimeout=duration(min(10, intervals.kwok)), lastScrape=stamp),
            dict(scrapeUrl='https://ksm/metrics', scrapePool='serviceMonitor/monitoring/kube-state-metrics/0', health='up',
                 labels=dict(job='kube-state-metrics', instance='exporter'),
                 scrapeInterval=duration(intervals.ksm), scrapeTimeout=duration(min(10, intervals.ksm)), lastScrape=stamp)]}


def test_readiness_requires_loaded_configuration_and_required_metrics(tmp_path):
    collector = make_collector(tmp_path)
    intervals = SamplingIntervals(5)
    wrong = targets(SamplingIntervals(1))
    collector._source_policies.clear()
    states = [wrong, targets(intervals)]
    now = [0]
    def api(endpoint, params):
        if endpoint == 'targets':
            return states.pop(0)
        return {'result': [{'metric': samples(params['query'][10:-1], [1000])[0]['metric'], 'value': [1000, '1000']}]}
    collector.api = api
    def wait(seconds):
        now[0] += seconds
    effective = collector.wait_ready(intervals, ['n'], timeout=3, monotonic=lambda: now[0], wait=wait, unix_time=lambda: 1000)
    assert effective == dict(kwok=3, kube_state_metrics=6)
    assert now == [1]
    collector.api = lambda *args: wrong
    with pytest.raises(CollectionError, match='timed out'):
        collector.wait_ready(intervals, ['n'], timeout=0)


def make_coordinator(tmp_path, monkeypatch, end, fail_once=False, confirmed=2000):
    from manager import Manager
    import manager as module
    result = Manager.__new__(Manager)
    result.output_dir = str(tmp_path / 'output_csv_test')
    result.intervals = SamplingIntervals(5)
    result.collector = make_collector(tmp_path, intervals=result.intervals)
    fake_queries(result.collector)
    now = [0]
    monkeypatch.setattr(module.time, 'monotonic', lambda: now[0])
    monkeypatch.setattr(module.time, 'sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    monkeypatch.chdir(tmp_path)
    result.collection_stop = Mock()
    result.collection_stop.wait.return_value = True
    result.collection_end = end
    result.collection_diagnostics = dict(complete=False, errors=[])
    result.collection_error = None
    calls = [0]
    def target(*args):
        calls[0] += 1
        if fail_once and calls[0] == 1:
            raise CollectionError('not ingested')
        return confirmed
    result.collector.targets = target
    result.scrape_watermark = target
    return result


def test_short_run_stop_flushes_origin_without_waiting_collection_cycle(tmp_path, monkeypatch):
    result = make_coordinator(tmp_path, monkeypatch, end=1001, fail_once=True)
    result.collect_loop(1000.125)
    assert result.collection_error is None
    assert result.collection_diagnostics['complete']
    assert result.collection_cursor.next_index == 1
    result.collection_stop.wait.assert_any_call(60)
    rows = list(csv.DictReader(next(tmp_path.glob('output_csv_*/*.csv')).open()))
    assert len(rows) == 1 and rows[0]['timestamp'] == '1000.125'
    # Collection leaves archiving to the coordinator after report finalization.
    assert not list(tmp_path.glob('*.zip'))


def test_final_ingestion_timeout_exposes_incomplete_sampling(tmp_path, monkeypatch):
    result = make_coordinator(tmp_path, monkeypatch, end=1010, confirmed=1000.125)
    result.collect_loop(1000.125)
    assert isinstance(result.collection_error, CollectionError)
    assert not result.collection_diagnostics['complete']
    assert result.collection_cursor.next_index == 1


def test_lookback_cannot_hide_old_scrapes(tmp_path):
    collector = make_collector(tmp_path)
    fake_queries(collector)
    query = collector.request_metrics.side_effect
    def old(metric, begin, end):
        result = query(metric, begin, end)
        if metric.startswith('timestamp(container_cpu'):
            for series in result:
                series['values'] = [[ts, str(ts - 31)] for ts, _ in series['values']]
        return result
    collector.request_metrics.side_effect = old
    with pytest.raises(CollectionError, match='stale scrape'):
        collector.collect(1000.125, 1006.125, tmp_path / 'csv')
    assert not (tmp_path / 'csv').exists()


def test_readiness_cannot_accept_missing_or_old_mandatory_metrics(tmp_path):
    collector = make_collector(tmp_path)
    intervals = SamplingIntervals(5)
    collector._source_policies.clear()
    def api(endpoint, params):
        if endpoint == 'targets':
            return targets(intervals)
        return {'result': [{'metric': samples(params['query'][10:-1], [1000])[0]['metric'], 'value': [1000, '800']}]}
    collector.api = api
    with pytest.raises(CollectionError, match='stale scrape'):
        collector.wait_ready(intervals, timeout=0, unix_time=lambda: 1000)


def test_http_requests_obey_remaining_real_flush_budget(tmp_path, monkeypatch):
    collector = make_collector(tmp_path)
    monkeypatch.setattr('collector.time.monotonic', lambda: 100)
    get = Mock()
    get.return_value.json.return_value = {'status': 'success', 'data': {}}
    monkeypatch.setattr('collector.requests.get', get)
    collector.request_deadline = 102.5
    collector.api('targets')
    assert get.call_args.kwargs['timeout'] == 2.5
    collector.request_deadline = 99
    with pytest.raises(CollectionError, match='deadline'):
        collector.api('targets')
    assert get.call_count == 1


def test_periodic_batch_and_final_tail_share_the_same_grid(tmp_path, monkeypatch):
    result = make_coordinator(tmp_path, monkeypatch, end=1020)
    monkeypatch.setattr('manager.time.time', lambda: 1008)
    result.collection_stop.wait.side_effect = [False, True]
    result.collect_loop(1000.125)
    assert result.collection_diagnostics['complete']
    rows = list(csv.DictReader(next(tmp_path.glob('output_csv_*/*.csv')).open()))
    assert [float(row['timestamp']) for row in rows] == [1000.125, 1006.125, 1012.125, 1018.125]


@pytest.mark.parametrize('step', [0, -1, float('nan'), float('inf'), .0001, 1/7])
def test_invalid_query_resolution_rejected_before_http(step):
    with pytest.raises(ValueError):
        Collector(step)
