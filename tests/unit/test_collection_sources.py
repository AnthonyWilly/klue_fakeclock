"""Freshness follows effective scrape origins in readiness and historical export."""
import copy
import csv
from unittest.mock import Mock

import pytest

from collector import CollectionCursor, CollectionError
from sampling import SamplingIntervals
from test_collection_windows import METRICS, make_collector, samples, targets


KUBELET = dict(job='kubelet', instance='real-node:10250', endpoint='https-metrics',
               service='kubelet', metrics_path='/metrics/cadvisor')


def setup_sources(tmp_path, factor=2, timeout='10s'):
    intervals = SamplingIntervals(factor)
    collector = make_collector(tmp_path, intervals.query, intervals)
    data = targets(intervals)
    data['activeTargets'].append(dict(labels=KUBELET, scrapeUrl='https://real-node/metrics/cadvisor',
        scrapePool='kubelet', scrapeInterval='30s', scrapeTimeout=timeout, lastScrape='1970-01-01T00:00:01Z'))
    collector.api = Mock(return_value=data)
    # Kubelet is catalogued, but cannot reduce the KWOK/KSM watermark.
    assert collector.targets(intervals) == 1000
    return collector, data


def vectors(metric, time=1000, kubelet_age=16.942, kwok_age=0):
    result = samples(metric, [time])
    if metric.startswith('container_'):
        result += samples(metric, [time], KUBELET)
    for series in result:
        age = kubelet_age if series['metric']['job'] == 'kubelet' else kwok_age
        series['values'] = [[time, str(time - age)]]
    return result


def queries(collector, kubelet_age=16.942, kwok_age=0):
    def query(metric, start, end):
        source = metric.startswith('timestamp(')
        name = metric[10:-1] if source else metric
        if name not in METRICS:
            return []
        results = vectors(name, start, kubelet_age, kwok_age)
        if not source:
            for series in results:
                series['values'] = [[start, '1234.567890']]
        return results
    collector.request_metrics = query


@pytest.mark.parametrize('factor', [1, 2, 5])
def test_mixed_origins_readiness_and_atomic_export(tmp_path, factor):
    collector, data = setup_sources(tmp_path, factor)
    def api(endpoint, params):
        if endpoint == 'targets':
            return data
        return {'result': [dict(metric=s['metric'], value=s['values'][0])
                           for s in vectors(params['query'][10:-1])]}
    collector.api = api
    assert collector.wait_ready(collector.intervals, timeout=0, unix_time=lambda: 1000) == {
        'kwok': 15 / factor, 'kube_state_metrics': 30 / factor}
    queries(collector)
    output = tmp_path / 'csv'
    cursor = CollectionCursor(1000, collector.step)
    collector.collect(1000, 1000, output)
    cursor.commit(0)
    for metric in METRICS:
        rows = list(csv.DictReader((output / f'{metric}.csv').open()))
        assert len(rows) == (2 if metric.startswith('container_') else 1)
        assert all(row['value'] == '1234.567890' for row in rows)
    assert {r['job'] for r in csv.DictReader((output / f'{METRICS[0]}.csv').open())} == {'kwok-controller', 'kubelet'}


@pytest.mark.parametrize('factor,age', [(2, 16.942), (5, 6.002)])
def test_stale_kwok_blocks_readiness_and_batch_without_advancing_cursor(tmp_path, factor, age):
    collector, data = setup_sources(tmp_path, factor)
    collector.api = lambda endpoint, params: data if endpoint == 'targets' else {
        'result': [dict(metric=s['metric'], value=s['values'][0])
                   for s in vectors(params['query'][10:-1], kwok_age=age)]}
    with pytest.raises(CollectionError, match='kwok-controller.*max_age='):
        collector.wait_ready(collector.intervals, timeout=0, unix_time=lambda: 1000)
    queries(collector, kwok_age=age)
    cursor = CollectionCursor(1000, collector.step)
    output = tmp_path / 'csv'
    with pytest.raises(CollectionError, match='stale scrape'):
        collector.collect(1000, 1000, output)
    assert cursor.next_index == 0 and not output.exists() and not collector._last_written
    queries(collector)
    collector.collect(1000, 1000, output)
    collector.collect(1000, 1000, output)
    cursor.commit(0)
    assert len(list(csv.DictReader((output / 'container_cpu_usage_seconds_total.csv').open()))) == 2


@pytest.mark.parametrize('timeout,age,valid', [
    ('10s', 40, True), ('10s', 40.001, True), ('10s', 40.002, False),
    ('4s', 34.001, True), ('4s', 34.002, False),
    ('10s', float('nan'), False), ('10s', float('inf'), False), ('10s', None, False)])
def test_effective_timeout_and_numeric_boundary(tmp_path, timeout, age, valid):
    collector, _ = setup_sources(tmp_path, 5, timeout)
    source = None if age is None else 1000 - age
    if valid:
        collector.validate_source_age('container_cpu_usage_seconds_total',
                                      *collector.source_policy(KUBELET), 1000, source)
    else:
        with pytest.raises(CollectionError, match='max_age='):
            collector.validate_source_age('container_cpu_usage_seconds_total',
                                      *collector.source_policy(KUBELET), 1000, source)


def test_identity_discriminators_equivalent_targets_and_conflicts(tmp_path):
    collector, data = setup_sources(tmp_path)
    original = data['activeTargets'][-1]
    # Workload namespace, pod and container need not match target labels.
    original['labels'] = dict(KUBELET, namespace='monitoring', pod='exporter')
    other = copy.deepcopy(original)
    other['labels']['endpoint'] = 'other'
    other.update(scrapeInterval='12s', scrapeTimeout='2s')
    instance = copy.deepcopy(other)
    instance['labels']['instance'] = 'other-node'
    data['activeTargets'] += [copy.deepcopy(original), other, instance]
    collector.targets(collector.intervals)
    assert collector.source_policy(dict(KUBELET, pod='workload', namespace='default', container='c'))[1] == (30, 10)
    assert collector.source_policy(dict(KUBELET, endpoint='other'))[1] == (12, 2)
    assert collector.source_policy(instance['labels'])[1] == (12, 2)
    with pytest.raises(CollectionError, match='ambiguous'):
        collector.source_policy(dict(job='kubelet', instance=KUBELET['instance']))
    conflict = copy.deepcopy(original)
    conflict['scrapeTimeout'] = '5s'
    data['activeTargets'].append(conflict)
    collector.targets(collector.intervals)
    with pytest.raises(CollectionError, match='ambiguous'):
        collector.source_policy(KUBELET)


@pytest.mark.parametrize('timeout', ['bad', '0s', '31s', None])
def test_invalid_origin_configuration_is_explicit(tmp_path, timeout):
    collector, _ = setup_sources(tmp_path, timeout=timeout)
    with pytest.raises(CollectionError, match='invalid scrape configuration.*kubelet'):
        collector.source_policy(KUBELET)


def test_unknown_origins_missing_timestamps_and_full_label_join(tmp_path):
    collector, _ = setup_sources(tmp_path)
    with pytest.raises(CollectionError, match='unresolved'):
        collector.source_policy(dict(KUBELET, instance='unknown'))
    with pytest.raises(CollectionError, match='unresolved'):
        collector.source_policy({})
    result = samples('optional', [1000], KUBELET)
    for sources in ([], samples('optional', [1000], dict(KUBELET, pod='different'))):
        collector.request_metrics = Mock(return_value=sources)
        with pytest.raises(CollectionError, match='missing or stale'):
            collector.validate_source_samples('optional', result, 1000, 1000)
    # __name__ is deliberately excluded from the timestamp join.
    source = copy.deepcopy(result)
    source[0]['metric']['__name__'] = 'timestamp'
    source[0]['values'] = [[1000.0004, '990']]
    collector.request_metrics = Mock(return_value=source)
    collector.validate_source_samples('optional', result, 1000, 1000)


def test_optional_present_obeys_origin_and_freshness(tmp_path):
    collector, _ = setup_sources(tmp_path)
    queries(collector)
    original = collector.request_metrics
    for labels, age, error in [(dict(KUBELET, instance='unknown'), 0, 'unresolved'),
                                (KUBELET, 40.002, 'stale scrape')]:
        def query(metric, start, end):
            if 'karpenter' in metric:
                result = samples('karpenter_pods_state', [start], labels)
                if metric.startswith('timestamp('):
                    result[0]['values'] = [[start, str(start - age)]]
                return result
            return original(metric, start, end)
        collector.request_metrics = query
        with pytest.raises(CollectionError, match=error):
            collector.collect(1000, 1000, tmp_path / 'csv')
        assert not (tmp_path / 'csv').exists()


def test_catalog_retains_removed_targets_for_historical_export(tmp_path):
    collector, data = setup_sources(tmp_path, 5)
    data['activeTargets'] = data['activeTargets'][1:2]
    assert collector.targets(collector.intervals, allow_empty_kwok=True) == 1000
    queries(collector)
    collector.collect(1000, 1000, tmp_path / 'csv')
    assert (tmp_path / 'csv/container_cpu_usage_seconds_total.csv').exists()
    with pytest.raises(CollectionError, match='not active'):
        collector.targets(collector.intervals)


@pytest.mark.parametrize('source', [None, 'NaN', '+Inf', '959.998'])
def test_readiness_rejects_missing_nonfinite_and_stale_kubelet(tmp_path, source):
    collector, data = setup_sources(tmp_path, 5)
    def api(endpoint, params):
        if endpoint == 'targets':
            return data
        result = [dict(metric=s['metric'], value=s['values'][0])
                  for s in vectors(params['query'][10:-1])]
        for item in result:
            if item['metric']['job'] == 'kubelet':
                item['value'] = None if source is None else [1000, source]
        return {'result': result}
    collector.api = api
    with pytest.raises(CollectionError, match='kubelet.*max_age=40'):
        collector.wait_ready(collector.intervals, timeout=0, unix_time=lambda: 1000)


def test_equivalent_kwok_node_targets_and_real_timeout(tmp_path):
    collector, data = setup_sources(tmp_path)
    duplicate = dict(data['activeTargets'][0], scrapeUrl='http://kwok/metrics/nodes/other/metrics/resource')
    data['activeTargets'].append(duplicate)
    collector.targets(collector.intervals, nodes=['n', 'other'])
    assert collector.source_policy(dict(job='kwok-controller', instance='exporter'))[1] == (7.5, 7.5)
    # A smaller configured KWOK timeout must not be replaced with min(10, interval).
    for target in data['activeTargets']:
        if target['labels']['job'] == 'kwok-controller':
            target['scrapeTimeout'] = '1s'
    collector._source_policies.clear()
    collector.targets(collector.intervals)
    assert collector.source_policy(dict(job='kwok-controller', instance='exporter'))[1] == (7.5, 1)
    queries(collector, kwok_age=8.502)
    with pytest.raises(CollectionError, match='max_age=8.5'):
        collector.collect(1000, 1000, tmp_path / 'csv')
