from types import SimpleNamespace as NS
from unittest.mock import Mock

import pytest

from collector import Collector, CollectionError
from manager import Manager
from sampling import SamplingIntervals
from test_collection_windows import make_coordinator, targets


def coordinator_with_nodes(live, actions=()):
    manager = Manager.__new__(Manager)
    manager.initial_nodes = ['a', 'b']
    manager.intervals = SamplingIntervals(1)
    manager.collection_diagnostics = {}
    manager.infrastructure_manager = NS(karpenter=False,
        replay_diagnostics=Mock(snapshot=lambda: {'actions': list(actions)}))
    manager.collector = Mock()
    manager.collector.targets.return_value = 1000
    manager.service_monitor_manager = NS(k8s_api=NS(v1=Mock()), _is_fake_node=lambda node: node.fake)
    manager.service_monitor_manager.k8s_api.v1.list_node.return_value = NS(
        items=[NS(metadata=NS(name=name), fake=True) for name in live] +
              [NS(metadata=NS(name='minikube'), fake=False)])
    return manager


def action(name, kind='delete-node', completed=True):
    return dict(completed=completed, context=dict(action=kind, name=name))


def test_completed_node_deletions_allow_empty_kwok():
    manager = coordinator_with_nodes([], [action('a'), action('b')])
    assert manager.scrape_watermark() == 1000
    manager.collector.targets.assert_called_once_with(manager.intervals, nodes=[], allow_empty_kwok=True)
    assert manager.collection_diagnostics['kwok_targets_not_required']


def test_partial_removal_still_requires_remaining_node_target():
    manager = coordinator_with_nodes(['b'], [action('a')])
    manager.scrape_watermark()
    manager.collector.targets.assert_called_once_with(manager.intervals, nodes=['b'], allow_empty_kwok=False)


def test_api_failure_cannot_be_mistaken_for_zero_nodes():
    manager = coordinator_with_nodes([], [action('a'), action('b')])
    manager.service_monitor_manager.k8s_api.v1.list_node.side_effect = RuntimeError('API unavailable')
    with pytest.raises(CollectionError, match='could not confirm'):
        manager.scrape_watermark()
    manager.collector.targets.assert_not_called()


@pytest.mark.parametrize('actions', [[], [action('a'), action('b', completed=False)],
    [action('a'), action('b'), action('b', kind='apply-node')]])
def test_missing_nodes_are_not_silently_accepted(actions):
    manager = coordinator_with_nodes([], actions)
    with pytest.raises(CollectionError, match='disappeared'):
        manager.scrape_watermark()
    manager.collector.targets.assert_not_called()


def test_empty_kwok_still_requires_healthy_ksm_and_normal_readiness_is_strict():
    intervals = SamplingIntervals(1)
    collector = Collector(30)
    data = targets(intervals)
    data['activeTargets'] = data['activeTargets'][1:]  # KSM only.
    collector.api = Mock(return_value=data)
    assert collector.targets(intervals, allow_empty_kwok=True) == 1000
    with pytest.raises(CollectionError, match='not active'):
        collector.targets(intervals)
    data['activeTargets'][0]['health'] = 'down'
    with pytest.raises(CollectionError, match='not ready'):
        collector.targets(intervals, allow_empty_kwok=True)
    data['activeTargets'] = []
    with pytest.raises(CollectionError, match='not active'):
        collector.targets(intervals, allow_empty_kwok=True)


def test_stale_removed_target_does_not_block_remaining_live_target():
    intervals = SamplingIntervals(1)
    collector = Collector(30)
    data = targets(intervals)
    removed = dict(data['activeTargets'][0], scrapeUrl='http://kwok/metrics/nodes/gone/metrics/resource',
                   health='down', lastScrape='2020-01-01T00:00:00Z')
    data['activeTargets'].append(removed)
    collector.api = Mock(return_value=data)
    assert collector.targets(intervals, nodes=['n']) == 1000
    with pytest.raises(CollectionError, match='not active'):
        collector.targets(intervals, nodes=['missing'])


def test_flush_exports_historical_grid_after_all_nodes_removed(tmp_path, monkeypatch):
    manager = make_coordinator(tmp_path, monkeypatch, end=1020)
    real = coordinator_with_nodes([], [action('a'), action('b')])
    manager.initial_nodes = real.initial_nodes
    manager.infrastructure_manager = real.infrastructure_manager
    manager.service_monitor_manager = real.service_monitor_manager
    manager.scrape_watermark = Manager.scrape_watermark.__get__(manager)
    manager.collector.targets = Collector.targets.__get__(manager.collector)
    data = targets(manager.intervals, last=2000)
    data['activeTargets'] = data['activeTargets'][1:]
    manager.collector.api = Mock(return_value=data)
    manager.collect_loop(1000.125)
    assert manager.collection_error is None
    assert manager.collection_diagnostics['complete']
    assert manager.collection_diagnostics['kwok_targets_not_required']
    assert manager.collection_cursor.next_index == 4
    assert len(list((tmp_path / 'output_csv_test').glob('*.csv'))) > 0
