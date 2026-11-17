from pathlib import Path
from types import SimpleNamespace as NS
from unittest.mock import Mock

import pandas as pd
import pytest

from pods_mapping import PodsMapping


def pod(namespace, name, owner):
    return NS(metadata=NS(namespace=namespace, name=name, labels={'deployment': owner}))


def node(name, instance='small', pool=None, fake=True):
    return NS(metadata=NS(name=name, labels={
        'node.kubernetes.io/instance-type': instance, 'karpenter.sh/nodepool': pool},
        annotations={'kwok.x-k8s.io/node': 'fake'} if fake else None))


@pytest.fixture
def mapping(tmp_path):
    mapper = PodsMapping.__new__(PodsMapping)
    mapper.karpenter = False
    mapper.k8s_api = Mock()
    mapper.k8s_api.list_node.return_value.items = [node('fake-b'), node('real', fake=False), node('fake-a')]
    mapper.k8s_api.list_pod_for_all_namespaces.return_value.items = [
        pod('a', 'p2', 'owner'), pod('b', 'p1', 'owner'), pod('a', 'p1', 'owner'),
        pod('other', 'unrelated', 'owner')]
    mapper.PODS_ALLOCATION_PATH = str(tmp_path / 'allocation.csv')
    mapper.PODS_AND_NODES_PATH = str(tmp_path / 'mapping.csv')
    frame = pd.DataFrame([
        dict(namespace='a', replicaset='owner', node='original-a', instance_type='small', pods_count=1),
        dict(namespace='a', replicaset='owner', node='original-b', instance_type='small', pods_count=1),
        dict(namespace='b', replicaset='owner', node='original-b', instance_type='small', pods_count=1)])
    frame.to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    return mapper, frame


def test_namespace_coverage_and_stable_destinations(mapping):
    mapper, frame = mapping
    expected = [dict(pod='p1', namespace='a', node='fake-a'),
                dict(pod='p2', namespace='a', node='fake-b'),
                dict(pod='p1', namespace='b', node='fake-b')]
    assert mapper.run().to_dict('records') == expected
    # List/watch completion order must not change pod or node assignment.
    frame.to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    mapper.k8s_api.list_node.return_value.items.reverse()
    mapper.k8s_api.list_pod_for_all_namespaces.return_value.items.reverse()
    assert mapper.run().to_dict('records') == expected
    assert pd.read_csv(mapper.PODS_AND_NODES_PATH).to_dict('records') == expected


@pytest.mark.parametrize('case', ['missing', 'excess', 'wrong-namespace', 'wrong-owner', 'duplicate'])
def test_incomplete_mapping_never_published(mapping, case):
    mapper, frame = mapping
    pods = mapper.k8s_api.list_pod_for_all_namespaces.return_value.items
    if case == 'missing':
        pods.pop(0)
    elif case == 'excess':
        pods.append(pod('a', 'extra', 'owner'))
    elif case == 'wrong-namespace':
        pods[0].metadata.namespace = 'other'
    elif case == 'wrong-owner':
        pods[0].metadata.labels['deployment'] = 'other'
    else:
        pods[0].metadata.name = 'p1'
    with pytest.raises(ValueError, match='Pod coverage'):
        mapper.run()
    assert not Path(mapper.PODS_AND_NODES_PATH).exists()
    pd.testing.assert_frame_equal(pd.read_csv(mapper.PODS_ALLOCATION_PATH), frame)


@pytest.mark.parametrize('case', ['missing-node', 'wrong-type', 'conflicting-type', 'negative', 'fractional', 'empty', 'no-column'])
def test_invalid_allocation_or_destination(mapping, case):
    mapper, frame = mapping
    if case == 'missing-node':
        mapper.k8s_api.list_node.return_value.items = [node('fake-a')]
    elif case == 'wrong-type':
        frame.loc[0, 'instance_type'] = 'large'
    elif case == 'conflicting-type':
        frame.loc[2, 'instance_type'] = 'large'
    elif case == 'negative':
        frame.loc[0, 'pods_count'] = -1
    elif case == 'fractional':
        frame['pods_count'] = frame['pods_count'].astype(float)
        frame.loc[0, 'pods_count'] = 1.5
    elif case == 'empty':
        frame.loc[0, 'namespace'] = None
    else:
        frame = frame.drop(columns='node')
    frame.to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    with pytest.raises(ValueError):
        mapper.run()


def test_nodepool_preserved(mapping):
    mapper, frame = mapping
    mapper.karpenter = True
    frame['nodepool'] = ['pool-a', 'pool-b', 'pool-b']
    frame.to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    mapper.k8s_api.list_node.return_value.items = [node('fake-z', pool='pool-a'), node('fake-a', pool='pool-b')]
    assert mapper.run()['node'].tolist() == ['fake-z', 'fake-a', 'fake-a']


def test_empty_allocation(mapping):
    mapper, frame = mapping
    frame.iloc[:0].to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    result = mapper.run()
    assert result.empty
    assert list(pd.read_csv(mapper.PODS_AND_NODES_PATH).columns) == ['pod', 'namespace', 'node']


@pytest.mark.parametrize('namespace', ['123', 'null'])
def test_namespace_is_read_as_literal_identity(mapping, namespace):
    mapper, frame = mapping
    frame['namespace'] = namespace
    frame.to_csv(mapper.PODS_ALLOCATION_PATH, index=False)
    mapper.k8s_api.list_pod_for_all_namespaces.return_value.items = [
        pod(namespace, f'p{i}', 'owner') for i in range(3)]
    assert mapper.run()['namespace'].tolist() == [namespace] * 3
