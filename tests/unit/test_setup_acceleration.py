"""Setup capacity and barriers, using fake APIs and real-deadline fake clocks."""
from copy import deepcopy
import csv
from types import SimpleNamespace as NS
import threading
from unittest.mock import Mock

import pytest
from kubernetes import client

from infrastructure.manager import InfrastructureManager
from workload.manager import WorkloadManager
from setup_execution import apply_independent, setup_workers
from util.k8s_api.k8s_api import K8SAPI


def obj(name='app', namespace='ns', replicas=1):
    return dict(kind='Deployment', metadata=dict(name=name, namespace=namespace),
                spec=dict(replicas=replicas))


def pod(name='p', owner='app', namespace='ns', node=None, ready=False, uid='uid'):
    return NS(metadata=NS(name=name, namespace=namespace, labels={'deployment': owner},
                          uid=uid, deletion_timestamp=None),
              spec=NS(node_name=node), status=NS(phase='Running' if ready else 'Pending',
              conditions=[NS(type='Ready', status='True' if ready else 'False')]))


def workload(pods=()):
    result = WorkloadManager.__new__(WorkloadManager)
    result.k8s_api = Mock()
    result.k8s_api.list_pod_for_all_namespaces.return_value.items = list(pods)
    result.expected_owners = {('ns', 'app'): 1}
    result.pods_before_setup = set()
    return result


@pytest.fixture
def fake_time(monkeypatch):
    now = [0]
    monkeypatch.setattr('workload.manager.time.monotonic', lambda: now[0])
    monkeypatch.setattr('workload.manager.time.sleep', lambda seconds: now.__setitem__(0, now[0] + seconds))
    return now


@pytest.mark.parametrize('factor,limit', [(1, 1), (1.5, 2), (2, 2), (5, 5), (8, 8), (5000, 8)])
def test_capacity_order_copies_and_equivalent_results(factor, limit):
    workers = setup_workers(factor)
    assert workers == limit
    objects = [obj(str(i)) for i in range(limit)]
    objects += [dict(deepcopy(entry), revision=2) for entry in objects]
    original = deepcopy(objects)
    lock = threading.Lock()
    barrier = threading.Barrier(limit)
    active, peak, order = [0], [0], {}
    def apply(entry):
        with lock:
            active[0] += 1
            peak[0] = max(peak[0], active[0])
        if 'revision' not in entry:
            barrier.wait(timeout=3)
        with lock:
            order.setdefault(entry['metadata']['name'], []).append(entry.get('revision', 1))
            active[0] -= 1
        entry['metadata']['mutated'] = True
    progress = []
    apply_independent(objects, apply, workers, progress.append)
    assert peak[0] == limit
    assert progress[-1] == len(objects)
    assert order == {str(i): [1, 2] for i in range(limit)}
    assert objects == original
    assert not any(t.name.startswith('setup') for t in threading.enumerate())


def test_all_parallel_failures_reach_caller_and_workers_join():
    def fail(entry):
        raise RuntimeError(entry['metadata']['name'])
    with pytest.raises(RuntimeError) as failure:
        apply_independent([obj('first'), obj('second')], fail, 2, Mock())
    assert 'first' in str(failure.value) and 'second' in str(failure.value)
    assert not any(t.name.startswith('setup') for t in threading.enumerate())


def test_failed_identity_does_not_apply_its_later_operations():
    apply = Mock(side_effect=[RuntimeError('bad'), None])
    with pytest.raises(RuntimeError, match='bad'):
        apply_independent([obj('a'), obj('a'), obj('b')], apply, 1, Mock())
    assert [call.args[0]['metadata']['name'] for call in apply.call_args_list] == ['a', 'b']


def test_namespaces_precede_objects_and_usage_is_submitted_in_trace_order():
    result = workload()
    result.data = {'setup': {'applied_objects': {'ns': [obj()], 'other': [obj('other', 'other')]},
        'workload_actions': [{'action': 'set-usage', 'name': 'first'}, {'action': 'set-usage', 'name': 'second'}]}}
    result.BEFORE_EMULATION_KINDS = {'horizontalpodautoscaler'}
    result.hpa = False
    result.setup_concurrency = 2
    result.usage_manager = Mock()
    result.wait_pods_created = Mock()
    namespaces = set()
    result.create_namespace_if_not_exists = namespaces.add
    applied = []
    result.k8s_object_applier = Mock()
    def apply(entry):
        assert entry['metadata']['namespace'] in namespaces
        applied.append(entry)
    result.k8s_object_applier.apply_object.side_effect = apply
    result.set_workload_usage = Mock()
    result.setup()
    assert len(applied) == 2
    assert [call.args[0]['name'] for call in result.set_workload_usage.call_args_list] == ['first', 'second']
    result.wait_pods_created.assert_called_once()


def test_setup_owner_counts_use_final_operation_per_resource():
    result = workload()
    result.data = {'setup': {'applied_objects': {'ns': [obj(replicas=2), obj(replicas=3)],
                                                'other': [obj(namespace='other', replicas=4)]}}}
    assert result.setup_owners() == {('ns', 'app'): 3, ('other', 'app'): 4}


def test_pending_pods_can_pass_creation_before_bind(fake_time):
    result = workload([pod(), pod('unrelated', owner='other'), pod('foreign', namespace='other')])
    result.wait_pods_created()
    assert result.created_pods == {('ns', 'p'): 'uid'}
    assert fake_time[0] == 0


@pytest.mark.parametrize('pods', [[], [pod(owner='wrong')], [pod(namespace='other')]])
def test_sufficient_global_counts_cannot_replace_expected_owner(pods, fake_time):
    result = workload(pods)
    result.CREATION_TIMEOUT = 3
    with pytest.raises(TimeoutError, match='app'):
        result.wait_pods_created()
    assert fake_time[0] == 3
    assert all(0 < call.kwargs['_request_timeout'] <= 3 for call in result.k8s_api.list_pod_for_all_namespaces.call_args_list)


def test_excess_and_old_pods_are_rejected(fake_time):
    result = workload([pod(), pod('extra')])
    with pytest.raises(RuntimeError, match='Excess'):
        result.wait_pods_created()
    result = workload([pod()])
    result.pods_before_setup = {('ns', 'p')}
    with pytest.raises(RuntimeError, match='Pre-existing'):
        result.wait_pods_created()


def mapping(tmp_path, rows=(('ns', 'p', 'node'),)):
    path = tmp_path / 'map.csv'
    with path.open('w', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(['namespace', 'pod', 'node'])
        writer.writerows(rows)
    return path


@pytest.mark.parametrize('state', ['pending', 'not-ready', 'missing', 'replaced', 'wrong-node', 'extra', 'ready'])
def test_operational_requires_same_pods_destination_and_ready(state, tmp_path, fake_time):
    p = pod(node='node', ready=True)
    result = workload([p])
    result.created_pods = {('ns', 'p'): 'uid'}
    result.SCHEDULING_TIMEOUT = 3
    if state == 'pending':
        p.spec.node_name = None
    elif state == 'not-ready':
        p.status.conditions[0].status = 'False'
    elif state == 'missing':
        result.k8s_api.list_pod_for_all_namespaces.return_value.items = []
    elif state == 'replaced':
        p.metadata.uid = 'new'
    elif state == 'wrong-node':
        p.spec.node_name = 'other'
    elif state == 'extra':
        result.k8s_api.list_pod_for_all_namespaces.return_value.items.append(pod('extra'))
    if state == 'ready':
        result.wait_pods_operational(mapping(tmp_path))
        assert fake_time[0] == 0
    else:
        with pytest.raises((TimeoutError, RuntimeError)):
            result.wait_pods_operational(mapping(tmp_path))
        assert fake_time[0] == (3 if state in {'pending', 'not-ready', 'missing'} else 0)


def test_operational_mapping_has_exact_coverage(tmp_path):
    result = workload()
    result.created_pods = {('ns', 'p'): 'uid'}
    with pytest.raises(RuntimeError, match='placement differs'):
        result.wait_pods_operational(mapping(tmp_path, []))


def fake_node(name, fake=True):
    return NS(metadata=NS(name=name, annotations={'kwok.x-k8s.io/node': 'fake'} if fake else {}, labels={}))


@pytest.mark.parametrize('observed,outcome', [
    ([fake_node('wanted'), fake_node('real1', False), fake_node('real2', False)], None),
    ([fake_node('real1', False), fake_node('other')], 'Unexpected fake'),
    ([fake_node('wanted'), fake_node('extra')], 'Unexpected fake'),
    ([fake_node('real1', False)], 'wanted'),
])
def test_nodes_use_identity_not_global_count(observed, outcome, fake_time):
    result = InfrastructureManager.__new__(InfrastructureManager)
    result.data = {'setup': [{'kind': 'Node', 'metadata': {'name': 'wanted'}}]}
    result.k8s_object_applier = Mock()
    result.k8s_api = Mock()
    result.k8s_api.list_node.return_value.items = observed
    result.SETUP_TIMEOUT = 3
    if outcome:
        with pytest.raises((TimeoutError, RuntimeError), match=outcome):
            result.setup()
    else:
        result.setup()
        assert result.node_count == 1


def test_clients_have_isolated_configuration_and_real_request_timeout(monkeypatch):
    monkeypatch.setattr('kubernetes.config.load_kube_config', Mock())
    default = client.Configuration._default
    first, second = K8SAPI(timeout=13), K8SAPI(timeout=27)
    assert first.v1.api_client.configuration is not second.v1.api_client.configuration
    assert client.Configuration._default is default
    call = Mock(side_effect=RuntimeError('request captured'))
    monkeypatch.setattr(client.ApiClient, 'call_api', call)
    with pytest.raises(RuntimeError, match="request captured"):
        first.list_node()
    assert call.call_args.kwargs['_request_timeout'] == 13
    with pytest.raises(RuntimeError, match="request captured"):
        second.list_pod_for_all_namespaces(_request_timeout=3)
    assert call.call_args.kwargs['_request_timeout'] == 3
    with pytest.raises(RuntimeError, match='request captured'):
        second.list_namespaced_pod('ns')
    assert call.call_args.kwargs['_request_timeout'] == 27



def test_executor_joins_on_coordinator_interruption():
    progress = Mock(side_effect=KeyboardInterrupt('stop'))
    with pytest.raises(KeyboardInterrupt, match='stop'):
        apply_independent([obj(str(i)) for i in range(30)], Mock(), 2, progress)
    assert not any(t.name.startswith('setup') for t in threading.enumerate())


def test_nodeclaims_resolve_expected_fake_node_identity(fake_time):
    result = InfrastructureManager.__new__(InfrastructureManager)
    result.data = {'setup': [{'kind': 'NodeClaim', 'metadata': {'name': 'claim'}}]}
    result.k8s_object_applier = Mock()
    result.k8s_api = Mock()
    result.k8s_api.list_node.return_value.items = [fake_node('generated-node'), fake_node('real', False)]
    result.k8s_api.get_cluster_custom_object.return_value = {'status': {'nodeName': 'generated-node'}}
    result.setup()
    assert result.node_count == 1
    result.k8s_api.get_cluster_custom_object.assert_called_once_with(
        'karpenter.sh', 'v1', 'nodeclaims', 'claim', _request_timeout=10)



def test_nodeclaim_status_can_converge_after_node_appears(fake_time):
    result = InfrastructureManager.__new__(InfrastructureManager)
    result.data = {'setup': [{'kind': 'NodeClaim', 'metadata': {'name': 'claim'}}]}
    result.k8s_object_applier = Mock()
    result.k8s_api = Mock()
    result.k8s_api.list_node.return_value.items = [fake_node('generated-node')]
    result.k8s_api.get_cluster_custom_object.side_effect = [{}, {'status': {'nodeName': 'generated-node'}}]
    result.setup()
    assert fake_time[0] == 2
    assert result.node_count == 1
