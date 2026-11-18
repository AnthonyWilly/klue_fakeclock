"""A workload observed once still needs a create event and initial placement."""
import pandas as pd
import pytest

from tracer.tracer_kwok import TracerKWOKOnly
from util.k8s_object_generator import K8SObjectGenerator


def row(timestamp, name='transient', namespace='ns'):
    return dict(timestamp=timestamp, namespace=namespace, replicaset=name,
                owner_kind='Deployment', cpu_request=.01, memory_request=100,
                cpu_limit=1., memory_limit=1000)


@pytest.mark.parametrize('timestamp', [0, 30, 60])
def test_single_sample_is_created_before_deletion(timestamp):
    tracer = TracerKWOKOnly.__new__(TracerKWOKOnly)
    tracer.step = 30
    tracer.df_final = pd.DataFrame([
        row(0, 'persistent'), row(30, 'persistent'), row(60, 'persistent'),
        row(timestamp), row(0, namespace='other'), row(30, namespace='other')])
    tracer.process_and_save_final_trace()
    transient = tracer.df_final.query("namespace == 'ns' and replicaset == 'transient'")
    expected = [(timestamp, 'create')]
    if timestamp < 60:
        expected.append((timestamp + 30, 'delete'))
    assert list(zip(transient.timestamp, transient.action)) == expected
    assert list(tracer.df_final.query("replicaset == 'persistent'").action) == ['create', 'delete']
    assert list(tracer.df_final.query("namespace == 'other'").action) == ['create', 'delete']

    tracer.k8s_objects_generator = K8SObjectGenerator(karpenter=False)
    tracer.df_container_usage = pd.DataFrame(columns=['timestamp'])
    tracer.df_hpa_trace = pd.DataFrame(columns=['timestamp'])
    workload = tracer.generate_workload_objects()
    initial_names = [obj['metadata']['name']
                     for obj in workload['setup']['applied_objects'].get('ns', [])]
    assert ('transient' in initial_names) == (timestamp == 0)
    if timestamp < 60:
        deletion = next(entry for entry in workload['emulation']
                        if entry['timestamp'] == timestamp + 30)
        assert dict(name='transient', namespace='ns', kind='Deployment') in deletion['deleted_objects']


@pytest.mark.parametrize('timestamp', [0, 30, 60])
def test_single_sample_node_is_kept_in_infrastructure_and_allocation(timestamp, monkeypatch):
    tracer = TracerKWOKOnly.__new__(TracerKWOKOnly)
    tracer.step = 30
    tracer.df_final = pd.DataFrame([
        dict(row(t, 'persistent'), node='stable', instance_type='small') for t in [0, 30, 60]
    ] + [dict(row(timestamp), node='transient', instance_type='small')])
    # These generation methods normally write to /tmp. Inspect their frames only.
    monkeypatch.setattr(pd.DataFrame, 'to_csv', lambda *args, **kwargs: None)
    tracer.process_and_save_pods_allocation()
    allocated = set(tracer.df_pods_allocation.node)
    assert ('transient' in allocated) == (timestamp == 0)
    tracer.process_and_save_infrastructure_objects()
    events = tracer.df_infrastructure.query("node == 'transient'")
    expected = [(timestamp, 'create')]
    if timestamp < 60:
        expected.append((timestamp + 30, 'delete'))
    assert list(zip(events.timestamp, events.action)) == expected
    assert list(tracer.df_infrastructure.query("node == 'stable'").action) == ['create', 'delete']


@pytest.mark.parametrize('types,expected', [(['small'], 'small'), (['small', 'large'], 'unallocated'), ([], 'unallocated')])
def test_resource_node_recovers_missing_cpu_metadata_only_with_unambiguous_type(types, expected):
    tracer = TracerKWOKOnly.__new__(TracerKWOKOnly)
    resources = [dict(timestamp=0, namespace='ns', pod='missing', node='real-node',
                      resource=resource, value=value)
                 for resource, value in [('cpu', .01), ('memory', 100)]]
    tracer.kube_pod_container_resource_requests = pd.DataFrame(resources)
    tracer.kube_pod_container_resource_limits = pd.DataFrame(columns=resources[0].keys())
    tracer.karpenter_pods_state = pd.DataFrame([
        dict(timestamp=0, namespace='other', pod=f'known-{i}', node='real-node',
             instance_type=instance, phase='Running') for i, instance in enumerate(types)
    ], columns=['timestamp', 'namespace', 'pod', 'node', 'instance_type', 'phase'])
    tracer.merge_pods_state_with_resources()
    assert len(tracer.df_final) == 1
    result = tracer.df_final.iloc[0]
    assert result.node == 'real-node'
    assert result.instance_type == expected
