"""Build a complete, deterministic initial pod placement from the allocation."""
import os
import tempfile

import pandas as pd
from util.k8s_api.k8s_api import K8SAPI


class PodsMapping:
    PODS_ALLOCATION_PATH = '/tmp/pods_allocation.csv'
    PODS_AND_NODES_PATH = '/tmp/pods_and_nodes_map.csv'

    def __init__(self, karpenter):
        self.k8s_api = K8SAPI()
        self.karpenter = karpenter

    def log(self, message):
        print(f'[PODS MAPPING] {message}')

    def get_fake_nodes_dict(self):
        node_data = {}
        seen = set()
        for node in self.k8s_api.list_node().items:
            if (node.metadata.annotations or {}).get('kwok.x-k8s.io/node') != 'fake':
                continue
            name = node.metadata.name
            labels = node.metadata.labels or {}
            instance_type = labels.get('node.kubernetes.io/instance-type')
            pool = labels.get('karpenter.sh/nodepool') if self.karpenter else 'none'
            if not name or not instance_type or not pool or name in seen:
                raise ValueError(f'Invalid or duplicate fake node: {name}')
            seen.add(name)
            node_data.setdefault(instance_type, []).append([name, pool])
        for nodes in node_data.values():
            nodes.sort()
        return node_data

    def get_real_nodes_dict(self):
        columns = ['node', 'instance_type'] + (['nodepool'] if self.karpenter else [])
        unique = self.pods_allocation[columns].drop_duplicates()
        if unique['node'].duplicated().any():
            raise ValueError('Conflicting instance type or nodepool for allocation node')
        node_data = {}
        for _, row in unique.iterrows():
            pool = row['nodepool'] if self.karpenter else 'none'
            node_data.setdefault(row['instance_type'], []).append([row['node'], pool])
        return node_data

    def get_owners_with_pods(self):
        owners = {}
        for pod in self.k8s_api.list_pod_for_all_namespaces().items:
            owner = (pod.metadata.labels or {}).get('deployment')
            if owner:
                key = (pod.metadata.namespace, owner)
                owners.setdefault(key, []).append(pod.metadata.name)
        return {key: sorted(names) for key, names in owners.items()}

    def map_fake_and_real_nodes(self, fake_nodes_dict, real_nodes_dict):
        node_mapping = {}
        for instance_type, real_nodes in real_nodes_dict.items():
            available = list(fake_nodes_dict.get(instance_type, []))
            for real_name, pool in real_nodes:
                match = next((node for node in available if node[1] == pool), None)
                if match is None:
                    raise ValueError(f'No fake node for {real_name}, type={instance_type}, nodepool={pool}')
                node_mapping[real_name] = match[0]
                available.remove(match)
        mapped = self.pods_allocation['node'].map(node_mapping)
        if mapped.isna().any():
            raise ValueError('Allocation contains unmapped destinations')
        self.pods_allocation['node'] = mapped

    def map_pods_and_nodes(self, owners_with_pods):
        expected = self.pods_allocation.groupby(['namespace', 'replicaset'])['pods_count'].sum()
        available = {}
        for key, count in expected.items():
            pods = sorted(owners_with_pods.get(key, []))
            if len(pods) != count or len(set(pods)) != len(pods):
                raise ValueError(f'Pod coverage for {key}: expected {count}, found {len(pods)} (must be unique)')
            available[key] = iter(pods)
        rows = []
        for _, row in self.pods_allocation.iterrows():
            key = (row['namespace'], row['replicaset'])
            for _ in range(row['pods_count']):
                rows.append(dict(pod=next(available[key]), namespace=row['namespace'], node=row['node']))
        result = pd.DataFrame(rows, columns=['pod', 'namespace', 'node'])
        if result[['namespace', 'pod']].duplicated().any():
            raise ValueError('Pod belongs to more than one allocation owner')
        return result

    @staticmethod
    def _publish(frame, path):
        # Readers must never observe a partial CSV.
        fd, temporary = tempfile.mkstemp(dir=os.path.dirname(path), suffix='.csv')
        try:
            with os.fdopen(fd, 'w') as output:
                frame.to_csv(output, index=False)
            os.replace(temporary, path)
        finally:
            if os.path.exists(temporary):
                os.unlink(temporary)

    def run(self):
        self.log('Building initial placement')
        self.pods_allocation = pd.read_csv(
            self.PODS_ALLOCATION_PATH, keep_default_na=False,
            dtype={field: str for field in ('namespace', 'replicaset', 'node', 'instance_type', 'nodepool')})
        required = ['namespace', 'replicaset', 'node', 'instance_type', 'pods_count']
        if self.karpenter:
            required.append('nodepool')
        if not set(required).issubset(self.pods_allocation.columns):
            raise ValueError(f'Allocation requires columns {required}')
        for column in required:
            values = self.pods_allocation[column]
            if values.isna().any() or values.astype(str).str.strip().eq('').any():
                raise ValueError(f'Empty allocation field: {column}')
        counts = pd.to_numeric(self.pods_allocation['pods_count'], errors='raise')
        if ((counts < 0) | (counts % 1 != 0)).any():
            raise ValueError('pods_count must be a nonnegative integer')
        self.pods_allocation['pods_count'] = counts.astype(int)
        self.map_fake_and_real_nodes(self.get_fake_nodes_dict(), self.get_real_nodes_dict())
        result = self.map_pods_and_nodes(self.get_owners_with_pods())
        self._publish(self.pods_allocation, self.PODS_ALLOCATION_PATH)
        self._publish(result, self.PODS_AND_NODES_PATH)
        return result
