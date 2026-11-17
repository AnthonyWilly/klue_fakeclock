"""Exercise the real strategic merge without accessing a Kubernetes cluster."""
import json
from pathlib import Path
import shutil
import subprocess

import pytest


ROOT = Path(__file__).resolve().parents[2]
PATCH = ROOT / 'kwok-karpenter-install/configuration-files/kube-state-metrics-resources.yaml'


def test_cpu_overlay_preserves_release_configuration_and_vpa_arguments(tmp_path):
    kubectl = shutil.which('kubectl')
    if kubectl is None:
        pytest.skip('kubectl required for offline strategic merge regression')
    containers = [
        dict(name='kube-state-metrics', image='ksm:test',
             args=['--port=8081', '--custom-resource-state-config=preserve-vpa'],
             resources=dict(requests=dict(cpu='10m', memory='190Mi'),
                            limits=dict(cpu='100m', memory='250Mi'))),
        dict(name='kube-rbac-proxy-main', image='proxy:test',
             args=['--secure-listen-address=:8443', '--upstream=http://127.0.0.1:8081/'],
             resources=dict(requests=dict(cpu='20m', memory='20Mi'),
                            limits=dict(cpu='40m', memory='40Mi'))),
        dict(name='kube-rbac-proxy-self', image='proxy:test',
             resources=dict(requests=dict(cpu='10m', memory='20Mi'),
                            limits=dict(cpu='20m', memory='40Mi'))),
    ]
    deployment = dict(apiVersion='apps/v1', kind='Deployment',
        metadata=dict(name='kube-state-metrics', namespace='monitoring'),
        spec=dict(selector=dict(matchLabels=dict(app='ksm')), template=dict(
            metadata=dict(labels=dict(app='ksm')), spec=dict(containers=containers))))
    path = tmp_path / 'deployment.json'
    path.write_text(json.dumps(deployment))
    result = subprocess.run([kubectl, 'patch', '--local', '-f', str(path),
        '--type=strategic', '--patch-file', str(PATCH), '-o', 'json'],
        capture_output=True, text=True, check=True, timeout=10)
    merged = json.loads(result.stdout)
    expected = json.loads(json.dumps(deployment))
    expected_containers = expected['spec']['template']['spec']['containers']
    expected_containers[0]['resources']['requests']['cpu'] = '100m'
    expected_containers[0]['resources']['limits']['cpu'] = '1000m'
    expected_containers[1]['resources']['requests']['cpu'] = '100m'
    expected_containers[1]['resources']['limits']['cpu'] = '500m'
    assert merged['spec'] == expected['spec']
