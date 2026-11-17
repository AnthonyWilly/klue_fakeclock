import copy
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock

import pytest
import yaml

from kwok_config import read_workers, merge_workers

ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('content', ['{}', '[]', 'null', 'unknown: 4', 'nodeLeaseParallelism: 0', 'nodeLeaseParallelism: -1',
                                      'podPlayStageParallelism: true', 'nodePlayStageParallelism: 1.5',
                                      'nodeLeaseParallelism: 4294967296', 'podPlayStageParallelism: ['])
def test_invalid_overrides_rejected_without_external_call(tmp_path, monkeypatch, content):
    monkeypatch.setattr(subprocess, 'run', Mock(side_effect=AssertionError('external call')))
    path = tmp_path / 'workers.yaml'
    path.write_text(content)
    with pytest.raises(ValueError):
        read_workers(path)


def test_merge_only_overrides_requested_fields_and_preserves_all_documents(tmp_path):
    path = tmp_path / 'workers.yaml'
    path.write_text('podPlayStageParallelism: 8\nnodeLeaseParallelism: 6\n')
    options = dict(podPlayStageParallelism=4, nodePlayStageParallelism=4, nodeLeaseParallelism=4,
                   nodeLeaseDurationSeconds=40, manageNodesWithAnnotationSelector='fake')
    documents = [dict(apiVersion='config.kwok.x-k8s.io/v1alpha1', kind='KwokConfiguration', options=options),
                 dict(apiVersion='kwok.x-k8s.io/v1alpha1', kind='Stage', spec={'x': 'keep'})]
    config = dict(metadata={'name': 'kwok'}, data={'kwok.yaml': yaml.safe_dump_all(documents), 'other': 'keep'})
    before = copy.deepcopy(config)
    merged = merge_workers(config, read_workers(path))
    actual = list(yaml.safe_load_all(merged['data']['kwok.yaml']))
    assert actual[0]['options'] == {**options, 'podPlayStageParallelism': 8, 'nodeLeaseParallelism': 6}
    assert actual[1] == documents[1]
    assert merged['data']['other'] == 'keep'
    assert config == before
    assert read_workers(None) == {}


def test_unsupported_installed_config_fails():
    with pytest.raises(ValueError):
        merge_workers({'data': {'kwok.yaml': 'kind: Unsupported'}}, {'nodeLeaseParallelism': 2})


def test_ksm_cli_emits_only_endpoint_patch_from_live_object():
    monitor = dict(apiVersion='monitoring.coreos.com/v1', kind='ServiceMonitor',
        metadata=dict(name='kube-state-metrics', resourceVersion='724', uid='server-uid',
                      annotations={'kubectl.kubernetes.io/last-applied-configuration': 'base'}),
        status={'server': 'owned'},
        spec=dict(selector={'keep': 'selector'}, endpoints=[
            dict(port='https-main', interval='30s', bearerTokenFile='/token',
                 tlsConfig={'insecureSkipVerify': True}),
            dict(port='https-self', interval='30s', scheme='https')]))
    result = subprocess.run([sys.executable, 'src/kwok_config.py', 'ksm', '--speed-factor', '2'],
        cwd=ROOT, input=json.dumps(monitor), capture_output=True, text=True,
        timeout=10, check=True)
    expected = copy.deepcopy(monitor['spec']['endpoints'])
    expected[0].update(interval='15s', scrapeTimeout='10s')
    assert json.loads(result.stdout) == {'spec': {'endpoints': expected}}


def test_preflight_shell_and_python_reject_workers_before_setup(tmp_path):
    # Running the actual preflight with failing inputs must never reach these shims.
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    calls = tmp_path / 'calls'
    for executable in ['kubectl', 'go', 'docker', 'curl']:
        shim = bindir / executable
        shim.write_text(f'#!/bin/sh\ntouch "{calls}"\nexit 90\n')
        shim.chmod(0o755)
    path = tmp_path / 'workers.yaml'
    path.write_text('podPlayStageParallelism: 0')
    import os
    env = dict(os.environ, PATH=str(bindir) + ':' + str(Path(sys.executable).parent) + ':' + os.environ['PATH'], KLUE_KWOK_WORKERS_CONFIG=str(path))
    for command in [
        ['bash', 'execute-emulation.sh', '--sim', '--data-path', 'trace', '--kwok-workers-config', str(path)],
        ['bash', 'kwok-karpenter-install/setup.sh', 'karpenter-off'],
        ['bash', 'kwok-karpenter-install/install-kwok.sh'],
        [sys.executable, 'src/main.py', 'trace', '', 'karpenter-off', 'skip-tracer', 'dynamic', 'dynamic', 'hpa-off', '--kwok-workers-config', str(path)],
    ]:
        result = subprocess.run(command, cwd=ROOT, env=env, capture_output=True, text=True, timeout=10)
        assert result.returncode != 0, result.stdout
        assert not calls.exists()
