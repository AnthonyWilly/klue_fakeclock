"""Run the setup entry point with fake external commands and no cluster."""
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

import pytest


ROOT = Path(__file__).resolve().parents[2]


@pytest.mark.parametrize('fail_creation', [False, True])
def test_fresh_setup_waits_for_prometheus_and_propagates_timeout(tmp_path, fail_creation):
    install = tmp_path / 'kwok-karpenter-install'
    install.mkdir()
    shutil.copy(ROOT / 'kwok-karpenter-install/setup.sh', install / 'setup.sh')
    src = tmp_path / 'src'
    src.mkdir()
    for name in ('kwok_config.py', 'sampling.py', 'experiment_clock.py', 'run_report.py'):
        shutil.copy(ROOT / 'src' / name, src / name)
    for name in ('install-kwok.sh', 'setup-vpa.sh'):
        stub = install / name
        stub.write_text('#!/bin/sh\nexit 0\n')
        stub.chmod(0o755)
    grafana = install / 'configuration-files/grafana'
    grafana.mkdir(parents=True)
    (grafana / 'grafana-configmap.yml').write_text('placeholder')
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    calls = tmp_path / 'calls.jsonl'
    shim = bindir / 'shim'
    shim.write_text('''#!'''+sys.executable+'''
import json, os, pathlib, sys
name = pathlib.Path(sys.argv[0]).name
args = sys.argv[1:]
with open(os.environ['KLUE_TEST_CALLS'], 'a') as log:
    log.write(json.dumps([name, *args]) + '\\n')
if name == 'kubectl':
    if '-f' in args and args[args.index('-f') + 1] == '-':
        sys.stdin.read()
    if 'get' in args and 'servicemonitor' in args:
        if '--ignore-not-found' not in args:
            print(json.dumps({'spec': {'endpoints': [{'port': 'https-main'}]}}))
    elif 'get' in args and 'pod' in args:
        print('Error from server (NotFound)', file=sys.stderr)
        sys.exit(1)
    elif '--for=create' in args and 'pod/prometheus-k8s-0' in args:
        if os.environ['KLUE_TEST_FAIL_CREATION'] == '1':
            print('timed out waiting for pod creation', file=sys.stderr)
            sys.exit(23)
elif name == 'minikube':
    print('127.0.0.1')
elif name == 'envsubst':
    print(sys.stdin.read())
''')
    shim.chmod(0o755)
    for name in ('kubectl', 'go', 'docker', 'minikube', 'envsubst'):
        (bindir / name).symlink_to(shim)
    env = dict(os.environ, PATH=str(bindir) + ':' + str(Path(sys.executable).parent) + ':' + os.environ['PATH'],
               KLUE_TEST_CALLS=str(calls), KLUE_TEST_FAIL_CREATION=str(int(fail_creation)),
               KLUE_SETUP_MEASUREMENTS=str(tmp_path / 'setup.jsonl'),
               KLUE_SPEED_FACTOR='1', KLUE_KWOK_WORKERS_CONFIG='')
    result = subprocess.run(['bash', 'setup.sh', 'karpenter-off'], cwd=install,
        env=env, capture_output=True, text=True, timeout=10)
    from run_report import RunReport
    report = RunReport(tmp_path / 'report')
    report.import_measurements(env['KLUE_SETUP_MEASUREMENTS'])
    phases = report.data['phases']
    assert phases['install_tools']['result'] == 'completed'
    assert phases['prometheus_ready']['result'] == ('failed' if fail_creation else 'completed')
    assert phases['prometheus_ready']['failed'] == int(fail_creation)
    assert all(phase['parent'] == 'external_setup' and phase['duration_real'] >= 0 for phase in phases.values())
    assert ('configure_grafana' in phases) != fail_creation
    commands = [json.loads(line) for line in calls.read_text().splitlines()]
    create = ['kubectl', '-n', 'monitoring', 'wait', '--for=create', 'pod/prometheus-k8s-0', '--timeout=300s']
    ready = ['kubectl', '-n', 'monitoring', 'wait', '--for=condition=Ready', 'pod/prometheus-k8s-0', '--timeout=300s']
    assert create in commands
    if fail_creation:
        assert result.returncode == 23, result.stdout + result.stderr
        assert 'timed out waiting for pod creation' in result.stderr
        assert ready not in commands
        assert not any(command[0] == 'minikube' for command in commands)
    else:
        assert result.returncode == 0, result.stdout + result.stderr
        assert commands.index(create) < commands.index(ready)
        assert ['minikube', 'ip'] in commands
