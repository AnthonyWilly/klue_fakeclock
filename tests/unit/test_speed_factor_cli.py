import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
from unittest.mock import Mock

import pytest

import main as entrypoint
from manager import Manager


ROOT = Path(__file__).resolve().parents[2]
BASE_ARGS = ['trace', 'nodepool', 'karpenter-off', 'skip-tracer', 'dynamic', 'dynamic', 'hpa-off']


@pytest.fixture(autouse=True)
def isolated_outputs(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)


@pytest.mark.parametrize('value', ['nan', 'inf', '-inf', 'oops', '0', '.5', '5001', '1e300'])
def test_python_cli_rejects_invalid_values_before_actions(monkeypatch, value):
    for name in ('apply_nodepool', 'run_tracer', 'run_manager'):
        monkeypatch.setattr(entrypoint.Main, name, Mock(side_effect=AssertionError('side effect')))
    with pytest.raises(SystemExit) as error:
        entrypoint.main(BASE_ARGS + [f'--speed-factor={value}'])
    assert error.value.code == 2


@pytest.mark.parametrize('position,value', [(2, 'karpenter-on'), (6, 'hpa-on')])
def test_python_cli_rejects_accelerated_autoscaling(monkeypatch, position, value):
    args = list(BASE_ARGS)
    args[position] = value
    actions = [Mock(side_effect=AssertionError('side effect')) for _ in range(3)]
    for name, action in zip(('apply_nodepool', 'run_tracer', 'run_manager'), actions):
        monkeypatch.setattr(entrypoint.Main, name, action)
    with pytest.raises(SystemExit) as error:
        entrypoint.main(args + ['--speed-factor=2'])
    assert error.value.code == 2
    assert not any(action.called for action in actions)


@pytest.mark.parametrize('factor', [None, '1', '2', '5', '1.5'])
def test_python_cli_passes_factor_to_manager(monkeypatch, factor):
    monkeypatch.setattr(entrypoint.Main, 'run_tracer', Mock())
    manager_class = Mock()
    monkeypatch.setattr(entrypoint, 'Manager', manager_class)
    entrypoint.main(BASE_ARGS + ([] if factor is None else ['--speed-factor', factor]))
    assert manager_class.call_args.kwargs['speed_factor'] == float(factor or 1)
    manager_class.return_value.run.assert_called_once()


@pytest.mark.parametrize('constructor', [entrypoint.Main, Manager])
def test_python_constructors_validate_before_cluster_access(constructor, monkeypatch):
    monkeypatch.setattr('kubernetes.config.load_kube_config', Mock(side_effect=AssertionError('cluster accessed')))
    kwargs = dict(karpenter=True, hpa=False, speed_factor=2)
    if constructor is entrypoint.Main:
        kwargs.update(trace_path='trace', nodepool_path='np', tracer_skip=True,
                      infrastructure='dynamic', workload='dynamic')
    with pytest.raises(ValueError, match='without HPA or Karpenter'):
        constructor(**kwargs)


@pytest.fixture
def shell_environment(tmp_path):
    # Isolated script copy: no command can reach the real setup script.
    (tmp_path / 'src').mkdir()
    (tmp_path / 'kwok-karpenter-install').mkdir()
    script = (ROOT / 'execute-emulation.sh').read_text().replace('/tmp/emulation.log', str(tmp_path / 'emulation.log'))
    (tmp_path / 'execute-emulation.sh').write_text(script)
    shutil.copyfile(ROOT / 'src/run_report.py', tmp_path / 'src/run_report.py')
    shutil.copyfile(ROOT / 'src/experiment_clock.py', tmp_path / 'src/experiment_clock.py')
    shutil.copyfile(ROOT / 'src/kwok_config.py', tmp_path / 'src/kwok_config.py')
    calls = tmp_path / 'calls'
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    # Run only the dependency-free validator. Record any main entry invocation.
    shim = bindir / 'python3'
    shim.write_text(f'''#!{sys.executable}
import json, os, runpy, sys
args = sys.argv[1:]
if args[0] == '-u':
    args.pop(0)
if args[0] == '-c':
    code = args[1]
    sys.argv = ['-c', *args[2:]]
    exec(code, {{'__name__': '__main__'}})
elif args[0].endswith(('experiment_clock.py', 'kwok_config.py', 'run_report.py')):
    sys.argv = args
    runpy.run_path(args[0], run_name='__main__')
else:
    with open(os.environ['CALLS'], 'a') as stream:
        stream.write(json.dumps(args) + '\\n')
''')
    shim.chmod(0o755)
    for path, label in [(bindir / 'kubectl', 'kubectl'),
                        (tmp_path / 'kwok-karpenter-install/setup.sh', 'setup')]:
        path.write_text(f'#!/bin/bash\nprintf "%s\\n" "{label}" >> "$CALLS"\n')
        path.chmod(0o755)
    env = dict(os.environ, PATH=str(bindir) + os.pathsep + os.environ['PATH'], CALLS=str(calls))
    def run(args):
        result = subprocess.run(['bash', 'execute-emulation.sh', *args], cwd=tmp_path,
                                env=env, capture_output=True, text=True, timeout=10)
        return result, calls.read_text().splitlines() if calls.exists() else []
    return run


@pytest.mark.parametrize('extra', [
    ['--speed-factor', 'nan'], ['--speed-factor', 'inf'], ['--speed-factor', '-inf'],
    ['--speed-factor', '-2'], ['--speed-factor', '.5'], ['--speed-factor', '5001'],
    ['--speed-factor', 'oops'], ['--speed-factor', '7'], ['--speed-factor'], ['--speed-factor', '--use-hpa'],
    ['--speed-factor', '2', '--use-hpa'],
    ['--speed-factor', '5', '--use-karpenter', '--nodepool-path', 'np'],
    ['--speed-factor', '2', '--dev'],
])
def test_shell_rejects_before_setup_or_kubernetes(shell_environment, extra):
    result, calls = shell_environment(['--sim', '--use-cluster', 'unused', '--data-path', 'trace', *extra])
    assert result.returncode != 0
    assert calls == []


@pytest.mark.parametrize('factor', [None, '1', '2', '5', '1.5'])
def test_shell_passes_valid_speed_factor(shell_environment, factor):
    result, calls = shell_environment(['--sim', '--data-path', 'trace'] +
                                     ([] if factor is None else ['--speed-factor', factor]))
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[0] == 'setup'
    assert f'--speed-factor={factor or "1"}' in json.loads(calls[1])


@pytest.mark.parametrize('extra', [['--use-hpa'], ['--use-karpenter', '--nodepool-path', 'np'], ['--dev']])
def test_shell_preserves_legacy_modes_at_one(shell_environment, extra):
    result, calls = shell_environment(['--sim', '--data-path', 'trace', *extra])
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[0] == 'setup'


def test_shell_passes_valid_worker_path_with_spaces(shell_environment, tmp_path):
    path = tmp_path / 'worker experiment.yaml'
    path.write_text('podPlayStageParallelism: 8')
    result, calls = shell_environment(['--sim', '--data-path', 'trace', '--speed-factor', '2',
                                      '--kwok-workers-config', str(path)])
    assert result.returncode == 0, result.stdout + result.stderr
    assert calls[0] == 'setup'
    arguments = json.loads(calls[1])
    assert arguments[arguments.index('--kwok-workers-config') + 1] == str(path)
    assert '--speed-factor=2' in arguments


def test_python_passes_valid_worker_overrides_to_manager(tmp_path, monkeypatch):
    path = tmp_path / 'workers.yaml'
    path.write_text('nodeLeaseParallelism: 6')
    monkeypatch.setattr(entrypoint.Main, 'run_tracer', Mock())
    manager = Mock()
    monkeypatch.setattr(entrypoint, 'Manager', manager)
    entrypoint.main(BASE_ARGS + ['--kwok-workers-config', str(path)])
    assert manager.call_args.kwargs['kwok_workers_config'] == str(path)
