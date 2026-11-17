import os
from pathlib import Path
import shutil
import signal
import subprocess
import threading
from unittest.mock import Mock

import pytest

from manager import Manager
from run_report import RunReport

ROOT = Path(__file__).resolve().parents[2]


def executable(path, text):
    path.write_text('#!/bin/bash\nset -eu\n' + text)
    path.chmod(0o755)


@pytest.fixture
def shell(tmp_path):
    src = tmp_path / 'src'
    scheduler = src / 'custom-k8s-scheduler'
    scheduler.mkdir(parents=True)
    shutil.copy(ROOT / 'src/build-scheduler.sh', src)
    shutil.copy(ROOT / 'src/custom-k8s-scheduler/default-script.sh', scheduler)
    commands = tmp_path / 'bin'
    commands.mkdir()
    log = tmp_path / 'commands'
    executable(commands / 'go', 'echo build >> "$COMMAND_LOG"\n')
    executable(commands / 'kubectl', 'echo "kubectl $*" >> "$COMMAND_LOG"\nexit "${APPLY_CODE:-0}"\n')
    executable(scheduler / 'custom-scheduler', 'echo "scheduler $*" >> "$COMMAND_LOG"\nexit "${SCHEDULER_CODE:-0}"\n')
    mapping = tmp_path / 'mapping.csv'
    mapping.write_text('pod,namespace,node\n' + ''.join(f'p{i},ns,node\n' for i in range(3000)))
    env = dict(os.environ, PATH=f'{commands}:' + os.environ['PATH'],
               COMMAND_LOG=str(log), PODS_AND_NODES_MAP=str(mapping))
    return src / 'build-scheduler.sh', env, log, scheduler


def test_script_bulk_confirmation_and_build_every_time(shell):
    script, env, log, _ = shell
    for _ in range(2):
        subprocess.run(['bash', str(script), '2'], env=env, check=True)
    lines = log.read_text().splitlines()
    assert lines.count('build') == 2
    assert len([line for line in lines if line.startswith('scheduler ')]) == 2
    assert len([line for line in lines if line.startswith('kubectl ')]) == 6
    assert all('get pod' not in line for line in lines)
    assert '--workers 2' in lines[1]


@pytest.mark.parametrize('failed', ['scheduler', 'build', 'apply', 'missing-map'])
def test_script_propagates_failure(shell, failed):
    script, env, log, scheduler = shell
    if failed == 'scheduler':
        env['SCHEDULER_CODE'] = '23'
    elif failed == 'build':
        executable(Path(env['PATH'].split(':')[0]) / 'go', 'exit 23\n')
    elif failed == 'apply':
        env['APPLY_CODE'] = '23'
    else:
        Path(env['PODS_AND_NODES_MAP']).unlink()
    result = subprocess.run(['bash', str(script)], env=env)
    assert result.returncode == (1 if failed == 'missing-map' else 23)
    lines = log.read_text().splitlines() if log.exists() else []
    assert len([line for line in lines if line.startswith('kubectl ')]) == (1 if failed == 'apply' else 0)
    if failed in ('build', 'missing-map'):
        assert not any(line.startswith('scheduler ') for line in lines)


def test_script_interrupt_reaps_scheduler(shell):
    script, env, log, scheduler = shell
    ready = threading.Event()
    executable(scheduler / 'custom-scheduler', 'echo $$ > "$COMMAND_LOG.pid"\ntrap \'exit 0\' TERM\nwhile :; do sleep 0.05; done\n')
    proc = subprocess.Popen(['bash', str(script)], env=env)
    try:
        # Wait on an observable child-start condition, with a bounded deadline.
        import time
        deadline = time.monotonic() + 3
        pidfile = Path(str(log) + '.pid')
        while not pidfile.exists() and time.monotonic() < deadline:
            ready.wait(0.01)
        assert pidfile.exists()
        pid = int(pidfile.read_text())
        proc.terminate()
        assert proc.wait(timeout=3) == 143
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


@pytest.mark.parametrize('factor, workers', [(1, 1), (1.5, 2), (2, 2), (5, 5), (10, 8)])
def test_coordinator_passes_bind_limit(tmp_path, monkeypatch, factor, workers):
    obj = Manager.__new__(Manager)
    obj.report = RunReport(tmp_path / 'out')
    obj.speed_factor = factor
    obj.pods_mapping = Mock()
    process = Mock(args=['scheduler'], pid=12345)
    process.wait.return_value = 0
    popen = Mock(return_value=process)
    monkeypatch.setattr('manager.subprocess.Popen', popen)
    obj.start_mapping_and_scheduler()
    popen.assert_called_once_with(['bash', 'src/build-scheduler.sh', str(workers)], start_new_session=True)
    assert obj.report.data['configuration']['bind_concurrency'] == workers


@pytest.mark.parametrize('failure', ['exit', 'interrupt', 'kill'])
def test_coordinator_propagates_failure_and_reaps_group(tmp_path, monkeypatch, failure):
    obj = Manager.__new__(Manager)
    obj.report = RunReport(tmp_path / 'out')
    obj.speed_factor = 2
    obj.pods_mapping = Mock()
    process = Mock(args=['scheduler'], pid=12345)
    if failure == 'exit':
        process.wait.side_effect = [23, 23]
        expected = subprocess.CalledProcessError
    elif failure == 'interrupt':
        process.wait.side_effect = [KeyboardInterrupt, 0]
        expected = KeyboardInterrupt
    else:
        process.wait.side_effect = [KeyboardInterrupt, subprocess.TimeoutExpired('scheduler', 5), 0]
        expected = KeyboardInterrupt
    monkeypatch.setattr('manager.subprocess.Popen', Mock(return_value=process))
    kill = Mock()
    monkeypatch.setattr('manager.os.killpg', kill)
    with pytest.raises(expected):
        obj.start_mapping_and_scheduler()
    assert kill.call_args_list[0].args == (12345, signal.SIGTERM)
    if failure == 'kill':
        assert kill.call_args_list[1].args == (12345, signal.SIGKILL)
    assert obj.report.data['phases']['scheduler_build_binds']['result'] in ('failed', 'interrupted')
