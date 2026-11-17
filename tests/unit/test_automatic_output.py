from datetime import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from run_report import RunReport


ROOT = Path(__file__).resolve().parents[2]


def test_workload_date_and_collision_safety(tmp_path, monkeypatch):
    class Fixed(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2026, 11, 17, 15, 30, tzinfo=tz)
    monkeypatch.setattr('run_report.datetime', Fixed)
    monkeypatch.chdir(tmp_path)
    first = RunReport(trace_path='/traces/tiny workload/')
    base = 'output_csv_tiny-workload_2026-11-17-15:30:00'
    assert first.output_dir.name == base
    second = RunReport(trace_path='/traces/tiny workload/')
    assert second.output_dir.name == base + '_1'
    assert second.data['run_id'] != first.data['run_id']
    (tmp_path / (base + '_2.zip')).write_bytes(b'previous archive')
    third = RunReport(trace_path='/traces/tiny workload/')
    assert third.output_dir.name == base + '_3'
    assert json.loads(first.path.read_text())['run_id'] == first.data['run_id']
    assert (tmp_path / (base + '_2.zip')).read_bytes() == b'previous archive'


def test_shell_without_output_flag_uses_trace_name_before_setup(tmp_path):
    src = tmp_path / 'src'
    src.mkdir()
    for name in ('run_report.py', 'experiment_clock.py'):
        shutil.copy(ROOT / 'src' / name, src / name)
    shutil.copy(ROOT / 'execute-emulation.sh', tmp_path / 'execute-emulation.sh')
    install = tmp_path / 'kwok-karpenter-install'
    install.mkdir()
    setup = install / 'setup.sh'
    setup.write_text('#!/bin/sh\nexit 23\n')
    setup.chmod(0o755)
    env = dict(os.environ, PATH=str(Path(sys.executable).parent) + ':' + os.environ['PATH'])
    result = subprocess.run(['bash', 'execute-emulation.sh', '--sim', '--data-path',
        '/traces/full-trace-reduzido-30-minutos/'], cwd=tmp_path, env=env,
        capture_output=True, text=True, timeout=10)
    assert result.returncode == 23, result.stdout + result.stderr
    outputs = list(tmp_path.glob('output_csv_full-trace-reduzido-30-minutos_*'))
    assert len(outputs) == 1
    report = json.loads((outputs[0] / 'run.json').read_text())
    assert report['status'] == 'failed'
    assert report['errors'][0]['stage'] == 'setup'
    assert (outputs[0] / 'emulation.log').exists()
