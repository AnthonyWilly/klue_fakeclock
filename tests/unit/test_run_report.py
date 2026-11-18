import hashlib
import json
from pathlib import Path
import subprocess
import sys
from unittest.mock import Mock
import zipfile

import pytest

from collector import Collector
from run_report import RunReport


ROOT = Path(__file__).resolve().parents[2]


def test_exclusive_output_atomic_updates_and_identity(tmp_path, monkeypatch):
    now = [10.0]
    report = RunReport(tmp_path / 'out', monotonic=lambda: now[0])
    with pytest.raises(FileExistsError):
        RunReport(report.output_dir)
    original_replace = __import__('os').replace
    seen = []
    def replace(source, target):
        # A reader can still parse the previous report until the rename.
        seen.append(json.loads(Path(target).read_text())['status'])
        json.loads(Path(source).read_text())
        original_replace(source, target)
    monkeypatch.setattr('run_report.os.replace', replace)
    trace = tmp_path / 'input.json'
    trace.write_bytes(b'{"emulation": []}')
    report.trace(trace, 'replay')
    report.stage('replay')
    report.issue('late', warning=True, component='infrastructure', timestamp_context=3)
    now[0] += 7
    report.finish()
    data = json.loads(report.path.read_text())
    assert data['schema_version'] == 1
    assert data['status'] == 'completed_with_warnings'
    assert data['timing']['total_real'] == 7
    assert data['timing']['replay_real'] is None
    # Additive fields start null; legacy fields keep their names.
    assert data['timing']['producers_real'] is None
    assert data['timing']['usage_drain_real'] is None
    assert data['timing']['usage_barrier_real'] is None  # Only coordinated replays wait on barriers.
    assert data['traces'][0]['sha256'] == hashlib.sha256(trace.read_bytes()).hexdigest()
    assert all(state == 'running' for state in seen)
    assert not list(report.output_dir.glob('.run-*.tmp'))
    assert not any('fidelity' in key for key in data)


def test_bounded_details_keep_total_and_primary_error(tmp_path):
    report = RunReport(tmp_path / 'out')
    report.issue('primary', stage='replay', component='workload')
    for index in range(120):
        report.issue(f'cleanup {index}', stage='teardown')
    report.finish()
    assert report.data['status'] == 'failed'
    assert report.data['error_count'] == 121
    assert len(report.data['errors']) == report.DETAIL_LIMIT
    assert report.data['errors'][0]['message'] == 'primary'


def test_interrupted_survives_cleanup_error(tmp_path):
    report = RunReport(tmp_path / 'out')
    report.finish(interrupted=True)
    report.issue('cleanup')
    report.finish()
    assert json.loads(report.path.read_text())['status'] == 'interrupted'


def test_failed_atomic_write_preserves_valid_report(tmp_path, monkeypatch):
    report = RunReport(tmp_path / 'out')
    before = report.path.read_bytes()
    monkeypatch.setattr('run_report.os.replace', Mock(side_effect=OSError('disk')))
    with pytest.raises(OSError):
        report.issue('failure')
    assert report.path.read_bytes() == before
    assert not list(report.output_dir.glob('.run-*.tmp'))


def test_archive_contains_finalized_report_even_without_csvs(tmp_path):
    report = RunReport(tmp_path / 'out')
    report.finish()
    Collector(30).archive(str(report.output_dir))
    with zipfile.ZipFile(str(report.output_dir) + '.zip') as archive:
        assert json.loads(archive.read('run.json')) == json.loads(report.path.read_text())


def test_shell_finalizer_preserves_specific_python_diagnosis(tmp_path):
    report = RunReport(tmp_path / 'out')
    report.stage('tracer')
    report.issue('bad trace', component='tracer')
    report.finish()
    before = report.path.read_bytes()
    subprocess.run([sys.executable, str(ROOT / 'src/run_report.py'), 'finish',
                    '--output-dir', str(report.output_dir), '--exit-code', '1'], check=True)
    assert report.path.read_bytes() == before


def test_existing_archive_is_not_overwritten(tmp_path):
    archive = tmp_path / 'out.zip'
    archive.write_bytes(b'previous run')
    with pytest.raises(FileExistsError, match='archive already exists'):
        RunReport(tmp_path / 'out')
    assert archive.read_bytes() == b'previous run'


def test_nested_phases_are_inclusive_and_preserve_partial_counts(tmp_path):
    now = [10.0]
    report = RunReport(tmp_path / 'out', monotonic=lambda: now[0])
    with pytest.raises(TimeoutError):
        with report.phase('setup', concurrency=1):
            now[0] = 12
            with report.phase('apply', parent='setup', expected=4, concurrency=1):
                report.phase_progress('apply', completed=2)
                now[0] = 15
                raise TimeoutError('deadline')
    phases = json.loads(report.path.read_text())['phases']
    assert phases['setup']['duration_real'] == 5
    assert phases['apply']['duration_real'] == 3
    assert phases['apply']['parent'] == 'setup'
    assert phases['apply']['completed'] == 2
    assert phases['apply']['expected'] == 4
    assert phases['apply']['failed'] == 1
    assert phases['setup']['result'] == phases['apply']['result'] == 'failed'
    assert report.data['timing']['effective_factor'] is None
    assert 'replay' not in phases


def test_phase_interruption_and_unfinished_shell_phase(tmp_path):
    now = [10.0]
    report = RunReport(tmp_path / 'out', monotonic=lambda: now[0])
    report.begin_phase('external_setup')
    with pytest.raises(KeyboardInterrupt):
        with report.phase('install', parent='external_setup'):
            now[0] = 13
            raise KeyboardInterrupt()
    report.finish(interrupted=True)
    assert report.data['phases']['external_setup']['result'] == 'interrupted'
    assert report.data['phases']['install']['duration_real'] == 3
    assert report.data['phases']['install']['result'] == 'interrupted'


def test_import_child_measurements_is_idempotent_and_uses_child_clock(tmp_path):
    report = RunReport(tmp_path / 'out', monotonic=lambda: 20)
    journal = tmp_path / 'measurements.jsonl'
    journal.write_text('\n'.join(json.dumps(event) for event in [
        dict(name='tools', event='begin', time=11, exit_code=0),
        dict(name='tools', event='end', time=15, exit_code=23),
        dict(name='monitoring', event='begin', time=16, exit_code=0)]))
    report.import_measurements(journal)
    report.import_measurements(journal)
    assert report.data['phases']['tools']['duration_real'] == 4
    assert report.data['phases']['tools']['result'] == 'failed'
    assert report.data['phases']['tools']['failed'] == 1
    report.finish(interrupted=True)
    assert report.data['phases']['monitoring']['result'] == 'interrupted'
    assert report.data['phases']['monitoring']['duration_real'] == 4


def test_incomplete_child_journal_retains_started_phase(tmp_path):
    report = RunReport(tmp_path / 'out', monotonic=lambda: 20)
    journal = tmp_path / 'measurements.jsonl'
    journal.write_text(json.dumps(dict(name='tools', event='begin', time=11, exit_code=0)) + '\n{"name":')
    report.import_measurements(journal)
    report.finish(interrupted=True)
    assert report.data['phases']['tools']['duration_real'] == 9
    assert report.data['phases']['tools']['result'] == 'interrupted'
    assert report.data['warning_count'] == 1
