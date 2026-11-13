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
