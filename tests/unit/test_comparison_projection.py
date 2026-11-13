import csv
import json
from decimal import Decimal

import pytest

from prepare_comparison import MEMORY, REQUIRED, prepare_comparison


def fixture_run(tmp_path, factor=5, status='completed'):
    source = tmp_path / 'raw'
    source.mkdir()
    report = dict(schema_version=1, run_id='example', revision='abc', status=status,
        configuration=dict(speed_factor=factor), timing=dict(origin_unix=1000,
        replay_real=3, tail_real=1, replay_complete=status == 'completed'))
    (source / 'run.json').write_text(json.dumps(report))
    for metric, schema in dict(REQUIRED, **{MEMORY[0]: REQUIRED['container_cpu_usage_seconds_total']}).items():
        rows = []
        for timestamp, value in [('999.5', '99'), ('1000', '4'), ('1001', '6'),
                                 ('1002', '10'), ('1003', '1'), ('1004', '3'), ('1005', '99')]:
            row = dict.fromkeys(schema, 'label')
            row.update(timestamp=timestamp)
            if 'value' in schema:
                row['value'] = value if 'cpu_usage' in metric else str(2 ** 30)
            if 'resource' in schema:
                for resource in ['cpu', 'memory']:
                    rows.append(dict(row, resource=resource))
            else:
                rows.append(row)
        write(source / (metric + '.csv'), sorted(schema), rows)
    return source


def write(path, fields, rows):
    with path.open('w', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def read(path):
    with path.open() as stream:
        return list(csv.DictReader(stream))


@pytest.mark.parametrize('factor', [1, 2, 5])
def test_units_resets_labels_and_raw_preserved(tmp_path, factor):
    source = fixture_run(tmp_path, factor)
    before = {p.name: p.read_bytes() for p in source.iterdir()}
    out = tmp_path / 'view'
    manifest = prepare_comparison(source, out)
    cpu = read(out / 'container_cpu_usage_seconds_total.csv')
    times = [Decimal(r['timestamp']) for r in cpu]
    values = [Decimal(r['value']) for r in cpu]
    rates = [(b-a)/(t-s) for a,b,s,t in zip(values, values[1:], times, times[1:])]
    assert rates == [2, 4, -9, 2]  # R retains first sample and masks negative deltas.
    assert values[1] - values[0] == 2 * factor  # 2 cores, 1 real second, F logical seconds.
    assert times[1] - times[0] == factor
    for metric in REQUIRED.keys() | {MEMORY[0]}:
        rows = read(out / (metric + '.csv'))
        assert rows[0]['timestamp'] == '0'
        assert rows[0].get('namespace') == 'label'
        if metric != 'container_cpu_usage_seconds_total' and 'value' in rows[0]:
            assert Decimal(rows[0]['value']) == 2 ** 30
    assert Decimal(read(out / (MEMORY[0] + '.csv'))[0]['value']) / 1024**3 == 1
    assert manifest['partial'] is False
    assert {p.name: p.read_bytes() for p in source.iterdir()} == before
    with pytest.raises(ValueError, match='already a comparison'):
        prepare_comparison(out, tmp_path / 'twice')
    with pytest.raises(ValueError, match='nonexistent'):
        prepare_comparison(source, out)


def test_fractional_offsets_aliases_unknown_metrics_and_buckets(tmp_path):
    source = fixture_run(tmp_path)
    memory = source / (MEMORY[0] + '.csv')
    rows = read(memory)
    rows[2]['timestamp'] = '1001.125'
    for row in rows:
        row['exported_pod'] = row.pop('pod')
        row['exported_namespace'] = row.pop('namespace')
    write(memory, list(rows[0]), rows)
    for name in ['pod_cpu_usage_seconds_total', 'node_cpu_seconds_total', 'unknown_cpu_counter', 'kube_pod_status_phase']:
        write(source / (name + '.csv'), ['timestamp', 'value'], [dict(timestamp='1000', value='2')])
    out = tmp_path / 'view'
    prepare_comparison(source, out)
    rows = read(out / memory.name)
    assert rows[1]['timestamp'] == '5.625'
    assert rows[1]['exported_pod'] == 'label'
    assert 'pod' not in rows[1]
    assert read(out / 'node_cpu_seconds_total.csv')[0]['value'] == '10'
    assert read(out / 'pod_cpu_usage_seconds_total.csv')[0]['value'] == '10'
    assert read(out / 'unknown_cpu_counter.csv')[0]['value'] == '2'
    assert read(out / 'kube_pod_status_phase.csv')[0]['value'] == '2'
    assert [int(Decimal(r['timestamp']) // 300) * 300 for r in rows] == [0] * 5


def test_legacy_metadata_and_available_window(tmp_path):
    source = fixture_run(tmp_path, 1)
    (source / 'run.json').unlink()
    with pytest.raises(ValueError, match='legacy input requires'):
        prepare_comparison(source, tmp_path / 'missing')
    with pytest.raises(ValueError, match='speed-factor 1'):
        prepare_comparison(source, tmp_path / 'wrong', speed_factor=2, real_origin=999.5)
    out = tmp_path / 'view'
    manifest = prepare_comparison(source, out, speed_factor=1, real_origin=999.5)
    assert manifest['source']['status'] == 'legacy'
    assert manifest['partial'] is None
    assert manifest['window']['consumed_available_end'] == '5.5'
    assert manifest['window']['real_end'] == '1005'
    assert manifest['window']['logical_end'] == '5.5'
    assert read(out / 'container_cpu_usage_seconds_total.csv')[0]['timestamp'] == '0.0'
    with pytest.raises(ValueError, match='alignment'):
        prepare_comparison(source, tmp_path / 'late', speed_factor=1, real_origin=1000)


@pytest.mark.parametrize('field,value', [('speed_factor', 0), ('speed_factor', 'NaN'),
    ('speed_factor', 'inf'), ('origin_unix', None), ('origin_unix', -1),
    ('replay_real', -1), ('tail_real', -1)])
def test_invalid_report(tmp_path, field, value):
    source = fixture_run(tmp_path)
    path = source / 'run.json'
    report = json.loads(path.read_text())
    report['configuration' if field == 'speed_factor' else 'timing'][field] = value
    path.write_text(json.dumps(report))
    with pytest.raises(ValueError):
        prepare_comparison(source, tmp_path / 'view')
    assert not (tmp_path / 'view').exists()


def test_partial_and_overrides(tmp_path):
    source = fixture_run(tmp_path, status='interrupted')
    manifest = prepare_comparison(source, tmp_path / 'view')
    assert manifest['partial'] is True
    assert manifest['warnings']
    assert manifest['source']['status'] == 'interrupted'
    with pytest.raises(ValueError, match='overrides'):
        prepare_comparison(source, tmp_path / 'other', real_origin=1000)


@pytest.mark.parametrize('defect', ['missing', 'schema', 'empty', 'origin', 'resource'])
def test_incompatible_input_is_not_published(tmp_path, defect):
    source = fixture_run(tmp_path)
    path = source / 'kube_pod_container_resource_requests.csv'
    rows = read(path)
    if defect == 'missing':
        path.unlink()
    elif defect == 'schema':
        write(path, ['timestamp', 'value'], [dict(timestamp=1000, value=1)])
    elif defect == 'empty':
        write(path, list(rows[0]), [])
    elif defect == 'resource':
        write(path, list(rows[0]), [r for r in rows if r['resource'] == 'cpu'])
    else:
        for path in source.glob('*.csv'):
            rows = read(path)
            write(path, list(rows[0]), [r for r in rows if Decimal(r['timestamp']) != 1000])
    with pytest.raises(ValueError):
        prepare_comparison(source, tmp_path / 'view')
    assert not (tmp_path / 'view').exists()
    assert not list(tmp_path.glob('.comparison-*'))


@pytest.mark.parametrize('factor', [1, 2, 5])
def test_bucket_boundaries_share_experiment_origin(tmp_path, factor):
    source = fixture_run(tmp_path, factor)
    report_path = source / 'run.json'
    report = json.loads(report_path.read_text())
    report['timing'].update(replay_real=700 / factor, tail_real=0)
    report_path.write_text(json.dumps(report))
    path = source / 'container_cpu_usage_seconds_total.csv'
    template = read(path)[0]
    logical_times = [Decimal('0'), Decimal('299.875'), Decimal('300'), Decimal('600.125')]
    rows = [dict(template, timestamp=str(Decimal(1000) + t / factor), value='2') for t in logical_times]
    write(path, list(template), rows)
    out = tmp_path / 'view'
    prepare_comparison(source, out)
    times = [Decimal(r['timestamp']) for r in read(out / path.name)]
    assert times == logical_times
    assert [int(t // 300) * 300 for t in times] == [0, 0, 300, 600]


def test_cli_reports_failure_without_publishing(tmp_path):
    import subprocess
    import sys
    from pathlib import Path

    result = subprocess.run([sys.executable, str(Path(__file__).parents[2] / 'src' / 'prepare_comparison.py'),
        '--input', str(tmp_path / 'missing'), '--output', str(tmp_path / 'view')],
        capture_output=True, text=True)
    assert result.returncode == 2
    assert 'input directory does not exist' in result.stderr
    assert not (tmp_path / 'view').exists()


def test_memory_fallback_and_unusable_origin(tmp_path):
    source = fixture_run(tmp_path)
    (source / (MEMORY[0] + '.csv')).rename(source / (MEMORY[1] + '.csv'))
    prepare_comparison(source, tmp_path / 'fallback')
    path = source / 'container_cpu_usage_seconds_total.csv'
    rows = read(path)
    for row in rows:
        row['container'] = 'POD'
    write(path, list(rows[0]), rows)
    with pytest.raises(ValueError, match='no samples'):
        prepare_comparison(source, tmp_path / 'invalid')
