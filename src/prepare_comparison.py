"""Create an explicit logical-time view for the existing R comparison scripts."""
import argparse
import csv
from decimal import Decimal, InvalidOperation
import hashlib
import json
from pathlib import Path
import shutil
import tempfile


MANIFEST = 'comparison.json'
CPU_COUNTERS = {
    'container_cpu_usage_seconds_total', 'pod_cpu_usage_seconds_total',
    'node_cpu_usage_seconds_total', 'node_cpu_seconds_total',
}
USAGE = {'timestamp', 'value', 'container', 'namespace', 'pod'}
RESOURCES = USAGE | {'node', 'uid', 'resource'}
REQUIRED = {
    'container_cpu_usage_seconds_total': USAGE,
    'kube_pod_container_resource_requests': RESOURCES,
    'kube_pod_container_resource_limits': RESOURCES,
    'kube_pod_owner': {'timestamp', 'uid', 'namespace', 'pod', 'owner_name', 'owner_kind'},
    'kube_replicaset_owner': {'timestamp', 'replicaset', 'namespace', 'owner_kind', 'owner_name'},
}
MEMORY = ('container_memory_working_set_bytes', 'container_memory_usage_bytes')
STATUSES = {'running', 'completed', 'completed_with_warnings', 'failed', 'interrupted'}


def number(value, name, minimum=None):
    try:
        result = Decimal(str(value))
    except InvalidOperation:
        raise ValueError(f'{name} must be a finite number') from None
    if not result.is_finite() or (minimum is not None and result < minimum):
        raise ValueError(f'{name} must be finite and >= {minimum}')
    return result


def normalized(row):
    result = dict(row)
    for label in ('namespace', 'pod'):
        if 'exported_' + label in row:
            result[label] = row['exported_' + label]
    return result


def consumed(metric, row):
    """Match the R filters used before computing its common t0."""
    row = normalized(row)
    if metric in ('container_cpu_usage_seconds_total', *MEMORY):
        return bool(row['pod'] and row['container'] and row['container'] != 'POD')
    if metric in ('kube_pod_container_resource_requests', 'kube_pod_container_resource_limits'):
        return bool(row['pod'] and row['node'] and row['resource'] in ('cpu', 'memory'))
    return False


def prepare_comparison(input_dir, output_dir, *, speed_factor=None, real_origin=None):
    source, output = Path(input_dir).resolve(), Path(output_dir).resolve()
    if not source.is_dir():
        raise ValueError(f'input directory does not exist: {source}')
    if output.exists() or output == source or source in output.parents:
        raise ValueError('output must be a distinct, nonexistent directory outside input')
    if (source / MANIFEST).exists():
        raise ValueError('input is already a comparison view; repeated transformation is forbidden')
    report_path = source / 'run.json'
    report = None
    warnings = []
    if report_path.exists():
        if speed_factor is not None or real_origin is not None:
            raise ValueError('run.json supplies factor and origin; overrides are forbidden')
        report = json.loads(report_path.read_text())
        if report.get('schema_version') != 1 or report.get('status') not in STATUSES:
            raise ValueError('unsupported run.json schema or status')
        factor = number(report.get('configuration', {}).get('speed_factor'), 'speed factor', 1)
        timing = report.get('timing', {})
        origin = number(timing.get('origin_unix'), 'real origin', 0)
        replay = number(timing.get('replay_real'), 'replay duration', 0)
        tail = number(timing.get('tail_real') or 0, 'tail duration', 0)
        end = origin + replay + tail
        partial = report['status'] not in ('completed', 'completed_with_warnings') or not timing.get('replay_complete')
        if partial:
            warnings.append('Partial execution: this view does not represent a complete replay.')
        if report.get('error_count') or report.get('warning_count'):
            warnings.append('Source report contains operational errors or warnings; inspect run.json.')
    else:
        if speed_factor is None or real_origin is None:
            raise ValueError('legacy input requires --speed-factor 1 and --real-origin explicitly')
        factor = number(speed_factor, 'speed factor', 1)
        if factor != 1:
            raise ValueError('legacy input requires --speed-factor 1')
        origin = number(real_origin, 'real origin', 0)
        end = None
        partial = None
        warnings.append('Legacy input: replay completeness and phase boundaries are unknown.')

    paths = {p.stem: p for p in sorted(source.glob('*.csv'))}
    memory = next((metric for metric in MEMORY if metric in paths), None)
    required = dict(REQUIRED)
    if memory is None:
        raise ValueError('missing required memory metric: ' + ' or '.join(MEMORY))
    required[memory] = USAGE
    missing = required.keys() - paths.keys()
    if missing:
        raise ValueError(f'missing required metrics: {sorted(missing)}')
    # Validate fully before publishing a directory. Temporary output stays beside it.
    output.parent.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix='.comparison-', dir=output.parent))
    files = []
    minimum = None
    maximum = None
    observed_min = None
    available_end = None
    try:
        for metric, path in paths.items():
            count = 0
            resources = set()
            usable = 0
            digest = hashlib.sha256()
            # Hash in bounded chunks so large exports do not need to fit in memory.
            with path.open('rb') as stream:
                for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                    digest.update(chunk)
            with path.open(newline='') as raw, (temporary / path.name).open('w', newline='') as target:
                reader = csv.DictReader(raw)
                fields = reader.fieldnames
                if not fields or len(set(fields)) != len(fields):
                    raise ValueError(f'{path.name}: missing or duplicate header')
                schema = set(normalized(dict.fromkeys(fields)))
                needed = required.get(metric, {'timestamp', 'value'})
                if not needed <= schema:
                    raise ValueError(f'{path.name}: missing columns {sorted(needed - schema)}')
                writer = csv.DictWriter(target, fieldnames=fields)
                writer.writeheader()
                for row in reader:
                    if None in row or any(value is None for value in row.values()):
                        raise ValueError(f'{path.name}: malformed CSV row')
                    timestamp = number(row['timestamp'], f'{path.name} timestamp')
                    if metric in required and consumed(metric, row):
                        observed_min = timestamp if observed_min is None else min(observed_min, timestamp)
                    if timestamp < origin or (end is not None and timestamp > end):
                        continue
                    if 'value' in row:
                        value = number(row['value'], f'{path.name} value')
                        if metric in CPU_COUNTERS:
                            row['value'] = str(value * factor)
                    available_end = timestamp if available_end is None else max(available_end, timestamp)
                    logical = (timestamp - origin) * factor
                    row['timestamp'] = str(logical)
                    writer.writerow(row)
                    count += 1
                    if metric in required:
                        if consumed(metric, row):
                            usable += 1
                            minimum = logical if minimum is None else min(minimum, logical)
                            maximum = logical if maximum is None else max(maximum, logical)
                        if metric in ('kube_pod_container_resource_requests', 'kube_pod_container_resource_limits'):
                            resources.add(row['resource'])
                needs_usable = metric in (*MEMORY, 'container_cpu_usage_seconds_total',
                                          'kube_pod_container_resource_requests',
                                          'kube_pod_container_resource_limits')
                if metric in required and (not count or (needs_usable and not usable)):
                    raise ValueError(f'{path.name}: no samples in replay window')
                if metric in ('kube_pod_container_resource_requests', 'kube_pod_container_resource_limits') and not {'cpu', 'memory'} <= resources:
                    raise ValueError(f'{path.name}: cpu and memory resource samples are required')
            files.append(dict(name=path.name, source_sha256=digest.hexdigest(), rows=count,
                              value_multiplier=str(factor if metric in CPU_COUNTERS else 1)))
        if minimum != 0 or (report is None and observed_min != origin):
            raise ValueError('alignment incompatible with R scripts: consumed metrics must start at origin (zero); no samples fabricated')
        manifest = dict(schema_version=1, kind='klue-comparison-view',
            source=dict(directory=str(source), run_id=report.get('run_id') if report else None,
                        revision=report.get('revision') if report else None,
                        status=report.get('status') if report else 'legacy',
                        report_sha256=hashlib.sha256(report_path.read_bytes()).hexdigest() if report else None),
            speed_factor=str(factor), real_origin=str(origin), partial=partial,
            window=dict(real_start=str(origin), real_end=str(end if end is not None else available_end),
                        logical_start='0', logical_end=str(((end if end is not None else available_end) - origin) * factor),
                        consumed_available_end=str(maximum)),
            transformation=dict(timestamp='(real_timestamp - real_origin) * speed_factor',
                                cpu_counters=sorted(CPU_COUNTERS), cpu_value='raw_value * speed_factor',
                                other_values='unchanged', interpolation=False),
            warnings=warnings, files=files)
        (temporary / MANIFEST).write_text(json.dumps(manifest, indent=2) + '\n')
        # mkdir exclusively claims the destination; it never overwrites existing data.
        output.mkdir()
        try:
            for path in temporary.iterdir():
                path.rename(output / path.name)
        except BaseException:
            shutil.rmtree(output)
            raise
        return manifest
    finally:
        shutil.rmtree(temporary)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', required=True)
    parser.add_argument('--output', required=True)
    parser.add_argument('--speed-factor')
    parser.add_argument('--real-origin')
    args = parser.parse_args()
    try:
        manifest = prepare_comparison(args.input, args.output,
            speed_factor=args.speed_factor, real_origin=args.real_origin)
    except (ValueError, OSError, KeyError, TypeError) as error:
        parser.exit(2, f'Cannot prepare comparison: {error}\n')
    print(f'Comparison view: {Path(args.output).resolve()}')
    for warning in manifest['warnings']:
        print(f'Warning: {warning}')


if __name__ == '__main__':
    main()
