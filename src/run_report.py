"""Versioned operational report shared by the shell and the replay coordinator.

Only one process owns the report at a time. Threads serialize updates and readers
always see a complete JSON document, including when the writer is interrupted.
"""
import argparse
from contextlib import contextmanager, nullcontext
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import threading
import time
import uuid
import re
from datetime import datetime, timezone


def utc_now():
    return datetime.now(timezone.utc).isoformat()


class RunReport:
    DETAIL_LIMIT = 100

    def __init__(self, output_dir=None, *, resume=False, monotonic=None, configuration=None, trace_path=None):
        self.monotonic = monotonic or time.monotonic
        self.lock = threading.RLock()
        self.interruption_requested = False
        run_id = uuid.uuid4().hex
        if resume and not output_dir:
            raise ValueError('resuming a run requires its output directory')
        automatic = not output_dir
        if automatic:
            label = Path(str(trace_path).rstrip('/')).name if trace_path else ''
            label = re.sub(r'[^\w.-]+', '-', label).strip('.-').encode()[:100].decode(errors='ignore')
            prefix = f'output_csv_{label}_' if label else 'output_csv_'
            base = prefix + datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
            suffix = 0
            while True:
                candidate = Path(base + (f'_{suffix}' if suffix else '')).resolve()
                if Path(str(candidate) + '.zip').exists():
                    suffix += 1
                    continue
                try:
                    candidate.mkdir(parents=True, exist_ok=False)
                except FileExistsError:
                    suffix += 1
                    continue
                output_dir = candidate
                break
        self.output_dir = Path(output_dir).resolve()
        self.path = self.output_dir / 'run.json'
        if resume:
            self.data = json.loads(self.path.read_text())
            if self.data['status'] != 'running':
                raise ValueError('cannot resume a finalized run')
        else:
            # Even an empty existing directory belongs to somebody else.
            if Path(str(self.output_dir) + '.zip').exists():
                raise FileExistsError(f'archive already exists: {self.output_dir}.zip')
            if not automatic:
                self.output_dir.mkdir(parents=True, exist_ok=False)
            try:
                revision = subprocess.check_output(
                    ['git', 'rev-parse', 'HEAD'], cwd=Path(__file__).resolve().parent,
                    stderr=subprocess.DEVNULL, text=True).strip()
            except (OSError, subprocess.CalledProcessError):
                revision = None
            self.data = dict(schema_version=1, run_id=run_id,
                started_at=utc_now(), finished_at=None, revision=revision,
                status='running', stage='validation', configuration=configuration or {},
                traces=[], timing=dict(started_monotonic=self.monotonic(),
                    preparation_real=None, replay_real=None, producers_real=None,
                    usage_drain_real=None, finalization_real=None,
                    total_real=None, origin_unix=None, origin_monotonic=None,
                    scheduled_logical=None, progress_logical=None, tail_logical=15,
                    tail_real=None, effective_factor=None, replay_complete=False),
                phases={}, progress={}, lag={}, errors=[], warnings=[], error_count=0, warning_count=0)
            self.save()

    def save(self):
        with self.lock:
            fd, name = tempfile.mkstemp(prefix='.run-', suffix='.tmp', dir=self.output_dir)
            try:
                with os.fdopen(fd, 'w') as stream:
                    json.dump(self.data, stream, indent=2, allow_nan=False)
                    stream.write('\n')
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(name, self.path)
            finally:
                if os.path.exists(name):
                    os.unlink(name)

    def update(self, **fields):
        with self.lock:
            self.data.update(fields)
            self.save()

    def stage(self, stage):
        self.update(stage=stage)

    def timing(self, **fields):
        with self.lock:
            self.data['timing'].update(fields)
            self.save()

    def begin_phase(self, name, *, parent=None, expected=None, concurrency=None, unit=None, started=None):
        """Record inclusive wall time. Sum siblings only, never parents + children."""
        with self.lock:
            phases = self.data.setdefault('phases', {})
            if name in phases:
                raise ValueError(f'phase already recorded: {name}')
            phases[name] = dict(parent=parent, started_monotonic=self.monotonic() if started is None else started,
                ended_monotonic=None, duration_real=None, result='running',
                expected=expected, completed=0 if expected is not None else None, failed=0,
                concurrency=concurrency, unit=unit)
            self.save()
        return name

    def phase_progress(self, name, *, completed=None, failed=None, expected=None):
        with self.lock:
            phase = self.data['phases'][name]
            for key, value in dict(completed=completed, failed=failed, expected=expected).items():
                if value is not None:
                    phase[key] = value
            self.save()

    def end_phase(self, name, *, result='completed', ended=None):
        with self.lock:
            phase = self.data['phases'][name]
            phase.update(ended_monotonic=self.monotonic() if ended is None else ended, result=result)
            phase['duration_real'] = phase['ended_monotonic'] - phase['started_monotonic']
            self.save()

    @contextmanager
    def phase(self, name, **fields):
        self.begin_phase(name, **fields)
        try:
            yield name
        except BaseException as error:
            self.phase_progress(name, failed=max(1, self.data['phases'][name]['failed']))
            self.end_phase(name, result='interrupted' if isinstance(error, (KeyboardInterrupt, SystemExit)) else 'failed')
            raise
        else:
            if self.data['phases'][name]['result'] == 'running':
                self.end_phase(name)

    def import_measurements(self, path):
        """Import a synchronous child's journal only after it has stopped writing."""
        if not Path(path).exists():
            return
        # Events contain only fixed phase names, monotonic times and exit codes.
        lines = Path(path).read_text().splitlines()
        for index, line in enumerate(lines):
            try:
                event = json.loads(line)
            except json.JSONDecodeError:
                if index != len(lines) - 1:
                    raise
                self.issue('incomplete final setup measurement; retaining recorded progress',
                           warning=True, component='shell')
                break
            name = event['name']
            if event['event'] == 'begin':
                if name not in self.data.get('phases', {}):
                    self.begin_phase(name, parent='external_setup', concurrency=1, started=event['time'])
            elif self.data['phases'][name]['result'] == 'running':
                code = event['exit_code']
                self.phase_progress(name, failed=int(code != 0))
                self.end_phase(name, ended=event['time'], result=(
                    'interrupted' if code in (130, 143) else 'failed' if code else 'completed'))

    def issue(self, message, *, warning=False, component='coordinator', stage=None, **context):
        with self.lock:
            key = 'warnings' if warning else 'errors'
            self.data['warning_count' if warning else 'error_count'] += 1
            detail = dict(timestamp=utc_now(), stage=stage or self.data['stage'],
                          component=component, message=str(message), **context)
            # Keep full context in the execution log even when JSON details are capped.
            print(f"[RUN] {key}: {json.dumps(detail)}", flush=True)
            if len(self.data[key]) < self.DETAIL_LIMIT:
                self.data[key].append(detail)
            self.save()

    def trace(self, path, role):
        path = Path(path).resolve()
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for chunk in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(chunk)
        with self.lock:
            self.data['traces'].append(dict(path=str(path), role=role,
                size=path.stat().st_size, sha256=digest.hexdigest()))
            self.save()

    def finish(self, *, interrupted=False):
        with self.lock:
            # Cleanup errors must not erase an interruption or its primary error.
            status = ('interrupted' if interrupted or self.interruption_requested or self.data['status'] == 'interrupted'
                      else 'failed' if self.data['error_count'] else
                      'completed_with_warnings' if self.data['warning_count'] else 'completed')
            for name, phase in self.data.get('phases', {}).items():
                if phase['result'] == 'running':
                    self.end_phase(name, result='interrupted' if status == 'interrupted' else 'failed')
            self.data['timing']['total_real'] = self.monotonic() - self.data['timing']['started_monotonic']
            self.update(status=status, finished_at=utc_now())


def setup_phase(owner, name, **fields):
    """Allow managers to run independently without creating another report."""
    report = getattr(owner, 'report', None)
    return report.phase(name, **fields) if report is not None else nullcontext()


def setup_progress(owner, name, **fields):
    report = getattr(owner, 'report', None)
    if report is not None:
        report.phase_progress(name, **fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'stage', 'finish', 'begin', 'end', 'import', 'measurement'])
    parser.add_argument('--output-dir')
    parser.add_argument('--phase')
    parser.add_argument('--parent')
    parser.add_argument('--measurements')
    parser.add_argument('--event', choices=['begin', 'end'])
    parser.add_argument('--stage', default='setup')
    parser.add_argument('--exit-code', type=int, default=0)
    parser.add_argument('--mode')
    parser.add_argument('--trace-path')
    parser.add_argument('--speed-factor')
    parser.add_argument('--infrastructure')
    parser.add_argument('--workload')
    parser.add_argument('--karpenter')
    parser.add_argument('--hpa')
    parser.add_argument('--workers-config')
    args = parser.parse_args()
    if args.action == 'measurement':
        # A child writes only its journal, never the coordinator report.
        event = dict(name=args.phase, event=args.event, time=time.monotonic(), exit_code=args.exit_code)
        with open(args.measurements, 'a') as stream:
            stream.write(json.dumps(event) + '\n')
            stream.flush()
            os.fsync(stream.fileno())
        return
    configuration = {key: value for key, value in vars(args).items()
                     if key in ('mode', 'speed_factor', 'infrastructure', 'workload', 'karpenter', 'hpa')
                     and value is not None}
    report = RunReport(args.output_dir, configuration=configuration, trace_path=args.trace_path) if args.action == 'init' else None
    if args.action == 'init':
        print(report.output_dir)
        return
    # Shell finalization is deliberately idempotent and preserves Python errors.
    path = Path(args.output_dir) / 'run.json'
    data = json.loads(path.read_text())
    if data['status'] != 'running':
        return
    report = RunReport(args.output_dir, resume=True)
    if args.action == 'begin':
        report.begin_phase(args.phase, parent=args.parent, concurrency=1)
    elif args.action == 'end':
        report.phase_progress(args.phase, failed=int(args.exit_code != 0))
        report.end_phase(args.phase, result='interrupted' if args.exit_code in (130, 143) else 'failed' if args.exit_code else 'completed')
    elif args.action == 'import':
        report.import_measurements(args.measurements)
    elif args.action == 'stage':
        configuration = dict(report.data['configuration'])
        if args.workers_config:
            from kwok_config import read_workers
            configuration['requested_workers'] = read_workers(args.workers_config)
        report.update(configuration=configuration)
        report.stage(args.stage)
    else:
        if args.exit_code:
            report.issue(f'external command exited with code {args.exit_code}', component='shell')
        report.finish(interrupted=args.exit_code in (130, 143))


if __name__ == '__main__':
    main()
