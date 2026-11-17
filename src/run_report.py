"""Versioned operational report shared by the shell and the replay coordinator.

Only one process owns the report at a time. Threads serialize updates and readers
always see a complete JSON document, including when the writer is interrupted.
"""
import argparse
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
                    preparation_real=None, replay_real=None, finalization_real=None,
                    total_real=None, origin_unix=None, origin_monotonic=None,
                    scheduled_logical=None, progress_logical=None, tail_logical=15,
                    tail_real=None, effective_factor=None, replay_complete=False),
                progress={}, lag={}, errors=[], warnings=[], error_count=0, warning_count=0)
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
            self.data['timing']['total_real'] = self.monotonic() - self.data['timing']['started_monotonic']
            self.update(status=status, finished_at=utc_now())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('action', choices=['init', 'stage', 'finish'])
    parser.add_argument('--output-dir')
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
    if args.action == 'stage':
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
