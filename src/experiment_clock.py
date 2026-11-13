"""Experiment deadlines and thread-safe replay diagnostics.

Only experiment durations are scaled. API/readiness/cleanup timeouts use real
seconds. Monotonic time schedules events; Unix time identifies the same origin
for consumers of exported metrics.
"""
import copy
import math
import threading
import time


# Keep the shortest logical interval representable at millisecond resolution.
MIN_INTERVAL_SECONDS = 0.001
LOGICAL_INTERVALS = (5, 15, 30)


def validate_speed_factor(value, karpenter=False, hpa=False, development=False):
    try:
        factor = float(value)
    except (TypeError, ValueError, OverflowError) as error:
        raise ValueError("speed-factor must be a finite number >= 1") from error
    if not math.isfinite(factor) or factor < 1:
        raise ValueError("speed-factor must be a finite number >= 1")
    if any(interval / factor < MIN_INTERVAL_SECONDS for interval in LOGICAL_INTERVALS):
        raise ValueError("speed-factor produces experiment intervals below 1 ms")
    if factor > 1 and (karpenter or hpa or development):
        raise ValueError("speed-factor > 1 requires emulation without HPA or Karpenter")
    return factor


class ExperimentClock:
    def __init__(self, speed_factor=1, monotonic=None, wait=None, unix_time=None):
        self.speed_factor = validate_speed_factor(speed_factor)
        self.monotonic = monotonic or time.monotonic
        self.wait = wait or time.sleep
        self.unix_time = unix_time or time.time
        self.origin_monotonic = None
        self.origin_unix = None

    def start(self):
        if self.origin_monotonic is not None:
            raise RuntimeError("experiment clock already started")
        self.origin_monotonic = self.monotonic()
        self.origin_unix = self.unix_time()
        return self

    def real_duration(self, logical_seconds):
        return logical_seconds / self.speed_factor

    def deadline(self, timestamp):
        if self.origin_monotonic is None:
            raise RuntimeError("experiment clock has not started")
        return self.origin_monotonic + self.real_duration(timestamp)

    def wait_until(self, deadline):
        # Recheck after an early wake-up without introducing relative drift.
        while True:
            remaining = deadline - self.monotonic()
            if remaining <= 0:
                return
            self.wait(remaining)

    def tail(self, logical_seconds=15):
        self.wait_until(self.monotonic() + self.real_duration(logical_seconds))


class ReplayDiagnostics:
    """Snapshots may be read by the coordinator while a replay is active.

    Completed actions mean their calls returned, not cluster convergence.
    A queued usage action means delivery to the queue, not reconciliation.
    """
    def __init__(self, expected_entries):
        self._lock = threading.Lock()
        self._data = dict(
            expected_entries=expected_entries, processed_entries=0,
            not_applicable_entries=0, not_applicable_actions=0,
            attempted_actions=0, completed_actions=0, failed_actions=0,
            final_timestamp=None, late_events=0, max_lag_real=0,
            total_lag_real=0, max_lag_logical=0, total_lag_logical=0,
            worst_event=None, actions=[],
        )

    def snapshot(self):
        with self._lock:
            return copy.deepcopy(self._data)

    def entry(self, index, timestamp, scheduled, started, factor, applicable):
        lag = max(0, started - scheduled)
        with self._lock:
            data = self._data
            data['processed_entries'] += 1
            data['not_applicable_entries'] += int(not applicable)
            data['final_timestamp'] = timestamp
            data['late_events'] += int(lag > 0)
            data['total_lag_real'] += lag
            data['total_lag_logical'] += lag * factor
            if lag > data['max_lag_real']:
                data['max_lag_real'] = lag
                data['max_lag_logical'] = lag * factor
                data['worst_event'] = dict(index=index, timestamp=timestamp,
                                          scheduled=scheduled, started=started)

    def skip(self):
        with self._lock:
            self._data['not_applicable_actions'] += 1

    def apply(self, clock, scheduled, context, action, *args):
        started = clock.monotonic()
        lag = max(0, started - scheduled)
        record = dict(context=copy.deepcopy(context), scheduled=scheduled, started=started,
                      trace_timestamp=(scheduled - clock.origin_monotonic) * clock.speed_factor,
                      lag_real=lag, lag_logical=lag * clock.speed_factor)
        with self._lock:
            self._data['attempted_actions'] += 1
        try:
            action(*args)
        except Exception as error:
            record.update(completed=False, error=str(error))
        else:
            record.update(completed=True, error=None)
        record['finished'] = clock.monotonic()
        with self._lock:
            self._data['completed_actions' if record['completed'] else 'failed_actions'] += 1
            self._data['actions'].append(record)
        return record


def replay_entries(trace, clock, diagnostics, dynamic):
    """Yield entries in input order, including ties and overdue events."""
    for index, entry in enumerate(trace):
        scheduled = clock.deadline(entry['timestamp'])
        clock.wait_until(scheduled)
        started = clock.monotonic()
        if dynamic:
            yield entry, scheduled
        diagnostics.entry(index, entry['timestamp'], scheduled,
                          started, clock.speed_factor, dynamic)
    # Tail begins after actual completion of all actions, even when late.
    clock.tail()


if __name__ == '__main__':
    # Small dependency-free preflight used by the shell before setup.
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--speed-factor', default='1')
    parser.add_argument('--karpenter', action='store_true')
    parser.add_argument('--hpa', action='store_true')
    parser.add_argument('--development', action='store_true')
    args = parser.parse_args()
    try:
        validate_speed_factor(args.speed_factor, args.karpenter, args.hpa, args.development)
    except ValueError as error:
        parser.error(str(error))
