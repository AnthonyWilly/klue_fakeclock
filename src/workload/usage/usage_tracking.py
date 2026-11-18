"""Parent-side accounting of usage events and their terminal acknowledgements.

Every accepted event receives a unique sequential identifier. The worker reports
when it starts an event and exactly one terminal outcome for it. Outcomes may
arrive out of order: the highest acknowledged identifier does not imply that
every earlier event finished, so the contiguous watermark is tracked separately.

Times are ``time.monotonic`` values, which share one clock across processes on
the same machine. Setup events have no replay deadline and therefore no logical
completion lag; their queue age is measured from submission.
"""
import copy
import time

APPLIED = 'applied'
EMPTY = 'empty'
TARGET_ABSENT = 'target_absent'
FAILED = 'failed'
CANCELLED = 'cancelled'
OUTCOMES = (APPLIED, EMPTY, TARGET_ABSENT, FAILED, CANCELLED)
# Terminal outcomes that keep the legacy ``completed`` meaning: the worker
# handled the event without error. Only APPLIED proves written usage.
SUCCESSFUL = frozenset((APPLIED, EMPTY, TARGET_ABSENT))


class UsageTracker:
    def __init__(self, monotonic=None):
        self.monotonic = monotonic or time.monotonic
        self.next_event_id = 1
        self.pending = {}
        self.active = set()
        # Terminal identifiers above the contiguous watermark.
        self.done_above = set()
        self.acknowledged_through = 0
        self.highest_acknowledged = 0
        self.out_of_order = 0
        self.protocol_errors = 0
        self.max_pending = 0
        self.max_active = 0
        self.max_oldest_pending_age_real = 0
        self.max_oldest_pending_age_logical = 0
        self.outcomes = dict.fromkeys(OUTCOMES, 0)
        self.execution = dict(count=0, total_real=0, max_real=0)
        self.completion = dict(replay_events=0, late=0, max_real=0, max_logical=0,
                               total_real=0, total_logical=0, worst=None)

    def register(self, event):
        """Record an event already delivered with ``self.next_event_id``."""
        event_id = self.next_event_id
        if event.get('event_id') != event_id:
            raise ValueError(f'usage event registered out of sequence: {event.get("event_id")}')
        timing = event.get('replay_timing') or {}
        self.pending[event_id] = dict(
            namespace=event.get('namespace'), resource=event.get('name'),
            trace_timestamp=timing.get('trace_timestamp'),
            scheduled_monotonic=timing.get('scheduled_monotonic'),
            speed_factor=timing.get('speed_factor'),
            enqueued_monotonic=self.monotonic())
        self.next_event_id += 1
        self.max_pending = max(self.max_pending, len(self.pending))
        return event_id

    def started(self, message):
        event_id = message.get('event_id')
        if event_id not in self.pending or event_id in self.active:
            return self._protocol_error('start', message)
        self.active.add(event_id)
        self.max_active = max(self.max_active, len(self.active))
        return None

    def acknowledge(self, message):
        """Apply one terminal outcome; return a protocol error message or None."""
        event_id = message.get('event_id')
        outcome = message.get('outcome')
        if event_id not in self.pending:
            known = isinstance(event_id, int) and 0 < event_id < self.next_event_id
            return self._protocol_error('duplicate' if known else 'unknown', message)
        if outcome not in OUTCOMES:
            return self._protocol_error('invalid outcome', message)
        if bool(message.get('completed')) != (outcome in SUCCESSFUL):
            return self._protocol_error('inconsistent outcome', message)
        event = self.pending.pop(event_id)
        self.active.discard(event_id)
        self.outcomes[outcome] += 1
        if event_id != self.acknowledged_through + 1:
            self.out_of_order += 1
        self.highest_acknowledged = max(self.highest_acknowledged, event_id)
        self.done_above.add(event_id)
        while self.acknowledged_through + 1 in self.done_above:
            self.acknowledged_through += 1
            self.done_above.discard(self.acknowledged_through)
        started, finished = message.get('started_monotonic'), message.get('finished_monotonic')
        if started is not None and finished is not None:
            duration = max(0, finished - started)
            self.execution['count'] += 1
            self.execution['total_real'] += duration
            self.execution['max_real'] = max(self.execution['max_real'], duration)
        scheduled = event['scheduled_monotonic']
        if scheduled is not None and finished is not None and outcome != CANCELLED:
            factor = event['speed_factor'] or 1
            lag = max(0, finished - scheduled)
            completion = self.completion
            completion['replay_events'] += 1
            completion['late'] += int(lag > 0)
            completion['total_real'] += lag
            completion['total_logical'] += lag * factor
            if completion['worst'] is None or lag > completion['max_real']:
                completion['max_real'] = lag
                completion['max_logical'] = lag * factor
                completion['worst'] = dict(
                    event_id=event_id, outcome=outcome, namespace=event['namespace'],
                    resource=event['resource'], trace_timestamp=event['trace_timestamp'],
                    scheduled_monotonic=scheduled, started_monotonic=started,
                    finished_monotonic=finished)
        return None

    def _protocol_error(self, problem, message):
        self.protocol_errors += 1
        return f'usage acknowledgement protocol error ({problem}): event {message.get("event_id")!r}'

    def oldest_pending(self, now):
        if not self.pending:
            return None
        event_id = min(self.pending)
        event = self.pending[event_id]
        reference = event['scheduled_monotonic']
        if reference is None:
            reference = event['enqueued_monotonic']
        age = max(0, now - reference)
        factor = event['speed_factor']
        return dict(event_id=event_id, namespace=event['namespace'], resource=event['resource'],
                    trace_timestamp=event['trace_timestamp'], active=event_id in self.active,
                    age_real=age, age_logical=age * factor if factor else None)

    def summary(self):
        """Bounded report fields; never a list of individual acknowledgements."""
        now = self.monotonic()
        oldest = self.oldest_pending(now)
        if oldest is not None:
            self.max_oldest_pending_age_real = max(self.max_oldest_pending_age_real, oldest['age_real'])
            if oldest['age_logical'] is not None:
                self.max_oldest_pending_age_logical = max(
                    self.max_oldest_pending_age_logical, oldest['age_logical'])
        execution = dict(self.execution, mean_real=(
            self.execution['total_real'] / self.execution['count'] if self.execution['count'] else 0))
        count = self.completion['replay_events']
        completion = dict(copy.deepcopy(self.completion),
                          mean_real=self.completion['total_real'] / count if count else 0,
                          mean_logical=self.completion['total_logical'] / count if count else 0)
        return dict(
            outcomes=dict(self.outcomes),
            acknowledgements=dict(
                registered=self.next_event_id - 1, acknowledged_through=self.acknowledged_through,
                highest_acknowledged=self.highest_acknowledged, gaps=len(self.done_above),
                out_of_order=self.out_of_order, protocol_errors=self.protocol_errors,
                active=len(self.active), max_active=self.max_active,
                pending=len(self.pending), max_pending=self.max_pending,
                oldest_pending=oldest,
                max_oldest_pending_age_real=self.max_oldest_pending_age_real,
                max_oldest_pending_age_logical=self.max_oldest_pending_age_logical),
            execution=execution,
            completion_lag=completion)
