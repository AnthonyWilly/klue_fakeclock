import pytest

from experiment_clock import ExperimentClock, ReplayDiagnostics, replay_entries, validate_speed_factor


class FakeTime:
    def __init__(self, now=100):
        self.now = now
        self.waits = []

    def monotonic(self):
        return self.now

    def wait(self, seconds):
        self.waits.append(seconds)
        self.now += seconds

    def clock(self, factor=1):
        return ExperimentClock(factor, self.monotonic, self.wait, lambda: 1700000000).start()


@pytest.mark.parametrize('factor', [1, 2, 5, 1.5])
def test_deadlines_and_tail(factor):
    time = FakeTime()
    clock = time.clock(factor)
    assert clock.origin_unix == 1700000000
    assert clock.deadline(30) == 100 + 30 / factor
    clock.wait_until(clock.deadline(30))
    clock.tail()
    assert time.now == pytest.approx(100 + 45 / factor)
    with pytest.raises(RuntimeError, match='already started'):
        clock.start()


def test_unstarted_clock_and_early_wakeup():
    time = FakeTime()
    clock = ExperimentClock(2, time.monotonic, lambda seconds: time.wait(min(1, seconds)))
    with pytest.raises(RuntimeError, match='not started'):
        clock.deadline(10)
    clock.start()
    clock.wait_until(clock.deadline(10))
    assert time.now == 105
    assert len(time.waits) == 5


@pytest.mark.parametrize('factor', ['bad', None, float('nan'), float('inf'), '-inf', 0, -1, .9, 5001, 1e300])
def test_invalid_factors(factor):
    with pytest.raises(ValueError):
        validate_speed_factor(factor)


@pytest.mark.parametrize('mode', ['karpenter', 'hpa', 'development'])
def test_legacy_modes_only_at_one(mode):
    assert validate_speed_factor(1, **{mode: True}) == 1
    with pytest.raises(ValueError):
        validate_speed_factor(2, **{mode: True})


def test_interval_boundary():
    assert validate_speed_factor(5000) == 5000
    assert ExperimentClock(5000).real_duration(5) == .001


def test_action_diagnostics_and_snapshot_isolation():
    time = FakeTime()
    clock = time.clock(5)
    stats = ReplayDiagnostics(1)

    def fail():
        time.wait(3)
        raise RuntimeError('API failed')

    stats.apply(clock, 90, {'action': 'test'}, fail)
    snapshot = stats.snapshot()
    assert snapshot['attempted_actions'] == snapshot['failed_actions'] == 1
    assert snapshot['completed_actions'] == 0
    assert snapshot['actions'][0] == dict(context={'action': 'test'}, scheduled=90,
                                        started=100, finished=103, trace_timestamp=-50,
                                        lag_real=10, lag_logical=50,
                                        completed=False, error='API failed')
    snapshot['actions'].clear()
    assert len(stats.snapshot()['actions']) == 1


def test_late_entries_preserve_order_and_tail_follows_completion():
    time = FakeTime()
    clock = time.clock(2)
    stats = ReplayDiagnostics(3)
    entries = [{'timestamp': ts, 'id': i} for i, ts in enumerate([4, 4, 8])]
    seen = []
    for entry, _ in replay_entries(entries, clock, stats, True):
        seen.append((entry['id'], time.now))
        time.wait(3)
    assert seen == [(0, 102), (1, 105), (2, 108)]
    assert time.now == 118.5
    snapshot = stats.snapshot()
    assert snapshot['processed_entries'] == 3
    assert snapshot['final_timestamp'] == 8
    assert snapshot['late_events'] == 2
    assert snapshot['max_lag_real'] == 4
    assert snapshot['total_lag_real'] == 7
    assert snapshot['max_lag_logical'] == 8
    assert snapshot['total_lag_logical'] == 14
    assert snapshot['worst_event']['index'] == 2
