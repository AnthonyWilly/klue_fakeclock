"""Experiment sampling intervals, serialized at Prometheus millisecond precision."""
import math
from experiment_clock import validate_speed_factor


def duration(seconds):
    milliseconds = seconds * 1000
    if not math.isfinite(milliseconds) or milliseconds < 1 or not math.isclose(milliseconds, round(milliseconds), rel_tol=0, abs_tol=1e-7):
        raise ValueError('sampling interval must be representable in whole milliseconds')
    milliseconds = round(milliseconds)
    return f'{milliseconds // 1000}s' if milliseconds % 1000 == 0 else f'{milliseconds}ms'


class SamplingIntervals:
    def __init__(self, speed_factor=1):
        factor = validate_speed_factor(speed_factor)
        self.reconcile = 5 / factor
        self.kwok = round(15000 / factor) / 1000
        self.ksm = self.query = round(30000 / factor) / 1000
        duration(self.kwok)
        duration(self.ksm)

    def snapshot(self):
        return dict(reconcile=self.reconcile, kwok=self.kwok, kube_state_metrics=self.ksm, query=self.query)


def configure_ksm(monitor, interval):
    """Overlay only the main metrics endpoint; preserve authentication and self scrape."""
    import copy
    result = copy.deepcopy(monitor)
    endpoints = [endpoint for endpoint in result['spec']['endpoints'] if endpoint['port'] == 'https-main']
    if len(endpoints) != 1:
        raise ValueError('expected one kube-state-metrics https-main endpoint')
    endpoints[0].update(interval=duration(interval), scrapeTimeout=duration(min(10, interval)))
    return result


def parse_duration(value):
    """Read Prometheus duration strings, including composite units in /targets."""
    import re
    units = {'ms': .001, 's': 1, 'm': 60, 'h': 3600, 'd': 86400, 'w': 604800, 'y': 31536000}
    parts = re.findall(r'(\d+)(ms|s|m|h|d|w|y)', value)
    if not parts or ''.join(number + unit for number, unit in parts) != value:
        raise ValueError(f'invalid Prometheus duration: {value}')
    return round(sum(int(number) * units[unit] for number, unit in parts), 3)
