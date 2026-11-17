"""Bounded setup capacity, independent of logical replay deadlines."""
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
import math
import threading

from experiment_clock import validate_speed_factor


def setup_workers(speed_factor):
    return min(8, math.ceil(validate_speed_factor(speed_factor)))


def apply_independent(objects, apply, workers, progress):
    """Serialize each identity; join every worker and surface all failures.

    Only the caller writes progress. Each operation owns its mutable body.
    After a failed operation its identity stops, while independent groups finish.
    """
    groups = OrderedDict()
    for obj in objects:
        meta = obj['metadata']
        key = (meta.get('namespace', 'default'), obj.get('kind', '').lower(), meta['name'])
        groups.setdefault(key, []).append(deepcopy(obj))

    cancelled = threading.Event()

    def run(key, entries):
        completed = 0
        try:
            for obj in entries:
                if cancelled.is_set():
                    break
                apply(obj)
                completed += 1
        except Exception as error:
            return completed, f'{key}: {error}'
        return completed, None

    completed, errors = 0, []
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix='setup') as executor:
        futures = [executor.submit(run, key, entries) for key, entries in groups.items()]
        try:
            for future in as_completed(futures):
                count, error = future.result()
                completed += count
                progress(completed)
                if error:
                    errors.append(error)
        except BaseException:
            cancelled.set()
            for future in futures:
                future.cancel()
            raise
    if errors:
        raise RuntimeError('Setup application failed: ' + '; '.join(errors))
