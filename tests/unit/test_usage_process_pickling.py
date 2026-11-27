import pickle
import threading
from multiprocessing import context
from multiprocessing.reduction import ForkingPickler

from workload.usage.usage_manager import UsageManager


def send_to_child(process):
    """Serialize the process as forkserver/spawn do when starting it."""
    context.set_spawning_popen(object())
    try:
        data = ForkingPickler.dumps(process)
    finally:
        context.set_spawning_popen(None)
    return pickle.loads(data)


def test_usage_manager_starts_without_fork():
    # Python 3.14 starts processes with forkserver on Linux, which pickles them.
    manager = UsageManager(None, reconcile_interval_seconds=1, event_workers=2)
    manager.event_scope.deadline = 10
    child = send_to_child(manager)

    assert child.reconcile_interval_seconds == 1
    assert child.event_workers == 2
    assert child.state_lock is not manager.state_lock
    with child.state_lock:
        with child.state_lock:
            pass
    assert isinstance(child.event_scope, threading.local)
    assert getattr(child.event_scope, 'deadline', None) is None
    # The parent keeps its own synchronization state.
    assert manager.event_scope.deadline == 10
