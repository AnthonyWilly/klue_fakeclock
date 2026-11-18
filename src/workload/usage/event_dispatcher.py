"""Concurrency primitives of the usage worker.

``EventDispatcher`` runs the tasks of one identity, a ``(namespace, workload)``
pair, strictly in submission order and never two at once, while tasks of
independent identities run on a small fixed pool of threads. Ordering is FIFO
per identity, not global: an event of one workload may finish before an earlier
event of another workload.

``FairExecutor`` bounds the API mutations issued by every task together. Each
submitter group has its own queue and groups are served round-robin, so a
workload with many pods cannot delay a small workload behind all of its writes.

Event tasks block waiting for their mutations, so the two pools are separate:
a task never waits on work queued behind itself. All threads are daemons and
every stop is bounded in real time; a hung call cannot keep the process alive.
"""
from collections import deque
from concurrent.futures import Future
import threading
import time


class _Task:
    __slots__ = ('key', 'run', 'on_cancel', 'lock', 'claimed')

    def __init__(self, key, run, on_cancel):
        self.key = key
        self.run = run
        self.on_cancel = on_cancel
        self.lock = threading.Lock()
        self.claimed = False

    def claim(self):
        """Return True exactly once: whoever claims reports the terminal outcome."""
        with self.lock:
            if self.claimed:
                return False
            self.claimed = True
            return True

    def cancel(self):
        """Report cancellation unless an outcome was already claimed."""
        if not self.claim():
            return False
        if self.on_cancel is not None:
            self.on_cancel()
        return True


class EventDispatcher:
    """Serialize work per identity and run independent identities concurrently.

    ``submit(key, run, on_cancel)`` queues ``run(claim)``. ``claim()`` returns
    True once; a task reports its terminal outcome only when it wins the claim.
    A task abandoned by ``cancel`` is claimed by the dispatcher, which calls
    ``on_cancel`` instead, so an abandoned task can never also report success.
    """

    def __init__(self, workers, name='usage-event', monotonic=time.monotonic):
        if workers < 1:
            raise ValueError('event dispatcher needs at least one worker')
        self.workers = workers
        self.monotonic = monotonic
        self.condition = threading.Condition()
        self.queues = {}
        # Identities with queued tasks and no active task, served round-robin.
        self.ready = deque()
        self.active = {}
        self.buffered = 0
        self.closed = False
        self.failure = None
        self.counters = dict(submitted=0, finished=0, cancelled=0, max_active=0, max_buffered=0)
        self.threads = [threading.Thread(target=self._work, name=f'{name}-{index}', daemon=True)
                        for index in range(workers)]
        for thread in self.threads:
            thread.start()

    def submit(self, key, run, on_cancel=None):
        task = _Task(key, run, on_cancel)
        with self.condition:
            if self.closed:
                raise RuntimeError('event dispatcher is closed')
            tasks = self.queues.setdefault(key, deque())
            tasks.append(task)
            if len(tasks) == 1 and key not in self.active:
                self.ready.append(key)
            self.buffered += 1
            self.counters['submitted'] += 1
            self.counters['max_buffered'] = max(self.counters['max_buffered'], self.buffered)
            self.condition.notify()
        return task

    def _next(self):
        with self.condition:
            while not self.ready:
                if self.closed:
                    return None
                self.condition.wait()
            key = self.ready.popleft()
            tasks = self.queues[key]
            task = tasks.popleft()
            if not tasks:
                del self.queues[key]
            self.buffered -= 1
            self.active[key] = task
            self.counters['max_active'] = max(self.counters['max_active'], len(self.active))
            return task

    def _done(self, task):
        with self.condition:
            del self.active[task.key]
            self.counters['finished'] += 1
            if task.key in self.queues:
                # Rotate to the tail: other identities get a turn first.
                self.ready.append(task.key)
            self.condition.notify_all()

    def _work(self):
        while True:
            task = self._next()
            if task is None:
                return
            try:
                task.run(task.claim)
            except BaseException as error:
                with self.condition:
                    self.failure = self.failure or error
            finally:
                self._done(task)

    # ------------------------------------------------------------ observation

    def check(self):
        """Raise when a task escaped its own error handling."""
        if self.failure is not None:
            raise RuntimeError(f'usage dispatcher worker failed: {self.failure!r}') from self.failure

    @property
    def idle(self):
        with self.condition:
            return not self.buffered and not self.active

    def wait_idle(self, timeout):
        with self.condition:
            return self.condition.wait_for(
                lambda: not self.buffered and not self.active, timeout)

    def queued(self, key):
        with self.condition:
            return len(self.queues.get(key, ()))

    def stats(self):
        with self.condition:
            return dict(self.counters, workers=self.workers, active=len(self.active),
                        buffered=self.buffered)

    # ------------------------------------------------------------ termination

    def _join(self, deadline):
        for thread in self.threads:
            thread.join(max(0, deadline - self.monotonic()))
        return not any(thread.is_alive() for thread in self.threads)

    def close(self, timeout):
        """Stop admission after accepted work finished; return True when joined."""
        with self.condition:
            self.closed = True
            self.condition.notify_all()
        return self._join(self.monotonic() + timeout)

    def cancel(self, timeout):
        """Stop admission, cancel queued tasks and abandon tasks still running.

        Running tasks get ``timeout`` real seconds to finish on their own. A task
        still running after that is reported cancelled; its thread is a daemon
        and its eventual result is discarded. Return True when every thread stopped.
        """
        deadline = self.monotonic() + timeout
        with self.condition:
            self.closed = True
            dropped = [task for tasks in self.queues.values() for task in tasks]
            self.queues.clear()
            self.ready.clear()
            self.buffered = 0
            self.condition.notify_all()
        for task in dropped:
            self._cancel(task)
        with self.condition:
            self.condition.wait_for(lambda: not self.active, max(0, deadline - self.monotonic()))
            abandoned = list(self.active.values())
        for task in abandoned:
            self._cancel(task)
        return self._join(deadline)

    def _cancel(self, task):
        if task.cancel():
            with self.condition:
                self.counters['cancelled'] += 1


class FairExecutor:
    """Bounded thread pool whose queued work is served round-robin by group.

    ``submit`` mirrors ``ThreadPoolExecutor.submit`` with an optional ``group``.
    At most ``workers`` calls run at once across all groups.
    """

    def __init__(self, workers, name='usage-write', monotonic=time.monotonic):
        if workers < 1:
            raise ValueError('executor needs at least one worker')
        self.workers = workers
        self.monotonic = monotonic
        self.condition = threading.Condition()
        self.groups = {}
        self.ready = deque()
        self.closed = False
        self.running = 0
        self.max_running = 0
        self.threads = [threading.Thread(target=self._work, name=f'{name}-{index}', daemon=True)
                        for index in range(workers)]
        for thread in self.threads:
            thread.start()

    def submit(self, fn, /, *args, group=None, **kwargs):
        future = Future()
        with self.condition:
            if self.closed:
                raise RuntimeError('executor is closed')
            calls = self.groups.setdefault(group, deque())
            calls.append((future, fn, args, kwargs))
            if len(calls) == 1:
                self.ready.append(group)
            self.condition.notify()
        return future

    def _next(self):
        with self.condition:
            while not self.ready:
                if self.closed:
                    return None
                self.condition.wait()
            group = self.ready.popleft()
            calls = self.groups[group]
            call = calls.popleft()
            if calls:
                self.ready.append(group)
            else:
                del self.groups[group]
            self.running += 1
            self.max_running = max(self.max_running, self.running)
            return call

    def _work(self):
        while True:
            call = self._next()
            if call is None:
                return
            future, fn, args, kwargs = call
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        future.set_result(fn(*args, **kwargs))
                    except BaseException as error:
                        future.set_exception(error)
            finally:
                with self.condition:
                    self.running -= 1

    def stats(self):
        with self.condition:
            return dict(workers=self.workers, running=self.running, max_running=self.max_running,
                        queued=sum(len(calls) for calls in self.groups.values()))

    def shutdown(self, timeout):
        """Cancel queued calls and join workers in bounded time; True when joined."""
        with self.condition:
            self.closed = True
            dropped = [call for calls in self.groups.values() for call in calls]
            self.groups.clear()
            self.ready.clear()
            self.condition.notify_all()
        for future, *_ in dropped:
            future.cancel()
        deadline = self.monotonic() + timeout
        for thread in self.threads:
            thread.join(max(0, deadline - self.monotonic()))
        return not any(thread.is_alive() for thread in self.threads)
