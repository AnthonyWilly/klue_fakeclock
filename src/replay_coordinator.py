"""Instant-by-instant replay of infrastructure, workloads and usage.

Without HPA or Karpenter, every topology change comes from the trace, so the
two producers can share one schedule: the ordered union of their timestamps,
measured from the common monotonic origin. For each instant:

1. wait for its original deadline (barriers never shift later deadlines);
2. create nodes;
3. apply workload objects, then scale and other non-usage actions;
4. publish every set-usage event of the instant, each carrying the topology
   its workload must show and its completion deadline;
5. wait until every one of those event ids has a terminal acknowledgement;
6. delete workloads, then delete nodes.

Usage of an instant therefore always completes before any later topology
change and before the deletions of its own instant. Independent workloads
still run in parallel in the usage worker. Entries keep their input order and
are all counted, including empty and not applicable ones.

The barrier is the set of event ids, never the highest id or a queue size.
More than ``limit_logical`` logical seconds between an instant and the
completion of one of its usage events fails the replay: the parent raises the
first violation from ``WorkloadManager.check_usage`` and nothing after it runs.
"""
import time


class ReplayCoordinator:
    USAGE_LIMIT_LOGICAL = 30
    POLL_SECONDS = .02

    def __init__(self, infrastructure, workload, limit_logical=USAGE_LIMIT_LOGICAL, poll=POLL_SECONDS):
        self.infrastructure = infrastructure
        self.workload = workload
        self.limit_logical = limit_logical
        self.poll = poll
        self.stats = dict(instants=0, processed_instants=0, usage_instants=0, usage_events=0,
                          barrier_wait_real=0, max_barrier_wait_real=0, max_start_lag_real=0,
                          worst_barrier=None)

    def instants(self):
        """Ordered union of timestamps with each producer's entries in input order."""
        instants = {}
        for role, manager in (('infrastructure', self.infrastructure), ('workload', self.workload)):
            for index, entry in enumerate(manager.data['emulation']):
                bucket = instants.setdefault(entry['timestamp'], dict(infrastructure=[], workload=[]))
                bucket[role].append((index, entry))
        return sorted(instants.items(), key=lambda item: item[0])

    def run(self, clock):
        infrastructure, workload = self.infrastructure, self.workload
        dynamic_nodes = infrastructure.emulation_phase == 'dynamic'
        dynamic_workloads = workload.emulation_phase == 'dynamic'
        instants = self.instants()
        self.stats['instants'] = len(instants)
        for timestamp, entries in instants:
            nodes, workloads = entries['infrastructure'], entries['workload']
            scheduled = clock.deadline(timestamp)
            clock.wait_until(scheduled)
            started = clock.monotonic()
            self.stats['max_start_lag_real'] = max(self.stats['max_start_lag_real'], started - scheduled)
            if dynamic_nodes:
                for _, entry in nodes:
                    infrastructure.apply_entry_nodes(entry, clock, scheduled)
            events = []
            if dynamic_workloads:
                for _, entry in workloads:
                    workload.apply_entry_topology(entry, clock, scheduled)
                for _, entry in workloads:
                    events.extend(workload.publish_entry_usage(entry, clock, scheduled))
            self.barrier(events, clock, timestamp)
            if dynamic_workloads:
                for _, entry in workloads:
                    workload.delete_entry_objects(entry, clock, scheduled)
            if dynamic_nodes:
                for _, entry in nodes:
                    infrastructure.delete_entry_nodes(entry, clock, scheduled)
            for manager, items, dynamic in ((infrastructure, nodes, dynamic_nodes),
                                            (workload, workloads, dynamic_workloads)):
                for index, _ in items:
                    manager.replay_diagnostics.entry(index, timestamp, scheduled, started,
                                                     clock.speed_factor, dynamic)
            self.stats['processed_instants'] += 1

    def barrier(self, events, clock, timestamp):
        """Wait for every event id; deadlines, cancellation and worker health are checked."""
        cancel = getattr(clock, 'cancel', None)
        started = clock.monotonic()
        while True:
            # Raises on the first temporal violation or a dead usage worker.
            self.workload.check_usage()
            if self.workload.usage_done(events):
                break
            if cancel is not None and cancel.is_set():
                raise InterruptedError('replay cancelled while waiting for usage')
            if cancel is not None and clock.wait is time.sleep:
                cancel.wait(self.poll)
            else:
                clock.wait(self.poll)
        if events:
            waited = clock.monotonic() - started
            stats = self.stats
            stats['usage_instants'] += 1
            stats['usage_events'] += len(events)
            stats['barrier_wait_real'] += waited
            if stats['worst_barrier'] is None or waited > stats['max_barrier_wait_real']:
                stats['max_barrier_wait_real'] = waited
                stats['worst_barrier'] = dict(trace_timestamp=timestamp, events=len(events),
                                              wait_real=waited)

    def snapshot(self):
        return dict(self.stats, worst_barrier=dict(self.stats['worst_barrier'] or {}) or None,
                    limit_logical=self.limit_logical)
