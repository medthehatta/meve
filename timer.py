import time


class Timer:

    def __init__(self, trace=False):
        self.events = [(time.perf_counter(), "start")]
        self.trace = trace

    def checkpoint(self, description=None):
        event = (time.perf_counter(), description or "checkpoint")
        self.events.append(event)
        if self.trace:
            print(event)
        return event

    def deltas(self):
        if len(self.events) < 2:
            raise ValueError("Not enough events for deltas")

        (prev, prev_desc) = self.events[0]
        for (tstamp, desc) in self.events[1:]:
            yield (tstamp - prev, prev_desc)
            prev = tstamp
            prev_desc = desc

