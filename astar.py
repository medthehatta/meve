from queue import PriorityQueue
import time


class QueueItem:

    def __init__(self, priority, item):
        self.priority = priority
        self.item = item

    def __lt__(self, b):
        return self.priority < b.priority


DONE = object()


def _path_from_predecessors(preds, end):
    if end is DONE:
        return []
    else:
        return _path_from_predecessors(preds, preds.get(end, DONE)) + [end]


def state_astar(
    initial,
    final,
    neighbors_of: callable,
    heuristic: callable,
    trace: callable = None,
):
    fringe = PriorityQueue()
    best_known_cost_to = {}
    best_known_predecessor_to = {}
    transition_list = {}

    best_known_cost_to[initial] = 0
    fringe.put(QueueItem(heuristic(initial, final), initial))

    fringe_size = fringe.qsize()
    trace_timer = time.time()
    trace_interval_seconds = 10

    while fringe_size:
        entry = fringe.get(block=False)
        current = entry.item

        if (
            trace and time.time() - trace_timer > trace_interval_seconds
        ):
            trace(
                f"Search progress: "
                f"heuristic={entry.priority} "
                f"cost={best_known_cost_to[current]} "
                f"[{current}] "
                f"({fringe_size=})"
            )
            trace_timer = time.time()

        if current == final:
            return (
                best_known_cost_to[current],
                [
                    transition_list.get(x) for x in
                    _path_from_predecessors(best_known_predecessor_to, current)[1:]
                ],
            )

        for (transition, n, cost) in neighbors_of(current):
            found_cost = best_known_cost_to[current] + cost
            if (
                n not in best_known_cost_to or
                found_cost < best_known_cost_to[n]
            ):
                best_known_cost_to[n] = found_cost
                best_known_predecessor_to[n] = current
                transition_list[n] = transition
                if n not in [x.item for x in fringe.queue]:
                    fringe.put(QueueItem(found_cost + heuristic(n, final), n))

        fringe_size = fringe.qsize()

    # If we exhausted all our options, there is no route
    raise RuntimeError("No route")
