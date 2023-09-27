from cytoolz import curry


class WeightedSeries:

    @classmethod
    def from_record_sequence(cls, seq, value_key, weight_key=None):
        pairs = [
            (record.get(value_key), record.get(weight_key, 1))
            for record in seq
        ]
        values = [v for (v, _) in pairs]
        weights = [w for (_, w) in pairs]
        return cls(values, weights)

    def __init__(self, values, weights=None):
        self._values = values
        self._weights = weights

    @property
    def values(self):
        return self._values

    @property
    def weights(self):
        if self._weights is None:
            return [1]*len(self.values)
        else:
            return self._weights

    def __repr__(self):
        return f"<WeightedSeries (size: {len(self.values)})>"


class WeightedSeriesMetrics:

    @classmethod
    def average(cls, series):
        return sum(series.values) / len(series.values)

    @classmethod
    def weighted_average(cls, series):
        total_cost = sum(
            x*y for (x, y) in zip(series.values, series.weights)
        )
        total_purchased = sum(series.weights)
        return total_cost / total_purchased

    @classmethod
    def maximum(cls, series):
        return max(series.values)

    @classmethod
    def minimum(cls, series):
        return min(series.values)

    @classmethod
    @curry
    def percentile(cls, pct, series):
        if not series:
            return None
        seq = series.values
        ordered = sorted(seq)
        if not ordered:
            return None
        N = len(ordered)
        k_d = (pct/100) * N
        k = int(k_d)
        d = k_d - k
        if k == 0:
            return ordered[0]
        elif k >= N-1:
            return ordered[-1]
        else:
            return ordered[k] + d*(ordered[k+1] - ordered[k])

    @classmethod
    def total_weight(cls, series):
        return sum(series.weights)
