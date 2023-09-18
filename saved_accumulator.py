from contextlib import contextmanager
from functools import reduce
import os
import pickle

from hxxp import DefaultHandlers


UNSET = object()


class SavedAccumulator:

    def __init__(
        self,
        accumulator,
        path,
        dumper=pickle.dump,
        loader=pickle.load,
    ):
        self.accumulator = accumulator
        self.path = path
        self.dumper = dumper
        self.loader = loader

    def accumulate(self, seq):
        seq = iter(seq)
        initial = self.read(default=None)
        if initial is None:
            initial = next(seq)
        final = reduce(self.accumulator, seq, initial)
        return final

    def write(self, data):
        with open(self.path, "wb") as f:
            return self.dumper(data, f)

    def read(self, default=UNSET):
        if not os.path.exist(self.path):
            if default is not UNSET:
                return default
            else:
                raise RuntimeError(f"No file: '{self.path}'")
        with open(self.path, "rb") as f:
            return self.loader(f)

    def accumulate_and_commit(self, seq):
        result = self.accumulate(seq)
        self.write(result)
        return result
