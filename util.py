from pathlib import Path


def upper_triangle(lst):
    lst = list(lst)
    for i in range(len(lst)):
        for j in range(i, len(lst)):
            yield (lst[i], lst[j])


def prefix(path):
    prefix_ = Path(path).resolve().parent

    def _relative(subpath):
        return str(prefix_ / subpath)

    return _relative
