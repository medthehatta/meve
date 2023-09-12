import operator


class Delayed:

    @classmethod
    def solo(cls, value):
        raise NotImplementedError()

    @classmethod
    def _evaluate_cls(cls, data):
        raise NotImplementedError()

    def evaluate(self):
        return self._evaluate(self)

    @classmethod
    def _evaluate(cls, data, **kwargs):
        if isinstance(data, cls):
            return cls._evaluate_cls(data, **kwargs)

        elif isinstance(data, (tuple, list)):
            return type(data)([cls._evaluate(d, **kwargs) for d in data])

        elif isinstance(data, dict):
            return type(data)({k: cls._evaluate(v, **kwargs) for (k, v) in data.items()})

        elif isinstance(data, _Delayed):
            inner_args = [cls._evaluate(arg, **kwargs) for arg in data.args]
            inner_kwargs = {k: cls._evaluate(v, **kwargs) for (k, v) in data.kwargs}
            return cls._evaluate(data.func(*inner_args, **inner_kwargs), **kwargs)

        else:
            return data

    @classmethod
    def delayed(cls, func):
        def _delayed(*args, **kwargs):
            return cls.solo(_Delayed(func, *args, **kwargs))
        return _delayed

    def __abs__(self, *args):
        return self.delayed(operator.abs)(self, *args)

    def __add__(self, *args):
        return self.delayed(operator.add)(self, *args)

    def __and__(self, *args):
        return self.delayed(operator.and_)(self, *args)

    def __contains__(self, *args):
        return self.delayed(operator.contains)(self, *args)

    def __countOf__(self, *args):
        return self.delayed(operator.countOf)(self, *args)

    def __eq__(self, *args):
        return self.delayed(operator.eq)(self, *args)

    def __floordiv__(self, *args):
        return self.delayed(operator.floordiv)(self, *args)

    def __ge__(self, *args):
        return self.delayed(operator.ge)(self, *args)

    def __getitem__(self, *args):
        return self.delayed(operator.getitem)(self, *args)

    def __gt__(self, *args):
        return self.delayed(operator.gt)(self, *args)

    def __iadd__(self, *args):
        return self.delayed(operator.iadd)(self, *args)

    def __iand__(self, *args):
        return self.delayed(operator.iand)(self, *args)

    def __iconcat__(self, *args):
        return self.delayed(operator.iconcat)(self, *args)

    def __ifloordiv__(self, *args):
        return self.delayed(operator.ifloordiv)(self, *args)

    def __ilshift__(self, *args):
        return self.delayed(operator.ilshift)(self, *args)

    def __imatmul__(self, *args):
        return self.delayed(operator.imatmul)(self, *args)

    def __imod__(self, *args):
        return self.delayed(operator.imod)(self, *args)

    def __imul__(self, *args):
        return self.delayed(operator.imul)(self, *args)

    def __index__(self, *args):
        return self.delayed(operator.index)(self, *args)

    def __indexOf__(self, *args):
        return self.delayed(operator.indexOf)(self, *args)

    def __inv__(self, *args):
        return self.delayed(operator.inv)(self, *args)

    def __invert__(self, *args):
        return self.delayed(operator.invert)(self, *args)

    def __ior__(self, *args):
        return self.delayed(operator.ior)(self, *args)

    def __ipow__(self, *args):
        return self.delayed(operator.ipow)(self, *args)

    def __irshift__(self, *args):
        return self.delayed(operator.irshift)(self, *args)

    def __is__(self, *args):
        return self.delayed(operator.is_)(self, *args)

    def __is_not__(self, *args):
        return self.delayed(operator.is_not)(self, *args)

    def __isub__(self, *args):
        return self.delayed(operator.isub)(self, *args)

    def __itruediv__(self, *args):
        return self.delayed(operator.itruediv)(self, *args)

    def __ixor__(self, *args):
        return self.delayed(operator.ixor)(self, *args)

    def __le__(self, *args):
        return self.delayed(operator.le)(self, *args)

    def __lshift__(self, *args):
        return self.delayed(operator.lshift)(self, *args)

    def __lt__(self, *args):
        return self.delayed(operator.lt)(self, *args)

    def __matmul__(self, *args):
        return self.delayed(operator.matmul)(self, *args)

    def __mod__(self, *args):
        return self.delayed(operator.mod)(self, *args)

    def __mul__(self, *args):
        return self.delayed(operator.mul)(self, *args)

    def __ne__(self, *args):
        return self.delayed(operator.ne)(self, *args)

    def __neg__(self, *args):
        return self.delayed(operator.neg)(self, *args)

    def __not__(self, *args):
        return self.delayed(operator.not_)(self, *args)

    def __or__(self, *args):
        return self.delayed(operator.or_)(self, *args)

    def __pos__(self, *args):
        return self.delayed(operator.pos)(self, *args)

    def __pow__(self, *args):
        return self.delayed(operator.pow)(self, *args)

    def __rshift__(self, *args):
        return self.delayed(operator.rshift)(self, *args)

    def __sub__(self, *args):
        return self.delayed(operator.sub)(self, *args)

    def __truediv__(self, *args):
        return self.delayed(operator.truediv)(self, *args)

    def __truth__(self, *args):
        return self.delayed(operator.truth)(self, *args)


class _Delayed:

    def __init__(self, func, *args, **kwargs):
        self.func = func
        self.args = args
        self.kwargs = kwargs

    def __repr__(self):
        args = ", ".join(
            [f"{arg}" for arg in self.args] +
            [f"{key}={value}" for (key, value) in self.kwargs.items()]
        )
        return f"{self.func.__name__}({args})"
