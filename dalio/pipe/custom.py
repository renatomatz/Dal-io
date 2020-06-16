from dalio.pipe import Pipe


class Custom(Pipe):

    def __init__(self, t_func, *args, **kwargs):
        super().__init__()
        self.t_func = t_func
        self._f_args = args
        self._f_kwargs = kwargs

    def transform(self, data, **kwargs):
        return self.t_func(data, *self._f_args, **self._f_kwargs)

    def copy(self):
        return type(self)(self.t_func, *self._f_args, **self._f_kwargs)
