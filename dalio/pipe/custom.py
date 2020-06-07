from dalio.pipe import Pipe


class Custom(Pipe):

    def __init__(self, t_func):
        super().__init__()
        self.t_func = t_func

    def transform(self, data, **kwargs):
        return self.t_func(data)

    def copy(self):
        return type(self)(self.t_func)
