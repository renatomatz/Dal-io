"""Custom transformation"""
from functools import partial

from dalio.pipe import Pipe


class Custom(Pipe):
    """Custom transformation for simple operations.

    These are very useful for simple operations or for testing, as no
    additional class definitions or understanding of the documentation is
    requred.

    Attributes:
        t_func (callable): function to transform data with preset arguments.
        _args: arguments to be passed onto the function at execution time.
        _kwargs: arguments to be passed onto the function at execution
            time.
    """

    t_func: callable

    def __init__(self, t_func, *args, **kwargs):
        """Initialize instance.

        Args:
            t_func (callable): function that takes in data and transorms it
                as needed.
            *args: arguments to be passed onto the function at execution time.
            **kwargs: arguments to be passed onto the function at execution
                time.
        """
        super().__init__()
        self.t_func = t_func
        self._args = args
        self._kwargs = kwargs

    def transform(self, data, **kwargs):
        return self.t_func(data, *self._args, **self._kwargs)

    def copy(self, *args, **kwargs):
        return super().copy(
            self.t_func, 
            *self._args,
            *args, 
            **self._kwargs,
            **kwargs
        )
