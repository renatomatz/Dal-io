import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from typing import Any, Tuple

from pypfopt import CLA
from pypfopt.plotting import Plotting

from dalio.external import External

from dalio.util import plot_efficient_frontier, \
    plot_covariance, plot_weights


class Figure(External):

    _connection: Any

    def __init__(self):
        super().__init__()
        self.reset()

    def request(self, **kwargs):
        return self._connection

    def plot(self, data, kind=None, **graph_opts):
        raise NotImplementedError()

    def reset(self):
        self._connection = None


class PyPlotGraph(Figure):

    _connection: plt.Figure
    _axes: Any

    def plot(self, x, y=None, kind=None, **graph_opts):
        if kind == "line":
            self._axes.plot(x, y, **graph_opts)
        elif kind == "scatter":
            self._axes.scatter(x, y, **graph_opts)
        else:
            self._axes.plot(x, y, **graph_opts)

    def reset(self):
        self._connection, self._axes = plt.subplots(1)


class PySubplotGraph(PyPlotGraph):

    _rows = int
    _cols = int
    _axes: np.ndarray

    def __init__(self, rows=1, cols=1):
        super().__init__()
        self._rows = rows
        self._cols = cols

    def plot(self, coords, x, y=None, **graph_opts):
        i, j = coords
        if i > self._rows or j > self._cols:
            self._axes[i, j].plot(x, y, **graph_opts)
        else:
            raise ValueError(f"Invalid indexes, this figure has {self._rows} \
                rows and {self._cols} columns")

    def reset(self):
        self._connection, self._axes = plt.subplots(self._rows, self._cols)

    def make(self, i, j=None):
        return SubplotManager(self).make(i, j)


class SubplotManager(PyPlotGraph):

    _rows = int
    _cols = int

    _coords = Tuple[int]
    _fig = PySubplotGraph

    def __init__(self, subplot):
        super().__init__()

        if isinstance(subplot, PySubplotGraph):
            self._fig = subplot
        else:
            TypeError("Subplot managers take in PySubplotGraph instances")

        self._rows = self._fig._rows
        self._cols = self._fig._rows

        self._coords = None

    def plot(self, x, y=None, **graph_opts):
        if self._coords is not None:
            self._fig.plot(self._coords, x, y, **graph_opts)
        else:
            ValueError("this multigraph is uniniciated, build make a subplot \
                    first")

    def reset(self):
        self._fig.reset()

    def make(self, i, j=None):
        if isinstance(i, tuple) and len(i) == 2:
            i, j = i

        if isinstance(i, int) or not isinstance(j, int):
            TypeError("Invalid inputs, either specify a tuple with the desired \
                    coordinates or two integers with the desired coordinates")

        ret = type(self)(self._fig)

        if i > self._rows or j > self._cols:
            ret.set_coords(i, j)
        else:
            raise ValueError(f"Invalid indexes, this figure has {self._rows} \
                rows and {self._cols} columns")

        return ret

    def set_coords(self, i, j):
        if isinstance(i, tuple) and len(i) == 2:
            self._coords = i
        elif isinstance(i, int) and isinstance(j, int):
            self._coords = (i, j,)
        else:
            TypeError("Invalid inputs, either specify a tuple with the desired \
                    coordinates or two integers with the desired coordinates")


class PyPfOptGraph(PyPlotGraph):

    def plot(self, data, **kwargs):
        if isinstance(data, CLA):
            plot_efficient_frontier(data, ax=self._axes, **kwargs)
        elif isinstance(data, (np.ndarray, pd.DataFrame)):
            plot_covariance(data, **kwargs)
        elif isinstance(data, dict):
            plot_weights(data, **kwargs)
        else:
            raise TypeError("Input data cannot be plotted in pypfopt")