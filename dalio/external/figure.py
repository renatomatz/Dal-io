import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from typing import Any, Dict, Tuple

from pypfopt.plotting import plot_covariance, plot_efficient_frontier, \
    plot_weights

from dalio.external import External


class Figure(External): 
    
    _connection: Any

    def __init__(self):
        super().__init__()
        self._reset()

    def request(self, **kwargs):
        return self._connection

    def plot(self, data, **graph_opts):
        raise NotImplementedError()

    def reset(self):
        self._connection = None


class PyPlotGraph(Figure):

    _connection: plt.Figure

    def plot(self, data, **graph_opts):
        self._connection.plot(data, **graph_opts)

    def reset(self):
        self._connection = plt.Figure()


class PySubplotGraph(PyPlotGraph):

    _rows = int
    _cols = int
    _axes: np.ndarray

    def __init__(self, rows=1, cols=1):
        super().__init__()
        self._rows = rows
        self._cols = cols

    def plot(self, i, j, data, **graph_opts):
        if i > self._rows or j > self._cols:
            self._axes[i, j].plot(data, **graph_opts)
        else:
            raise ValueError(f"Invalid indexes, this figure has {slef._rows} \
                rows and {self._cols} columns")

    def reset(self):
        self._connection, self._axes = plt.subplots(self._rows, self._cols)


class PyPfOptGraph(PyPlotGraph):
    
    def plot(self, data, **kwargs):
        if isisntance(data, CLA):
            plot_efficient_frontier(data, **kwargs).set_figure(self._connection)
        elif isinstance(data, (np.ndarray, pd.DataFrame)):
            plot_covariance(data, **kwargs).set_figure(self._connection)
        elif isinstance(data, dict):
            plot_weights(data, **kwargs).set_figure(self._connection)
        else:
            raise TypeError("Input data cannot be plotted in pypfopt")
