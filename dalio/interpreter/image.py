"""Define classes for image pieces

Images, be it a plot, picture or video are considered external outputs as the
figure itself is not contained in the python session, and must be shown in a
screen or server.
"""
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from pypfopt import CLA

from dalio.base import _Interpreter

from dalio.util import plot_efficient_frontier, \
    plot_covariance, plot_weights


class _Figure(_Interpreter):
    """Base Figure class, implements _Interpreter.

    These serve to implement the basic logic of a figure, and are not limited
    to any specific python package. Python packages should be standardazied
    in these classes to take in these broad commands.

    Attributes:
        _engine: figure object dealt with by this class
    """

    def plot(self, data, *args, kind=None, **kwargs):
        """Plots data on the figure based on implemented plot types.

        Args:
            data: data to be used in the plot.
            kind: kind of plot to be plotted. None by default. Method of form
                plot_{kind} should be implemented in the class with the right
                number of inputs.
            *args, **kwargs: optional graphing options

        Raises:
            AttributeError: if plot kind is not available in this interpreter.
            NotImplementedError: if plot kind is available in the interpreter
                but not implemented in this subclass.
        """
        try:
            plot_func = self.__getattribute__("plot_" + kind)
            if isinstance(data, tuple):
                plot_func(*data, *args, **kwargs)
            else:
                plot_func(data, *args, **kwargs)
        except AttributeError:
            raise AttributeError(f"Unknown plot kind: {kind}")
        except NotImplementedError:
            raise NotImplementedError(f"Unimplemented plot kind: {kind}")

    @property
    def figsize(self):
        """Get figure size"""
        raise NotImplementedError()

    @figsize.setter
    def figsize(self, figsize):
        """Set figure size"""
        raise NotImplementedError()

    def plot_hist(self, x, *args, **kwargs):
        """Plot a histogram"""
        raise NotImplementedError()

    def plot_line(self, x, y, *args, **kwargs):
        """Plot a line"""
        raise NotImplementedError()

    def plot_scatter(self, x, y, *args, **kwargs):
        """Plot a scatter plot"""
        raise NotImplementedError()

    def plot_efficient_frontier(self, cla, *args, **kwargs):
        """Plot an efficient frontier based on a fitted Critical Line
        Algorithm.
        """
        raise NotImplementedError()

    def plot_covariance(self, cov, *args, **kwargs):
        """Plot covariance"""
        raise NotImplementedError()

    def plot_weights(self, weights, *args, **kwargs):
        """Plot a set of weights"""
        raise NotImplementedError()


class _MultiFigure(_Interpreter):
    """Base MultiFigure class

    These serve to implement the basic logic of a plotting multiple figures.
    Python packages should be standardazied in these classes to take in
    these broad commands.
    """

    def __getitem__(self, key):
        """Get a figure from location specified by a key

        Args:
            key (int, tuple): locator for specific figure
        """
        raise NotImplementedError()

    def plot(self, data, *args, loc=None, kind=None, **kwargs):
        """Plots data on the figure.

        Args:
            data: data to be used in the plot.
            loc: coordinates or location of a target graph
            kind: kind of plot to be plotted. None by default. Method of form
                plot_{kind} should be implemented in the class with the right
                number of inputs.
            *args, **kwargs: optional graphing options

        Raises:
            AttributeError: if plot kind is not available in this interpreter.
            NotImplementedError: if plot kind is available in the interpreter
                but not implemented in this subclass.
        """
        try:
            plot_func = self.__getattribute__("plot_" + kind)
            if isinstance(data, tuple):
                plot_func(*data, *args, loc=loc, **kwargs)
            else:
                plot_func(data, *args, loc=loc, **kwargs)
        except AttributeError:
            raise AttributeError(f"Unknown plot kind: {kind}")
        except NotImplementedError:
            raise NotImplementedError(f"Unimplemented plot kind: {kind}")

    @property
    def figsize(self):
        """Get figure size"""
        raise NotImplementedError()

    @figsize.setter
    def figsize(self, figsize):
        """Set figure size"""
        raise NotImplementedError()

    def plot_hist(self, x, *args, loc=None, **kwargs):
        """Plot histogram on specified location"""
        raise NotImplementedError()

    def plot_line(self, x, y, *args, loc=None, **kwargs):
        """Plot line on specified location"""
        raise NotImplementedError()

    def plot_scatter(self, x, y, *args, loc=None, **kwargs):
        """Plot a scatter plot on specified location"""
        raise NotImplementedError()

    def plot_efficient_frontier(self, cla, *args, loc=None, **kwargs):
        """Plot an efficient frontier based on a fitted Critical Line
        Algorithm.
        """
        raise NotImplementedError()

    def plot_covariance(self, cov, *args, loc=None, **kwargs):
        """Plot covariance"""
        raise NotImplementedError()

    def plot_weights(self, weights, *args, loc=None, **kwargs):
        """Plot a set of weights"""
        raise NotImplementedError()


class _PyPlotBaseInterpreter:

    @property
    def figsize(self):
        """Get the figure size in inches"""
        return self.figure.get_size_inches()

    @figsize.setter
    def figsize(self, figsize):
        """Set the figure size in inches"""
        w, h = figsize
        self.figure.set_size_inches(w, h=h)

    @property
    def figure(self):
        """Get the engine's figure"""
        return self.engine[0]

    @property
    def axis(self):
        """Get the engine's axis"""
        return self.engine[1]


class PyPlotGraph(_PyPlotBaseInterpreter, _Figure):
    """Figure from the matplotlib.pyplot package."""

    def __init__(self):
        super().__init__()
        self.clear()

    def clear(self):
        """Set connection and axes to a single figure and axis"""
        self.engine = plt.subplots(1, 1)

    @property
    def engine(self):
        """Get the figure and axes"""
        if self._engine is not None:
            return self._engine

        raise AttributeError("matplotlib figure engine not set")

    @engine.setter
    def engine(self, newengine):
        if isinstance(newengine, tuple):
            fig, ax = newengine

            if isinstance(fig, plt.Figure) and isinstance(ax, plt.Axes):
                self._engine = newengine

            raise TypeError(f"new engine must be a tuple of \
                {type(plt.Figure)} and {type(plt.Axes)}")

    def plot_hist(self, x, *args, **kwargs):
        self.axis.hist(x, *args, **kwargs)

    def plot_line(self, x, y, *args, **kwargs):
        self.axis.plot(x, y, *args, **kwargs)

    def plot_scatter(self, x, y, *args, **kwargs):
        self.axis.scatter(x, y, *args, **kwargs)

    def plot_efficient_frontier(self, cla, *args, **kwargs):
        if isinstance(cla, CLA):
            plot_efficient_frontier(cla, *args, ax=self.axis, **kwargs)
        else:
            raise TypeError(f"cla must be of type {CLA} not \
                {type(cla)}")

    def plot_covariance(self, cov, *args, **kwargs):
        if isinstance(cov, (np.ndarray, pd.DataFrame)):
            plot_covariance(cov, *args, ax=self.axis, **kwargs)
        else:
            raise TypeError(f"cov must be of type {np.array} or \
                {pd.DataFrame} not {type(cov)}")

    def plot_weights(self, weights, *args, **kwargs):
        if isinstance(weights, dict):
            plot_weights(weights, *args, ax=self.axis, **kwargs)
        else:
            raise TypeError(f"weights must be of type {dict} not \
                {type(weights)}")


class PySubplotGraph(_PyPlotBaseInterpreter, _MultiFigure):
    """A matplotlib.pyplot.Figure containing multiple subplots.

    This has a set number of axes, rows and columns which can be accessed
    individually to have data plotted on. These will often be used inside of
    applications that require more than one subplot all contained in the
    same instance.

    Attributes:
        _rows (int): number of rows in the subplot
        _cols (int): number of columns in the subplot
    """

    _rows = int
    _cols = int

    def __init__(self, rows, cols, figsize=None):
        """Initialize instance, check and set rows and columns

        Args:
            rows (int): number of subplot rows
            cols (int): number of subplot columns
        """
        super().__init__()

        self._rows = rows
        self._cols = cols

        self.clear()

    def clear(self):
        """Resets figure and all axes"""
        self.engine = plt.subplots(self._rows, self._cols)

    def __getitem__(self, key):
        return self.axis[key]

    @property
    def engine(self):
        """Get the figure and axes"""
        if self._engine is not None:
            return self._engine

        raise AttributeError("matplotlib figure engine not set")

    @engine.setter
    def engine(self, newengine):
        if isinstance(newengine, tuple):
            fig, axes = newengine

            if isinstance(fig, plt.Figure) \
                    and all([isinstance(ax, plt.Axes) for ax in axes]):
                self._engine = newengine

            raise TypeError(f"new engine must be a tuple of \
                {type(plt.Figure)} and {type(plt.Axes)}")

    def plot_hist(self, x, *args, loc=None, **kwargs):
        self[loc].hist(x, *args, **kwargs)

    def plot_line(self, x, y, *args, loc=None, **kwargs):
        self[loc].plot(x, y, *args, **kwargs)

    def plot_scatter(self, x, y, *args, loc=None, **kwargs):
        self[loc].scatter(x, y, *args, **kwargs)

    def plot_efficient_frontier(self, cla, *args, loc=None, **kwargs):
        if isinstance(cla, CLA):
            plot_efficient_frontier(cla, *args, ax=self[loc], **kwargs)
        else:
            raise TypeError(f"cla must be of type {CLA} not \
                {type(cla)}")

    def plot_covariance(self, cov, *args, loc=None, **kwargs):
        if isinstance(cov, (np.ndarray, pd.DataFrame)):
            plot_covariance(cov, *args, ax=self[loc], **kwargs)
        else:
            raise TypeError(f"cov must be of type {np.array} or \
                {pd.DataFrame} not {type(cov)}")

    def plot_weights(self, weights, *args, loc=None, **kwargs):
        if isinstance(weights, dict):
            plot_weights(weights, *args, ax=self[loc], **kwargs)
        else:
            raise TypeError(f"weights must be of type {dict} not \
                {type(weights)}")

    def make_manager(self, loc):
        """Create a SubPlotManager to manage this instance's subplots"""
        return SubplotManager(self, loc)


class SubplotManager(PyPlotGraph):
    """A manager object for treating a subplot axis like a single plot.

    Applications will often take in single plots and have their functionality
    catered to such. Subplots, while useful, will often be used for specific
    applications. A subplot manager allows you to create multiple subplots
    and pass each one individually onto applications that take a single
    subplot axis and still have access to the underlying figure.
    """

    def __init__(self, subplot, loc):
        """Initialize instance, check and assimilate managed subplot.

        Args:
            subplot (PySubplotGraph): subplot instance containing axis to be
                managed.
            loc (tuple): tuple containing row and column of the subplot
                to manage

        Raises:
            TypeError: if specified subplot is not a PySubplotGraph.
        """
        if not isinstance(subplot, PySubplotGraph):
            TypeError("Subplot managers take in PySubplotGraph instances")

        super().__init__()
        self.engine = subplot.figure, subplot[loc]

    def clear(self):
        self.axis.cla()
