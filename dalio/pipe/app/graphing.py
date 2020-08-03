"""Applications based on graphing input data"""

import numpy as np

from dalio.base.constants import RETURNS, MAX_EXEDENCE
from dalio.pipe.app import TransformerApplication
from dalio.validator import IS_PD_DF, HAS_COLS, IS_PD_TS, HAS_ATTR
from dalio.util import process_cols


class PandasXYGrapher(PipeApplication):
    """Graph data from a pandas dataframe with option of selecting columns
    used as axis
    """

    def __init__(self):
        """Initialize instance.

        Defines data_in source as a pandas dataframe. If x is set, it must
        be a column in this dataframe; same applies to y.

        Args:
            _x (str): name of column to be used for x-axis. Optional. None by
                default. If None, index is used.
            _y (str): name of column to be used for y-axis. Optional. None by
                default. If None, all columns are used.
            _legend (str, None): legend position. Optional. None by default.

        Raises:
            TypeError: if specified x or legend is not a string or if
                specified y is not string or list of strings.
        """
        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())

        self._init_pieces([
            "x",
            "y",
            "legend",
            "plot",
        ])

    def transform(self, data, **kwargs):
        """Get data, separate columns and feed it to data output graph"""

        fig = self.build(data)

        fig.show()

        return fig

    def build(self, data, **kwargs):

        fig = self.interpreter
        
        x = self._pieces("x").name
        if ((isinstance(x, tuple) and not all([col in data for col in x]))
            or (isinstance(x, str) and not x in data)):
            raise ValueError("Specified columns {x} not in data")

        x = process_cols(x)
        x = data[x].to_numpy() if x is not None else data.index
        
        y = self._pieces("y").name
        if ((isinstance(y, tuple) and not all([col in data for col in y]))
            or (isinstance(y, str) and not y in data)):
            raise ValueError("Specified columns {y} not in data")

        y = process_cols(y)
        y = data[y] if y is not None else data

        legend = self._pieces("legend")
        if legend.name is not None:
            fig.set_legend(legend.name, *legend.args, **legend.kwargs)

        figsize = self._pieces("figsize")
        if figsize.name is not None:
            fig.set_figsize(figsize.name, *figsize.args, **figsize.kwargs)

        plot = self._pieces["plot"]

        fig.plot((x, y), 
                *plot.args,
                kind=plot.name if plot.name is not None else "line",
                **plot.kwargs)

        return fig


class VaRGrapher(PipeApplication):
    """Application to visualize Value at Risk"""

    def __init__(self):
        """Initialize instance.

        Defines data_in as having columns named RETURNS and MAX_EXEDENCE.
        """
        super().__init__()

        self._source\
            .add_desc(HAS_COLS([RETURNS, MAX_EXEDENCE]))

        self._init_pieces([
            "plot",
            "var_plot",
        ])

    def transform(self, data, **kwargs):
        """Get value at risk data, plot returns, value at risk lines and
        exceptions at their maximum exedence.

        Thank you for the creators of the arch package for the amazing
        visulaization idea!
        """
        fig = self.build(data)

        fig.show()

        return fig

    def build(self, data, **kwargs):

        fig = self.interpreter

        returns = data[RETURNS]
        exedence = data[MAX_EXEDENCE]
        data.drop([RETURNS, MAX_EXEDENCE], axis=1, inplace=True)

        # x axis is shared for all points
        x = data.index

        var_plot = self._pieces["var_plot"]
        line_opts = {
            linewidth=0.5,
            alpha=0.6
        }
        line_opts.update(var_plot.kwargs) 

        # plot value at risk lines
        fig.plot((x, data), 
                *var_plot.args,
                kind = var_plot.name if var_plot.name is not None else "line",
                **line_opts)

        rets_plot = self._pieces["rets_plot"]
        line_opts = {
            c=exedence,
            alpha=0.5
        }
        line_opts.update(rets_plot.kwargs) 

        # plot value at risk lines
        fig.plot((x, -returns), 
                *rets_plot.args,
                kind = rets_plot.name if rets_plot.name is not None else "scatter",
                **line_opts)

        return fig
