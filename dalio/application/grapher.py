import numpy as np

from dalio.base.constants import RETURNS, MAX_EXEDENCE
from dalio.application import Application
from dalio.validator import IS_PD_DF, HAS_COLS, IS_PD_TS, HAS_DIMS, HAS_ATTR
from dalio.util import process_cols


class Grapher(Application):

    def __init__(self):
        super().__init__()

        self._init_source([
            "data_in"
        ])

        self._init_output([
            "data_out"
        ])

    def run(self, **kwargs):
        data = self._source_from("data_in", **kwargs)

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(data, **graph_opts)
        return self._get_output("data_out").request()


class MultiGrapher(Grapher):
    pass


class PandasXYGrapher(Grapher):

    _x: str
    _y: str
    _legend: str

    def __init__(self, x=None, y=None, legend=None):
        super().__init__()

        self._get_source("data_in")\
            .add_desc(IS_PD_DF())

        if isinstance(x, str):
            self._get_source("data_in")\
                .add_desc(HAS_COLS(x))
            self._x = x
        elif x is None:
            self._x = x
        else:
            raise ValueError(f"argument x must be None or of type {str} \
                    not {type(x)}")

        if isinstance(y, (str, list)):
            self._get_source("data_in")\
                .add_desc(HAS_COLS(y))

            self._y = process_cols(y)

        elif y is None:
            self._y = y

        else:
            raise ValueError(f"argument y must be None or of type \
                {str} or {list} not {type(y)}")

        if legend is None or isinstance(legend, str):
            self._legend = legend
        else:
            raise ValueError(f"argument legend must be None or of type \
                {str} not {type(legend)}")
            
    def run(self, **kwargs):
        data = self._source_from("data_in", **kwargs)

        x = data[self._x] if self._x is not None else data.index
        y = data[self._y] if self._y is not None else data

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(
                x=x, 
                y=y, 
                **graph_opts)

        fig = self._get_output("data_out").request()

        if self._legend is not None:
            fig.legend(labels=y.columns, loc=self._legend)

        fig.show()
        return fig


class PandasTSGrapher(PandasXYGrapher):

    def __init__(self, y=None, legend=None):
        super().__init__(y=y, legend=legend)

        self._get_source("data_in")\
            .add_desc(IS_PD_TS())


class ForecastGrapher(Grapher):

    def __init__(self):
        super().__init__()

        self._init_source([
            "forecast_in"
        ])

        self._get_source("data_in")\
            .add_desc(IS_PD_DF())\
            .add_desc(IS_PD_TS())\
            .add_desc(HAS_DIMS(1))

        self._get_source("forecast_in")\
            .add_desc(IS_PD_DF())\
            .add_desc(IS_PD_TS())\
            .add_desc(HAS_DIMS(1))

    def run(self, **kwargs):
        # TODO: Parallelize
        data = self._source_from("data_in", **kwargs)
        forecast = self._source_from("forecast_in", **kwargs)

        y = data.join(forecast, how="outer", sort=True)

        labels = [kwargs.get("data_label", "Data"),
                  kwargs.get("forecast_label", "Forecast")]

        y.columns = labels

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(x=y.index, y=y, **graph_opts)
        fig = self._get_output("data_out").request()
        return fig


class VaRGrapher(Grapher):

    def __init__(self):
        super().__init__()

        self._get_source("data_in")\
            .add_desc(HAS_COLS([RETURNS, MAX_EXEDENCE]))

        # use regex to check for % sign in columns

    def run(self, **kwargs):
        VaR = self._source_from("data_in", **kwargs)

        returns = VaR[RETURNS]
        exedence = VaR[MAX_EXEDENCE]
        VaR.drop([RETURNS, MAX_EXEDENCE], axis=1, inplace=True)

        # x axis is shared for all points
        x = VaR.index

        graph_opts = kwargs.get("graph_opts", {})

        line_opts = graph_opts.copy()
        line_opts.update(
            linewidth=0.5,
            alpha=0.6
        )

        # plot value at risk lines
        self._get_output("data_out").plot(x=x, y=VaR, **graph_opts)

        # add exedence as color
        scatter_opts = graph_opts.copy()
        scatter_opts.update(
            s=0.5,
            c=exedence,
            alpha=0.3
        )

        self._get_output("data_out").plot(
            x=x,
            y=-returns,
            kind="scatter",
            **scatter_opts)

        fig = self._get_output("data_out").request()
        return fig


class LMGrapher(Grapher):

    def __init__(self, legend=None):
        super().__init__()

        self._init_source([
            "linear_model"
        ])

        self._get_source("data_in")\
            .add_desc(IS_PD_DF())

        self._get_source("linear_model")\
            .add_desc(HAS_ATTR(["coef_", "intercept_"]))

        if legend is None or isinstance(legend, str):
            self._legend = legend
        else:
            raise ValueError(f"argument legend must be None or of type \
                {str} not {type(legend)}")

    def run(self, **kwargs):

        data = self._source_from("data_in", **kwargs)
        lm = self._source_from("linear_model", **kwargs)

        linspace = np.arange(data.shape[0]) + 1

        graph_opts = kwargs.get("graph_opts", {})

        for i, col in enumerate(data):

            # graph returns 
            self._get_output("data_out").plot(
                    x=data.index, 
                    y=data[col], 
                    kind="scatter",
                    s=0.5,
                    **graph_opts)

            # graph fitted lm
            self._get_output("data_out").plot(
                    x=data.index, 
                    y=linspace*lm.coef_[i] + lm.intercept_[i], 
                    kind="line",
                    **graph_opts)

        fig = self._get_output("data_out").request()

        if self._legend is not None:
            fig.legend(labels=data.columns, loc=self._legend)

        fig.show()
        return fig
