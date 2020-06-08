from dalio.application import Application
from dalio.validator import IS_PD_DF, HAS_COLS, IS_PD_TS, HAS_DIMS


class Grapher(Application):

    def __init__(self, extra_source=list(), extra_output=list()):
        super().__init__()

        self._init_source([
            "data_in"
        ] + extra_source)

        self._init_output([
            "data_out"
        ] + extra_output)

    def run(self, **kwargs):
        data = self._source_from("data_in", **kwargs)

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(data, **graph_opts)
        self._get_output("data_out").request()


class MultiGrapher(Grapher):
    pass


class PandasGrapher(Grapher):

    def __init__(self, x=None, y=None):
        super().__init__()

        self._get_source("data_in")\
            .add_desc(IS_PD_DF())

        if x is None or isinstance(x, str):
            self._get_source("data_in")\
                .add_desc(HAS_COLS(x))
            self._x = x
        else:
            raise ValueError(f"argument x must be None or of type {str} \
                    not {type(x)}")

        if y is None or isinstance(y, str):
            self._get_source("data_in")\
                .add_desc(HAS_COLS(y))
            self._y = y
        else:
            raise ValueError(f"argument y must be None or of type {str} \
                    not {type(y)}")

    def run(self, **kwargs):
        data = self._source_from("data_in", **kwargs)

        if self._x is not None:
            data.set_index(self._x, inplace=True)

        if self._y is not None:
            data = data[self._y]

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(data, **graph_opts)
        fig = self._get_output("data_out").request()
        return fig


class PandasTSGrapher(PandasGrapher):

    def __init__(self, y=None):
        super().__init__(y=y)

        self._get_source("data_in")\
            .add_desc(IS_PD_TS())


class ForecastGrapher(Grapher):

    def __init__(self):
        super().__init__(extra_source=["forecast_in"])

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

        joined = data.join(forecast, how="outer", sort=True)

        labels = [kwargs.get("data_label", "Data"), 
                  kwargs.get("forecast_label", "Forecast")]
        joined.columns = labels

        graph_opts = kwargs.get("graph_opts", {})

        self._get_output("data_out").plot(joined, **graph_opts)
        fig = self._get_output("data_out").request()
        return fig
