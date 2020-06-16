from functools import partial

from pypfopt import CovarianceShrinkage
from pypfopt.expected_returns import return_model as ReturnModel

from dalio.pipe import Pipe
from dalio.validator import STOCK_STREAM, HAS_COLS
from dalio.util import _Builder


class CovShrink(Pipe, _Builder):

    _SHRINKAGE_PRESETS = [
        "shrunk_covariance",
        "ledoit_wolf"
    ]

    frequency: int

    def __init__(self, frequency=252):
        super().__init__()

        self._init_piece([
            "shrinkage"
        ])

        self._source\
            .add_desc(STOCK_STREAM)\

        if isinstance(frequency, int):
            self.frequency = frequency
        else:
            raise TypeError(f"argument frequency must be of type {int} not \
                {type(frequency)}")

    def transform(self, data, **kwargs):
        return self.build_model(data)()

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in CovShrink._SHRINKAGE_PRESETS:
            raise ValueError(f"argument shrinkage must be one of \
                {CovShrink._SHRINKAGE_PRESETS}")

    def build_model(self, data):

        cs = CovarianceShrinkage(data, frequency=self.frequency)
        shrink = self._piece["shrinkage"]

        if shrink["name"] is None:
            ValueError("shrinkage piece 'name' not set")
        elif shrink["name"] == "shrunk_covariance":
            shrink_func = cs.shrunk_covariance
        elif shrink["name"] == "ledoit_wolf":
            shrink_func = cs.ledoit_wolf

        return partial(shrink_func,
                       *shrink["args"],
                       **shrink["kwargs"])


class ExpectedReturns(Pipe, _Builder):

    _RETURN_MODEL_PRESETS = [
        "mean_historical_return",
        "ema_historical_return",
        "james_stein_shrinkage",
        "capm_return"
    ]

    def __init__(self):
        super().__init__()

        self._init_piece([
            "return_model"
        ])

        self._source\
            .add_desc(STOCK_STREAM)\

    def transform(self, data, **kwargs):
        return self.build_model(data)(data)

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in ExpectedReturns._RETURN_MODEL_PRESETS:
            raise ValueError(f"argument return_model must be one of \
                {ExpectedReturns._RETURN_MODEL_PRESETS}")

    def build_model(self, data):
        return_model = self._piece["return_model"]
        return partial(ReturnModel,
                       method=return_model["name"],
                       **return_model["kwargs"])
