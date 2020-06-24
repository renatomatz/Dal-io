"""Pipes used in portfolio optimization

While each of these serve a specific function, there are several model
instances that rely on their outputs.
"""
from functools import partial

from pypfopt import CovarianceShrinkage
from pypfopt.expected_returns import return_model as ReturnModel

from dalio.pipe import Pipe
from dalio.validator.presets import STOCK_STREAM
from dalio.util import _Builder


class CovShrink(Pipe, _Builder):
    """Perform Covariance Shrinkage on data

    Builder with a single piece: shirnkage. Shrinkage defines what kind of
    shrinkage to apply on a resultant covariance matrix. If none is set,
    covariance will not be shrunk.

    Attributes:
        frequency (int): data time period frequency
    """

    _SHRINKAGE_PRESETS = [
        "shrunk_covariance",
        "ledoit_wolf"
    ]

    frequency: int

    def __init__(self, frequency=252):
        """Initialize instance.

        Defines source data as a stock stream

        Args:
            frequency (int): data time period frequency.

        Raises:
            TypeError: if frequence argument is not an integer.
        """
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
        """Build model using data get results.

        Returns:
            A covariance matrix
        """
        return self.build_model(data)()

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in CovShrink._SHRINKAGE_PRESETS:
            raise ValueError(f"argument shrinkage must be one of \
                {CovShrink._SHRINKAGE_PRESETS}")

    def build_model(self, data):
        """Builds Covariance Srhinkage object and returns selected shrinkage
        strategy

        Returns:
            Function fitted on the data.
        """

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
    """Get stock's time series expected returns.

    Builder with a single piece: return_model. return_model is what model to
    get the expected returns from.
    """

    _RETURN_MODEL_PRESETS = [
        "mean_historical_return",
        "ema_historical_return",
        "james_stein_shrinkage",
        "capm_return"
    ]

    def __init__(self):
        """Initialize instance.

        Defines data source as a stock stream.
        """
        super().__init__()

        self._init_piece([
            "return_model"
        ])

        self._source\
            .add_desc(STOCK_STREAM)

    def transform(self, data, **kwargs):
        """Builds model using data and gets expected returns from it"""
        return self.build_model(data)(data)

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in ExpectedReturns._RETURN_MODEL_PRESETS:
            raise ValueError(f"argument return_model must be one of \
                {ExpectedReturns._RETURN_MODEL_PRESETS}")

    def build_model(self, data):
        return_model = self._piece["return_model"]
        return partial(ReturnModel,
                       *return_model["args"],
                       method=return_model["name"],
                       **return_model["kwargs"])
