"""Pipes that create ARCH Models and derivatives"""
import numpy as np
import pandas as pd

from typing import List

from arch.univariate.base import ARCHModel

# Import mean models
from arch.univariate import (
    ConstantMean,
    ARX
)

# Import variance models
from arch.univariate import (
    ARCH,
    GARCH,
    EWMAVariance
)

# Import distribution models
from arch.univariate import (
    Normal,
    StudentsT,
    SkewStudent
)

from dalio.base.constants import RETURNS, MAX_EXEDENCE, EXPECTED_SHORTFALL
from dalio.util import _Builder
from dalio.pipe import Pipe
from dalio.validator import HAS_DIMS, IS_TYPE, HAS_ATTR


class MakeARCH(Pipe, _Builder):
    """Build arch model and make it based on input data.

    This class allows for the creation of arch models by configuring three
    pieces: the mean, volatility and distribution. These are set after
    initialization through the _Builder interface.

    Attributes:
        _piece (list): see _Builder class.
    """

    _MEAN_DICT = {
        "ConstantMean": ConstantMean,
        "constant": ConstantMean,
        "ARX": ARX,
        "arx": ARX
    }
    _VOL_DICT = {
        "ARCH": ARCH,
        "GARCH": GARCH,
        "EWMAVariance": EWMAVariance,
        "EWMA": EWMAVariance,
        "RiskMetrics": EWMAVariance
    }
    _DIST_DICT = {
        "Normal": Normal,
        "normal": Normal,
        "StudentsT": StudentsT,
        "SkewStudent": SkewStudent
    }
    # TODO: make this agnostic to upper or lower case

    def __init__(self):
        """Initialize instance and pieces"""
        super().__init__()

        self._init_piece([
            "mean",
            "volatility",
            "distribution"
        ])

        self._source\
            .add_desc(HAS_DIMS(1))

    def transform(self, data, **kwargs):
        """Build model with sourced data"""
        return self.build_model(data)

    def copy(self, *args, **kwargs):
        return _Builder.copy(self, *args, **kwargs)

    def build_model(self, data):
        """Build ARCH Model using data, set pieces and their arguments

        Returns:
            A built arch model from the arch package.
        """

        # set mean model piece
        mean = self._piece["mean"]
        am = MakeARCH._MEAN_DICT[mean["name"]](
            data,
            *mean["args"],
            **mean["kwargs"]
        )

        # set volatility model piece
        vol = self._piece["volatility"]
        am.volatility = MakeARCH._VOL_DICT[vol["name"]](
            *vol["args"],
            **vol["kwargs"]
        )

        # set distribution model piece
        dist = self._piece["distribution"]
        am.distribution = MakeARCH._DIST_DICT[dist["name"]](
            *dist["args"],
            **dist["kwargs"]
        )

        return am

    def assimilate(self, model):
        """Assimilate core pieces of an existent ARCH Model.

        Assimilation means setting this model's' pieces in accordance to an
        existing model's pieces. Assimilation is shallow, so only the main
        pieces are assimilated, not their parameters.

        Args:
            model (ARCHModel): Existing ARCH Model.
        """

        self.set_piece(
            "mean",
            type(model).__name__
        )
        self.set_piece(
            "volatility",
            type(model.volatility).__name__
        )
        self.set_piece(
            "distribution",
            type(model.distribution).__name__
        )


class ValueAtRisk(Pipe):
    """Get the value at risk for data based on an ARHC Model

    This takes in an ARCH Model maker, not data, which might be unintuitive,
    yet necessary, as this allows users to modify the ARCH model generating
    these values separately. A useful strategy that allows for this
    is using a pipeline with an arch model as its first input and a
    ValueAtRisk instance as its second layer. This allows us to treat the
    PipeLine as a data input with VaR output and still have control over the
    ARCH Model pieces (given you left a local variable for it behind.)

    Attributes:
        _quantiles (list): list of quantiles to check the value at risk for.
    """

    _quantiles: List[float]

    def __init__(self, quantiles=None):
        """Initialize instance and set quantiles.

        Source requires an ARCHModel input with a .fit() method

        Raises:
            TypeError: if quantiles is neither a float or a list of floats.
        """
        super().__init__()

        self._source\
            .add_desc(IS_TYPE(ARCHModel))\
            .add_desc(HAS_ATTR("fit"))

        if isinstance(quantiles, list):
            if sum([n for n in quantiles if not isinstance(n, float)]) > 0:
                raise TypeError("all quantiles must be floats")
            else:
                self._quantiles = sorted(quantiles)
        elif isinstance(quantiles, float):
            self._quantiles = [quantiles]
        elif quantiles is None:
            self._quantiles = [0.01, 0.05]
        else:
            raise TypeError("quantiles argument must either be a float or \
                    list of floats")

    def transform(self, data, **kwargs):
        """Get values at risk at each quantile and each results maximum
        exedence from the mean.

        The maximum exedence columns tells which quantile the loss is placed
        on. The word "maximum" might be misleading as it is compared to the
        minimum quantile, however, this definition is accurate as the column
        essentially answers the question: "what quantile furthest away from
        the mean does the data exeed?"

        Thank you for the creators of the arch package for the beautiful
        visualizations and ideas!

        Raises:
            ValueError: if ARCH model does not have returns. This is often
                the case for unfitted models. Ensure your graph is complete.
            TypeError: if ARCH model has unsuported distribution parameter.
        """

        # This might seem redundant considering the above FitARCHModel pipe
        # but keep in mind here we need both the fitted results and the model
        if "fit_opts" in kwargs:
            fit_opts = kwargs.pop("fit_opts")
        else:
            fit_opts = {}

        fit_res = data.fit(**fit_opts)

        # prepare returns data
        returns = data._y_original
        while type(returns.columns).__name__ == "MultiIndex":
            returns = returns.droplevel(1, axis=1)
        returns.columns = [RETURNS]

        # start is the earliest index fitted by the algorithm
        start = returns.index[max(0, fit_res._fit_indices[0]-1)]

        if returns is None:
            raise ValueError("Invalid ARCH model: does not have returns")

        forecasts = fit_res.forecast(start=start)

        cond_mean = forecasts.mean[start:]
        cond_var = forecasts.variance[start:]

        dist = type(data.distribution).__name__

        # set ppf parameters according to data distribution
        if dist == "Normal":
            params = None
        elif dist == "StudentsT":
            params = fit_res.params[-1]
        elif dist == "SkewStudent":
            params = fit_res.params[-2:]
        else:
            raise TypeError(f"model has unsuported distribution {dist}")

        q = data.distribution.ppf(self._quantiles, params)

        cols = ["".join([str(n*100), "%"]) for n in self._quantiles]

        value_at_risk = -cond_mean.values \
            - np.sqrt(cond_var).values \
            * q[None, :]

        value_at_risk = pd.DataFrame(
            value_at_risk,
            columns=cols,
            index=cond_var.index
        )

        max_exedence = []
        for idx in value_at_risk.index:
            for i, quantile in enumerate(cols[::-1]):
                if float(returns.loc[idx]) < -value_at_risk.loc[idx, quantile]:
                    max_exedence.append(self._quantiles[-(i+1)])
                    break
            else:
                # this only executes if the whole for loop completes
                max_exedence.append(1)

        max_exedence = pd.DataFrame(
            max_exedence,
            columns=[MAX_EXEDENCE],
            index=cond_var.index
        )

        return pd.concat([returns, value_at_risk, max_exedence],
                         join="inner",
                         axis=1).dropna()

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            quantiles=self._quantiles,
            **kwargs
        )


class ExpectedShortfall(ValueAtRisk):
    """Get expected shortfal for given quantiles

    See base class for more in depth explanation.
    """

    def transform(self, data, **kwargs):
        """Get the value at risk given by an arch model and calculate the
        expected shortfall at given quantiles.
        """
        data = super().transform(data, **kwargs)[[MAX_EXEDENCE, RETURNS]]

        ret = {}
        for quant in self._quantiles:
            # calculate mean returns that exeed a quantile
            ret[quant] = np.mean(data[RETURNS][data[MAX_EXEDENCE] <= quant])

        return pd.DataFrame.from_dict(
                ret,
                orient="index",
                columns=[EXPECTED_SHORTFALL]
        )
