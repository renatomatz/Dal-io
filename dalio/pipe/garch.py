import numpu as np
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
    StudentsT
    SkewStudent
)

from dalio.util import _Builder
from dalio.pipe import Pipe
from dalio.validator import HAS_DIMS, IS_TYPE


class MakeARCH(Pipe, _Builder):

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
        super().__init__()

        self._init_piece([
            "mean",
            "volatility",
            "distribution"
        ])

        self._source\
            .add_desc(HAS_DIMS(1))

    def transform(self, data, **kwargs):
        return self.build_model(data)

    def copy(self):
        ret = type(self)()

        # users shouldn't have access to arg and kwarg keys from pieces
        # attribute, so no need to deep copy those too
        ret._piece = self._piece.copy()
        return ret

    def build_model(self, data):

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
        # shallow assimilation

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


class FitARCHModel(Pipe):

    def __init__(self):
        super().__init__()

        self._source\
            .add_desc(IS_TYPE(ARCHModel))\
            .add_desc(HAS_ATTR("fit"))

    def transform(self, data, **kwargs):
        if "fit_opts" in kwargs:
            fit_opts = kwargs.pop("fit_opts")
        else:
            fit_opts = {}
        return data.fit(**fit_opts)


class ValueAtRisk(Pipe):

    _quantiles: List[float]

    def __init__(self, quantiles=[0.01, 0.05]):

        self._source\
            .add_desc(IS_TYPE(ARCHModel))
            .add_desc(HAS_ATTR{"fit"})

        if isinstance(quantiles, list):
            if sum([n for n in quantiles if not isinstance(n, float)]) > 0:
                raise TypeError("all quantiles must be floats")
            else:
                self._quandiles = sorted(quantiles)
        elif isinstance(quantiles, float):
            self._quandiles = [quantiles]
        else:
            raise TypeError("quantiles argument must either be a float or \
                    list of floats")

    def transform(self, data, **kwargs):
        '''Thank you for the creators of the arch package for the beautiful
        visualizations!
        '''

        # This might seem redundant considering the above FitARCHModel pipe
        # but keep in mind here we need both the fitted results and the model
        if "fit_opts" in kwargs:
            fit_opts = kwargs.pop("fit_opts")
        else:
            fit_opts = {}

        fit_res = data.fit(**fit_opts)

        returns = data._y_original
        first_date = returns.index[0]

        if returns is None:
            raise ValueError("Invalid ARCH model: does not have returns")

        forecasts = fit_res.forecast(start=first_date)

        cond_mean = forecasts.mean
        cond_var = forecasts.variance

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

        cols = ["".join([str(int(n * 100)), "%"]) for n in self._quantiles]

        value_at_risk = -cond_mean.values - np.sqrt(cond_var).values * q[None, :]
        value_at_risk = pd.DataFrame(
                value_at_risk, 
                columns=cols, 
                index=cond_var.index)

        max_exedence = []
        for idx in value_at_risk.index:
            for i, quantile in enumerate(self.cols[::-1]):
                if returns[idx] < -value_at_risk.loc[idx, quantile]:
                    c.append(self._quantiles[-(i+1)])
                    break
                c.append(1)

        max_exedence = pd.DataFrame(
                max_exedence,
                columns="max_exedence",
                index=cond_var.index)

        return pd.concat(returns, value_at_risk, max_exedence)
