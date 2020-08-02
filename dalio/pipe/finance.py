import numpy as np
import pandas as pd

from typing import List

from dalio.base.constants import (
    RETURNS,
    MAX_EXEDENCE,
    EXPECTED_SHORTFALL,
    MEAN, 
    VARIANCE, 
    RESIDUAL_VARIANCE,
)

from dalio.interpreter import _ARCH, _PortfolioOptimizer

from dalio.pipe import Pipe

from dalio.validator import (
    IS_TYPE,
    HAS_ATTR,
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
            .add_desc(IS_TYPE(_ARCH))\
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

        # prepare returns data
        returns = data.get_orig_data()
        while type(returns.columns).__name__ == "MultiIndex":
            returns = returns.droplevel(1, axis=1)
        returns.columns = [RETURNS]

        # start is the earliest index fitted by the algorithm
        start = returns.index[max(0, data.get_fit_indices()[0]-1)]

        if returns is None:
            raise ValueError("Invalid ARCH model: does not have returns")

        cond_mean = data.forecast_mean(start=start)[start:]
        cond_var = data.forecast_variance(start=start)[start:]

        params = data.get_fit_params()

        # set ppf parameters according to data distribution

        q = data.get_dist_ppf()(self._quantiles, params)

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


class OptimumWeights(Pipe):
    """Get optimum portfolio weights from an efficient frontier or CLA.
    This is also a builder with one piece: strategy. The strategy piece
    refers to the optimization strategy.
    """

    def __init__(self):
        """Initialize instance and strategy builder piece."""
        super().__init__()

        self._source\
            .add_desc(IS_TYPE(_PortfolioOptimizer))

    def transform(self, data, **kwargs):
        """Get efficient frontier, fit it to model and get weights"""
        return data.weights


class GARCHForecast(Pipe):
    """Forecast data based on a fitted GARCH model

    Attributes:
        _start (pd.Timestamp): forecast start time and date.
    """

    _start: pd.Timestamp

    def __init__(self, start=None, horizon=1):
        """Initialize instance and process start attribute.

        Args:
            start: pd.Timestamp or any object that can be converted to one.

        Raises:
            TypeError: if start value cannot be converted to a pd.Timestamp.
        """
        super().__init__(horizon=horizon)

        self._source\
            .add_desc(IS_TYPE(ARCHModel))\
            .add_desc(HAS_ATTR("fit"))

        if isinstance(start, pd.Timestamp) or start is None:
            self._start = start
        else:
            try:
                self._start = pd.Timestamp(start)
            except TypeError:
                raise TypeError("Arg 'start' must be None or covertible \
                    to pd.Timestamp")

    def transform(self, data, **kwargs):
        """Make a mean, variance and residual variance forecast.

        Forecast will be made for the specified horizon starting at the
        specified time. This means that will only get data for the steps
        starting at the specified start date and the steps after it.

        Returns:
            A DataFrame with the columns MEAN, VARIANCE and RESIDUAL_VARIANCE
            for the time horizon after the start date.
        """
        forecaster = data.fit().forecast(
            start=self._start,
            horizon=self.horizon
        )

        concat_dict = {
            MEAN: forecaster.mean.iloc[-1].values(),
            VARIANCE: forecaster.variance.iloc[-1].values(),
            RESIDUAL_VARIANCE: forecaster.residual_variance.iloc[-1].values()
        }

        # concatenate all forecasts into one dataframe
        return pd.concat(concat_dict, axis=1)
