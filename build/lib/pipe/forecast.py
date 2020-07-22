"""Transformations makes forecasts based on data"""
import pandas as pd

from arch.univariate.base import ARCHModel

from dalio.base.constants import MEAN, VARIANCE, RESIDUAL_VARIANCE
from dalio.pipe import Pipe
from dalio.validator import IS_TYPE, HAS_ATTR


class Forecast(Pipe):
    """Generalized forecasting class.

    This should be used mostly for subclassing or very generic forecasting
    interfaces.

    Attributes:
        horizon (int): how many steps ahead to forecast
    """

    horizon: int

    def __init__(self, horizon=10):
        """Initialize instance and set horizon

        Args:
            horizon (int): steps ahead to forecast. 10 by default.

        Raises:
            TypeError: if horizon is not an int.
        """
        super().__init__()

        if isinstance(horizon, int):
            self.horizon = horizon
        else:
            raise TypeError(f"Horizon should be an int, not {type(horizon)}")

    def transform(self, data, **kwargs):
        """Return forecast of data"""
        return data.forecast(horizon=self.horizon)


class GARCHForecast(Forecast):
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
