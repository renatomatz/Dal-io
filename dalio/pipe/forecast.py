import pandas as pd
import datetime

from typing import Union

from arch.base import ARCHModel

from dalio.base.constants import MEAN, VARIANCE, RESIDUAL_VARIANCE
from dalio.pipe import Pipe
                                 

class Forecast(Pipe):

    _start: Union[pd.Timestamp, datetime.datetime]
    _horizon: int

    def __init__(self, start=None, horizon=100):
        super().__init__()

        if isinstance(start, (pd.Timestamp, datetime.datetime)):
            self._start = start 
        else:
            raise TypeError("Invalid start type")

        if isinstance(horizon, int):
            self._horizon = horizon 
        else:
            raise TypeError("Invalid horizon type")

    def transform(self, data, **kwargs):
        return data.forecast(
            start=self._start, 
            horizon=self._horizon,
            **kwargs)


class GARCHForecast(Forecast):

    def __init__(self, start=None, horizon=100):
        super().__init__(start=start, horizon=horizon)

        self._init_piece([
            "stat"
        ])

        self._source\
            .add_desc(IS_TYPE(ARCHModel))

    def transform(self, data, **kwargs):

        forecaster = data.forecast(
            start=start, 
            horizon=self.horizon,
            **kwargs)
        
        # concatenate all forecasts into one dataframe
        # TODO: understant how this words
        return pd.concat(
                {
                    MEAN: forecaster.mean.iloc[-1].values(), 
                    VARIANCE: forecaster.variance.iloc[-1].values(),
                    RESIDUAL_VARIANCE: forecaster.residual_variance\
                            .iloc[-1].values()
                },
                axis=1
                )

    def check_name(self, param, name):
        super().check_name(param, name)
        if name not in GARCHForecast._STATISTICS:
            raise ValueError(f"strategy piece must be one of \
                    {GARCHForecast._STATISTICS}")
