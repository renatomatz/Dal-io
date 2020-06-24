"""Resample data to a different period"""
from typing import Callable, Any, Iterable

import numpy as np

from dalio.pipe import Pipe
from dalio.validator import IS_PD_TS


class Period(Pipe):
    """Resample input time series data to a different period

    Attributes:
        agg_func (callable): function to aggregate data to one period.
            Default set to np.mean.
        _period (str): period to resample data to. Can be either daily,
            monthly, quarterly or yearly.
    """

    agg_func: Callable[[Iterable], Any]
    _period: str

    def __init__(self, period=None, agg_func=np.mean):
        """Initialize instance.

        Describes source data as a time series.
        Check that provided period is valid to some preset standards.

        Raises:
            TypeError: if aggregation function is not callable.
        """
        super().__init__()

        self._source\
            .add_desc(IS_PD_TS())

        if period is None:
            raise NameError("Please specify a period or use Period subclasses\
                    instead")

        if not isinstance(period, str):
            raise TypeError("Argument period must be of type string")

        if period.upper() in ["DAILY", "DAY"]:
            self._period = "D"
        elif period.upper() in ["MONTHLY", "MONTH"]:
            self._period = "M"
        elif period.upper() in ["QUARTERLY", "QUARTER"]:
            self._period = "Q"
        elif period.upper() in ["YEARLY", "YEAR"]:
            self._period = "Y"
        else:
            self._period = period

        if not callable(agg_func):
            raise TypeError("Argument agg_func must be callable")

        self.agg_func = agg_func

    def transform(self, data, **kwargs):
        """Apply data resampling"""
        return data.resample(self._period).apply(self.agg_func)

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            period=self._period,
            agg_func=self.agg_func,
            **kwargs
        )
