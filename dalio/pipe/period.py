import numpy as np

from typing import Callable, Any, Iterable

from dalio.pipe import Pipe
from dalio.validator import IS_PD_TS


class Period(Pipe):

    agg_func: Callable[[Iterable], Any]
    _period: str
    
    def __init__(self, period=None, agg_func=np.mean):
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
        return data.resample(self._period).apply(self.agg_func)

    def copy(self):
        ret = type(self)(
            period=self._period
        )
        ret.agg_func = self.agg_func
        return ret
