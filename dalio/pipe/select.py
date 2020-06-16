import pandas as pd

from dalio.pipe import Pipe
from dalio.validator import HAS_COLS, IS_PD_TS
from dalio.util import process_cols, process_date


class ColSelect(Pipe):

    def __init__(self, cols=None):
        super().__init__()

        self._source\
            .add_desc(HAS_COLS(cols))

        self._cols = process_cols(cols)

    def transform(self, data, **kwargs):
        return data[self._cols] if self._cols is not None else data

    def copy(self):
        ret = type(self)(
            cols=self._cols
        )
        return ret


class DateSelect(Pipe):

    _start: pd.Timestamp
    _end: pd.Timestamp

    def __init__(self, start=None, end=None):
        super().__init__()

        self._source\
            .add_desc(IS_PD_TS())

        self._start = process_date(start)
        self._end = process_date(end)

    def transform(self, data, **kwargs):
        return data.loc[self._start:self._end]

    def copy(self):
        return type(self)(
            start=self._start,
            end=self._end
        )

    def set_start(self, start):
        self._start = process_date(start)

    def set_end(self, end):
        self._end = process_date(end)
