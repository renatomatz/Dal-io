"""Defines various ways of getting a subset of data based on some condition"""
import pandas as pd

from dalio.pipe import Pipe
from dalio.validator import HAS_COLS, IS_PD_TS
from dalio.util import process_cols, process_date


class ColSelect(Pipe):
    """Select columns.

    Attributes:
        _cols (list): names of columns to select.
    """
    def __init__(self, cols=None):
        """Initialize instance.

        Defines input data as having the specified columns.
        """
        super().__init__()

        self._source\
            .add_desc(HAS_COLS(cols))

        self._cols = process_cols(cols)

    def transform(self, data, **kwargs):
        """Selects the specified columns or returns data as is if no column
        was specified.

        Returns:
            Data of the same format as before but only only containing the
            specified columns.
        """
        return data[self._cols] if self._cols is not None else data

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            cols=self._cols,
            **kwargs
        )


class DateSelect(Pipe):
    """Select a date range.

    This is commonly left as a local variable to control date range being
    used at a piece of a graph.

    Attributes:
        _start (pd.Timestamp): start date.
        _end (pd.Timestamp): end date.
    """

    _start: pd.Timestamp
    _end: pd.Timestamp

    def __init__(self, start=None, end=None):
        """Initialize instance, processes and sets dates.

        Defines source data as a pandas time series.
        """
        super().__init__()

        self._source\
            .add_desc(IS_PD_TS())

        self._start = process_date(start)
        self._end = process_date(end)

    def transform(self, data, **kwargs):
        """Slices time series into selected date range.

        Returns:
            Time series of the same format as input containing a subset of
            the original dates.
        """
        return data.loc[self._start:self._end]

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            start=self._start,
            end=self._end,
            **kwargs
        )

    def set_start(self, start):
        """Set the _start attribute"""
        self._start = process_date(start)

    def set_end(self, end):
        """Set the _end attribute"""
        self._end = process_date(end)
