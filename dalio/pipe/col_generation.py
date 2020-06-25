"""Implement transformations that generates new colums from exising ones"""

from typing import (
    Any,
    List,
    Union,
    Callable,
    Iterable,
)

import numpy as np
import pandas as pd

from dalio.pipe import Pipe

from dalio.base.constants import RETURNS

from dalio.validator import (
    IS_PD_DF,
    IS_PD_TS,
    HAS_COLS,
)

from dalio.validator.presets import STOCK_STREAM

from dalio.util import (
    process_cols,
    process_new_colnames,
    process_new_df,
)

from dalio.ops import index_cols


class Change(Pipe):
    """Perform item-by-item change

    This has two main forms, percentage change and absolute change
    (difference).

    Attributes:
        _strategy (str, callable): change strategy.
        _new_cols (list, str): either list of new columns or suffix.
    """

    _PANDAS_PRESETS = ["pct_change", "diff"]

    _cols: List[str]
    _strategy: Union[str, Callable[[pd.Series], pd.Series]]
    _new_cols: Union[List[str], str]

    def __init__(self, strategy="pct_change", cols=None, new_cols=None):
        """Initialize instance and perform argument checks

        Args:
            strategy: change strategy.
            cols: specific columns to apply strategy to. If None are
                specified, all columns from sourced data will be used.
            new_cols: either a list of new columns or suffix to add to new
                columns. If None are specified, original columns will be
                dropped.

        Raises:
            ValueError: if strategy is not a valid string or new columns
                are not the same length as the columns to be transformed.
        """
        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())

        if cols is not None:
            self._source\
                .add_desc(HAS_COLS(cols))

        self._cols = process_cols(cols)

        if isinstance(strategy, str)\
                and strategy not in Change._PANDAS_PRESETS:
            raise ValueError(f"Argument strategy must be one of\
                {Change._PANDAS_PRESETS}")

        self._strategy = strategy

        if isinstance(new_cols, list) and len(new_cols) != len(self._cols):
            raise ValueError(f"argument new_cols must either be a string or\
                a list with {len(self._cols)} elements")

        self._new_cols = new_cols

    def transform(self, data, **kwargs):
        """Applies change transformation to sourced data"""

        data = data.copy()

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        if self._strategy == "pct_change":
            data = process_new_df(
                data,
                data[col_names].pct_change().fillna(0),
                col_names,
                new_col_names
            )

        elif self._strategy == "diff":
            data = process_new_df(
                data,
                data[col_names].diff().fillna(0),
                col_names,
                new_col_names
            )
        else:
            data = process_new_df(
                data,
                data[col_names].apply(self._strategy),
                col_names,
                new_col_names
            )

        return data

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            strategy=self._strategy,
            cols=self._cols,
            new_cols=self._new_cols,
            **kwargs
        )


class StockReturns(Change):
    """Perform percent change and minor aesthetic changes to data"""

    def __init__(self, cols=None, new_cols=False):
        super().__init__(
            cols=cols,
            strategy="pct_change",
            new_cols=RETURNS if new_cols else None
        )

        self._source.clear_desc()
        self._source.add_desc(STOCK_STREAM)

    def transform(self, data, **kwargs):
        """Same as base class but with relevant presets and multiplying by
        100 for aesthetic purposes
        """
        data = super().transform(data, **kwargs)

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        data.loc(axis=1)[new_col_names] = \
            data[col_names].apply(lambda x: x * 100)

        return data


class Rolling(Pipe):
    """Apply rolling function to columns

    Attributes:
        _rolling_func (callable): function to be performed on a window.
        _window (int): size of the rolling window
    """

    _rolling_func: Callable
    _window: int

    def __init__(self,
                 window=2,
                 rolling_func=lambda x: x,
                 cols=None,
                 new_cols=None):
        """Initialize instance

        Args:
            window (int): rolling window size.
            rolling_func (callable): function to apply to rolling window.
            cols, new_cols: See base class.

        Raise:
            ValueError: See base class
        """

        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())\
            .add_desc(HAS_COLS(cols))

        self._window = window

        # TODO: Place additional checks on these
        self._rolling_func = rolling_func

        self._cols = process_cols(cols)

        if isinstance(new_cols, list) and len(new_cols) != len(self._cols):
            raise ValueError(f"argument new_cols must either be a string or\
                a list with {len(self._cols)} elements")

        self._new_cols = new_cols

    def transform(self, data, **kwargs):
        """Apply rolling transformation to sourced data"""

        data = data.copy()

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        data.loc(axis=1)[new_col_names] = \
            data[col_names]\
            .rolling(window=self._window)\
            .apply(self._rolling_func)

        return data

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            window=self._window,
            rolling_func=self._rolling_func,
            **kwargs
        )


class Index(Pipe):
    """Index data at a specified value

    Attributes:
        index_at (int, float): value to index data at
        _cols (list): columns to index
        _groupby (list): columns to group data by
    """

    index_at: int
    _cols: List[str]
    _groupby: List[str]

    def __init__(self, index_at, cols=None, groupby=None):
        """Initialize instance

        Source must be a pandas time-series dataframe

        Args:
            See attributes
        """
        super().__init__()

        self._source\
            .add_desc(IS_PD_TS())\
            .add_desc(IS_PD_DF())

        self.index_at = index_at
        self._cols = process_cols(cols)
        self._groupby = process_cols(groupby)

    def transform(self, data, **kwargs):
        """Perform indexing"""

        cols_to_change = self._cols if self._cols is not None \
            else data.columns.to_list()

        if self._groupby is None:
            data.loc(axis=1)[cols_to_change] = \
                    data[cols_to_change].transform(index_cols, i=self.index_at)
        else:
            data[cols_to_change] = \
                    data[cols_to_change + self._groupby]\
                    .groupby(self._groupby)\
                    .transform(index_cols, i=self.index_at)

        return data

    def copy(self, *args, **kwargs):
        return super().copy(
            self.index_at,
            *args,
            cols=self._cols,
            groupby=self._groupby,
            **kwargs
        )


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
