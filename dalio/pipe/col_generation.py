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

from dalio.base.constants import RETURNS

from dalio.pipe import Pipe, Custom

from dalio.util import (
    process_cols,
    process_new_colnames,
    process_new_df,
)

from dalio.ops import index_cols

from dalio.validator import (
    IS_PD_DF,
    IS_PD_TS,
    HAS_COLS,
)

from dalio.validator.presets import STOCK_STREAM


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


class Bin(Custom):
    """A pipeline stage that adds a binned version of a column or columns.

    If drop is set to True the new columns retain the names of the source
    columns; otherwise, the resulting column gain the suffix '_bin'

    Attributes:
        bin_map (array-like): implicitly projects a left-most bin containing
            all elements smaller than the left-most end point and a right-most
            bin containing all elements larger that the right-most end point.
            For example, the list [0, 5, 8] is interpreted as
            the bins (-∞, 0), [0-5), [5-8) and [8, ∞).

    Example:
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[-3],[4],[5], [9]], [1,2,3, 4], ['speed'])
        >>> pdp.Bin({'speed': [5]}, drop=False).apply(df)
           speed speed_bin
        1     -3        <5
        2      4        <5
        3      5        5≤
        4      9        5≤
        >>> pdp.Bin({'speed': [0,5,8]}, drop=False).apply(df)
           speed speed_bin
        1     -3        <0
        2      4       0-5
        3      5       5-8
        4      9        8≤
    """

    def __init__(self,
                 bin_map,
                 *args,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 replace=False,
                 **kwargs):

        super().__init__(
            self,
            lambda col: col.transform(
                pd.cut,
                axis=1,
                bins=bin_map,
                args=args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            replace=replace
        )


class MapColVals(Custom):
    """A pipeline stage that replaces the values of a column by a map.

    Attributes:
        value_map (dict, function or pandas.Series): A dictionary mapping
            existing values to new ones. Values not in the dictionary as keys
            will be converted to NaN. If a function is given, it is applied
            element-wise to given columns. If a Series is given, values are
            mapped by its index to its values.

    Example:
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1], [3], [2]], ['UK', 'USSR', 'US'], ['Medal'])
        >>> value_map = {1: 'Gold', 2: 'Silver', 3: 'Bronze'}
        >>> pdp.MapColVals('Medal', value_map).apply(df)
               Medal
        UK      Gold
        USSR  Bronze
        US    Silver
    """

    def __init__(self,
                 value_map,
                 *args,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 replace=False,
                 **kwargs):

        super().__init__(
            self,
            lambda col: col.map(
                value_map,
                *args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            replace=replace
        )


class ApplyByCols(Custom):
    """A pipeline stage applying an element-wise function to columns.

    Attributes:
        func (function): The function to be applied to each element of the
            given columns.

    Example:
        >>> import pandas as pd; import pdpipe as pdp; import math;
        >>> data = [[3.2, "acd"], [7.2, "alk"], [12.1, "alk"]]
        >>> df = pd.DataFrame(data, [1,2,3], ["ph","lbl"])
        >>> round_ph = pdp.ApplyByCols("ph", math.ceil)
        >>> round_ph(df)
           ph  lbl
        1   4  acd
        2   8  alk
        3  13  alk
    """
    def __init__(self,
                 func,
                 *args,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 replace=False,
                 **kwargs):

        super().__init__(
            self,
            lambda col: col.apply(
                func,
                axis=0,
                args=args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            replace=replace
        )


class AggByCols(Custom):
    """A pipeline stage applying a series-wise function to columns.

    Attributes:
    ----------
    func : function
        The function to be applied to each element of the given columns.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp; import numpy as np;
        >>> data = [[3.2, "acd"], [7.2, "alk"], [12.1, "alk"]]
        >>> df = pd.DataFrame(data, [1,2,3], ["ph","lbl"])
        >>> log_ph = pdp.ApplyByCols("ph", np.log)
        >>> log_ph(df)
                 ph  lbl
        1  1.163151  acd
        2  1.974081  alk
        3  2.493205  alk
    """
    def __init__(self,
                 func,
                 *args,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 replace=False,
                 **kwargs):

        super().__init__(
            self,
            lambda col: col.agg(
                func,
                *args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            replace=replace
        )


class Log(Custom):
    """A pipeline stage that log-transforms numeric data.

    Attributes:
        non_neg (bool, default False): If True, each transformed column is
            first shifted by smallest negative value it includes
            (non-negative columns are thus not shifted).
        const_shift (int, optional): If given, each transformed column is
            first shifted by this constant. If non_neg is True then that
            transformation is applied first, and only then is the column
            shifted by this constant.

    Example:
        >>> import pandas as pd; import pdpipe as pdp;
        >>> data = [[3.2, "acd"], [7.2, "alk"], [12.1, "alk"]]
        >>> df = pd.DataFrame(data, [1,2,3], ["ph","lbl"])
        >>> log_stage = pdp.Log("ph", drop=True)
        >>> log_stage(df)
                 ph  lbl
        1  1.163151  acd
        2  1.974081  alk
        3  2.493205  alk
    """

    @staticmethod
    def _cust_log(data, *args, non_neg=False, const_shift=None, **kwargs):

        if non_neg:
            minval = min(data)
            if minval < 0:
                data = data + abs(minval)

        # must check not None as neg numbers eval to False
        if const_shift is not None:
            data = data + const_shift

        np.log(data, *args, **kwargs)

    def __init__(self,
                 *args,
                 columns=None,
                 new_cols=None,
                 non_neg=False,
                 const_shift=None,
                 drop=True,
                 replace=False,
                 **kwargs):

        super().__init__(
            self,
            lambda col: col.apply(
                Log._cust_log,
                axis=0,
                non_neg=non_neg,
                const_shift=const_shift,
                args=args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            replace=replace
        )
