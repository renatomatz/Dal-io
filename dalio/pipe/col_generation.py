"""Implement transformations that generates new colums from exising ones"""

from typing import (
    Callable,
    Iterable,
    Any,
)

import numpy as np
import pandas as pd

from scipy.stats import boxcox

from dalio.base.constants import RETURNS

from dalio.pipe.select import _ColSelection

from dalio.ops import index_cols

from dalio.util import (
    process_cols,
    extract_level_names_dict,
    filter_levels,
    extract_cols,
    add_suffix,
    drop_cols,
    mi_join,
    insert_cols,
)

from dalio.validator import IS_PD_TS

from dalio.validator.presets import STOCK_STREAM


class _ColGeneration(_ColSelection):
    """Generate column based on a selection from a dataframe.

    These are very useful for simple operations or for testing, as no
    additional class definitions or understanding of the documentation is
    requred.
    Attributes:
        columns (single label or list-like): Column labels in the DataFrame
            to be mapped.
        func (callable): The function to be applied to each row of the
            processed DataFrame.
        result_columns (str or list-like, default None): If list-like, labels
            for the new columns resulting from the mapping operation. Must be
            of the same length as columns. If str, the suffix mapped columns
            gain if no new column labels are given. If None, behavior depends
            on the replace parameter.
        axis (int, default 1): axis to apply value funciton to. Irrelevant if
            strategy = "pipe".
        drop (bool, default True): If set to True, source columns are dropped
            after being mapped.
        reintegrate (bool, default False): If set to False, modified version is
            returned without being placed back into original dataframe. If set
            to True, an insertion is attemtped; if the transformation changes
            the data's shape, a RuntimeError will be raised.
        _args: arguments to be passed onto the function at execution time.
        _kwargs: keyword arguments to be passed onto the function at
            execution time.

    Example:
        >>> import pandas as pd; from dalio.pipe import Custom;
        >>> data = [[3, 2143], [10, 1321], [7, 1255]]
        >>> df = pd.DataFrame(data, [1,2,3], ['years', 'avg_revenue'])
        >>> total_rev = lambda row: row['years'] * row['avg_revenue']
        >>> add_total_rev = Custom(total_rev, 'total_revenue', axis=1)
        >>> add_total_rev.transform(df)
           years  avg_revenue  total_revenue
        1      3         2143           6429
        2     10         1321          13210
        3      7         1255           8785
        >>> def halfer(row):
        ...     new = {'year/2': row['years']/2,
        ...            'rev/2': row['avg_revenue']/2}
        ...     return pd.Series(new)
        >>> half_cols = Custom(halfer, axis=1, drop=False)
        >>> half_cols.transform(df)
           years  avg_revenue   rev/2  year/2
        1      3         2143  1071.5     1.5
        2     10         1321   660.5     5.0
        3      7         1255   627.5     3.5

        >>> data = [[3, 3], [2, 4], [1, 5]]
        >>> df = pd.DataFrame(data, [1,2,3], ["A","B"])
        >>> func = lambda df: df['A'] == df['B']
        >>> add_equal = Custom(func, "A==B", strategy="pipe", drop=False)
        >>> add_equal.transform(df)
           A  B   A==B
        1  3  3   True
        2  2  4  False
        3  1  5  False
    """

    def __init__(self,
                 *args,
                 columns=None,
                 new_cols=None,
                 axis=0,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        super().__init__(columns=columns)

        if isinstance(new_cols, str):
            self._new_cols = new_cols
        else:
            self._new_cols = process_cols(new_cols)

        if axis in [0, 1, None]:
            self._axis = axis
        else:
            raise ValueError(f"Invalid axis {axis} \
                pick one of [0, 1, None]")

        self._drop = drop
        self._reintegrate = reintegrate

        self._args = args
        self._kwargs = kwargs

    def transform(self, data, **kwargs):
        """Apply custom transformation and insert back as specified

        This applies the transformation in three main steps:
        1. Extract specified columns
        2. Apply modification
        3. Insert columns if needed or return modified dataframe

        These steps have further details for dealing with levels.

        Raises:
            RuntimeError: if transformed data is to be reintegrated but has a
                different shape than data being reintegrated on the dataframe.
        """

        cols = filter_levels(
            extract_level_names_dict(data),
            self._columns
        )

        inter_df = extract_cols(data, cols)
        orig_shape = inter_df.shape

        inter_df = self._gen_cols(inter_df)

        if isinstance(self._new_cols, str):
            inter_df.columns = add_suffix(
                inter_df.columns,
                cols,
                self._new_cols
            )
        elif self._new_cols is not None:
            inter_df.columns = self._new_cols

        # No reintegration
        if not self._reintegrate:
            return inter_df

        # Reintegration by joining
        if self._new_cols is not None:
            if self._drop:
                data = drop_cols(data, cols)

            # Join new columns into the data frame
            return mi_join(data, inter_df)

        # Reintegration by replacement
        if inter_df.shape != orig_shape:
            raise RuntimeError("Existing columns cannot be \
                reintegrated if transformation changes data shape")

        # Insert new columns on top of old ones.
        return insert_cols(data, inter_df, cols)

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            *self._args,
            new_cols=self._new_cols,
            drop=self._drop,
            reintegrate=self._reintegrate,
            **kwargs,
            **self._kwargs,
        )

    def _gen_cols(self, inter_df, **kwargs):
        raise NotImplementedError()


class Custom(_ColGeneration):
    """Apply custom function.

    Attributes:
        strategy (str, default "pipe"): strategy for applying value function.
            One of ["apply", "transform", "agg", "pipe"]
    """

    def __init__(self,
                 func,
                 *args,
                 columns=None,
                 new_cols=None,
                 strategy="apply",
                 axis=0,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        super().__init__(
            *args,
            columns=columns,
            new_cols=new_cols,
            axis=axis,
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
        )

        if callable(func):
            self._func = func
        else:
            raise ValueError("func must be callable")

        if strategy in ["apply", "transform", "agg", "pipe"]:
            self._strategy = strategy
        else:
            raise ValueError(f"Invalid strategy {strategy} \
                pick one of ['apply', 'transform', 'agg', 'pipe']")

    def _gen_cols(self, inter_df, **kwargs):

        if self._strategy == "apply":
            return inter_df.apply(
                self._func,
                axis=self._axis,
                args=self._args,
                **self._kwargs
            )
        elif self._strategy == "transform":
            return inter_df.transform(
                self._func,
                axis=self._axis,
                args=self._args,
                **self._kwargs
            )
        elif self._strategy == "agg":
            return inter_df.agg(
                self._func,
                axis=self._axis,
                *self._args,
                **self._kwargs
            )
        elif self._strategy == "pipe":
            return inter_df.pipe(
                self._func,
                *self._args,
                **self._kwargs
            )

        return inter_df

    def copy(self, *args, **kwargs):
        return super().copy(
            self._func,
            *args,
            strategy=self._strategy,
            **kwargs,
        )


class CustomByCols(Custom):
    """A pipeline stage applying a function to individual columns iteratively.

    Attributes:
        func (function): The function to be applied to each element of the
            given columns.
        strategy (str): Application strategy. Different from Custom class'
            strategy parameter (which here is kept at "apply") as this will
            now be done on a series (each column). Extra care should be taken
            to ensure resulting column lengths match.

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
                 strategy="apply",
                 columns=None,
                 new_cols=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        if strategy == "apply":
            def cust_func(col):
                return col.apply(
                    func,
                    args=args,
                    **kwargs
                )
        elif strategy == "transform":
            def cust_func(col):
                return col.transform(
                    func,
                    args=args,
                    **kwargs
                )

        super().__init__(
            cust_func,
            columns=columns,
            new_cols=new_cols,
            strategy="apply",
            axis=0,
            drop=drop,
            reintegrate=reintegrate
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
        bin_strat (str, default "normal"): binning strategy to use. "normal"
            uses the default binning strategy per a list of value separations
            or number of bins. "quantile" uses a list of quantiles or a
            preset quantile range (4 for quartiles and 10 for deciles).

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
                 bin_strat="normal",
                 columns=None,
                 new_cols=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        if bin_strat == "normal":
            bin_func = pd.cut
            kwargs.update({"bins": bin_map})
        elif bin_strat == "quantile":
            bin_func = pd.qcut
            kwargs.update({"q": bin_map})

        super().__init__(
            bin_func,
            *args,
            columns=columns,
            new_cols=new_cols,
            strategy="apply",
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
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

        if const_shift is not None:
            data = data + const_shift

        return np.log(data, *args, **kwargs)

    def __init__(self,
                 *args,
                 columns=None,
                 new_cols=None,
                 non_neg=False,
                 const_shift=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        kwargs.update({
            "non_neg": non_neg,
            "const_shift": const_shift
        })

        super().__init__(
            Log._cust_log,
            *args,
            columns=columns,
            new_cols=new_cols,
            strategy="apply",
            drop=drop,
            reintegrate=reintegrate,
            **kwargs,
        )


class BoxCox(Custom):
    """A pipeline stage that applies the BoxCox transformation on data.

    Attributes:
        const_shift (int, optional): If given, each transformed column is
            first shifted by this constant. If non_neg is True then that
            transformation is applied first, and only then is the column
            shifted by this constant.
    """

    @staticmethod
    def _cust_boxcox(data, *args, const_shift=None, **kwargs):

        minval = min(data)
        if minval < 0:
            data = data + abs(minval)

        if const_shift is not None:
            data = data + const_shift

        # returns log-likelihood-optimized array and optimal lambda
        opt, _ = boxcox(np.array(data.dropna()), *args, **kwargs)

        return opt

    def __init__(self,
                 *args,
                 columns=None,
                 new_cols=None,
                 non_neg=False,
                 const_shift=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        kwargs.update({
            "non_neg": non_neg,
            "const_shift": const_shift
        })

        super().__init__(
            Log._cust_log,
            *args,
            columns=columns,
            new_cols=new_cols,
            strategy="apply",
            drop=drop,
            reintegrate=reintegrate,
            **kwargs,
        )


class MapColVals(Custom):
    """A pipeline stage that reintegrates the values of a column by a map.

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
                 reintegrate=False,
                 **kwargs):

        super().__init__(
            lambda col: col.map(
                value_map,
                *args,
                **kwargs
            ),
            columns,
            new_cols=new_cols,
            strategy="apply",
            drop=drop,
            reintegrate=reintegrate
        )


class Rolling(_ColGeneration):
    """Apply rolling function

    Attributes:
        rolling_window (int, defailt None): rolling window to apply
            function. If none, no rolling window is applied.
    """

    def __init__(self,
                 func,
                 *args,
                 columns=None,
                 new_cols=None,
                 rolling_window=2,
                 axis=0,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        super().__init__(
            *args,
            columns=columns,
            new_cols=new_cols,
            axis=axis,
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
        )

        if callable(func):
            self._func = func
        else:
            raise ValueError("func must be callable")

        if isinstance(rolling_window, int):
            self._rolling_window = rolling_window
        else:
            raise TypeError("rolling must be none or an integer")

    def _gen_cols(self, inter_df, **kwargs):

        return inter_df.rolling(
            self._rolling_window,
            axis=self._axis
        ).apply(self._func, args=self._args, **self._kwargs)

    def copy(self, *args, **kwargs):
        return super().copy(
            self._func,
            *args,
            rolling_window=self._rolling_window,
            **kwargs,
        )


class Period(_ColGeneration):
    """Resample input time series data to a different period

    Attributes:
        agg_func (callable): function to aggregate data to one period.

# Quandl Input
            Default set to np.mean.
        _period (str): period to resample data to. Can be either daily,
            monthly, quarterly or yearly.
    """

    agg_func: Callable[[Iterable], Any]
    _period: str

    def __init__(self,
                 period,
                 *args,
                 agg_func=np.mean,
                 columns=None,
                 new_cols=None,
                 axis=0,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        """Initialize instance.

        Describes source data as a time series.
        Check that provided period is valid to some preset standards.

        Raises:
            TypeError: if aggregation function is not callable.
        """

        super().__init__(
            *args,
            columns=columns,
            new_cols=new_cols,
            axis=axis,
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
        )

        if callable(agg_func):
            self._agg_func = agg_func
        else:
            raise ValueError("func must be callable")

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

    def _gen_cols(self, inter_df, **kwargs):

        return inter_df.resample(
            self._period,
            axis=self._axis
        ).apply(self._agg_func, *self._args, **self._kwargs)

    def copy(self, *args, **kwargs):
        return super().copy(
            self._period,
            *args,
            **kwargs,
        )


class Change(_ColGeneration):
    """Perform item-by-item change

    This has two main forms, percentage change and absolute change
    (difference).

    Attributes:
        _strategy (str, callable): change strategy.
    """

    _PANDAS_PRESETS = ["pct_change", "diff"]

    def __init__(self,
                 *args,
                 strategy="diff",
                 columns=None,
                 new_cols=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):
        """Initialize instance and perform argument checks

        Args:
            strategy: strategy strategy.

        Raises:
            ValueError: if strategy is not a valid string or new columns
                are not the same length as the columns to be transformed.
        """

        if isinstance(strategy, str)\
                and strategy not in Change._PANDAS_PRESETS:
            raise ValueError(f"Argument strategy must be one of\
                {Change._PANDAS_PRESETS}")

        self._strategy = strategy

        if self._strategy == "pct_change":
            self._ch_func = lambda df: df.pct_change().fillna(0)
        elif self._strategy == "diff":
            self._ch_func = lambda df: df.diff().fillna(0)

        super().__init__(
            columns=columns,
            new_cols=new_cols,
            drop=drop,
            reintegrate=reintegrate
        )

    def _gen_cols(self, inter_df, **kwargs):
        return inter_df.pipe(
            self._ch_func,
            *self._args,
            **self._kwargs
        )

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            strategy=self._strategy,
            **kwargs,
        )


class StockReturns(_ColGeneration):
    """Perform percent change and minor aesthetic changes to data"""

    def __init__(self,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 reintegrate=False):

        super().__init__(
            columns=columns,
            new_cols="_" + RETURNS if new_cols else None,
            drop=drop,
            reintegrate=reintegrate
        )

        self._source\
            .add_desc(STOCK_STREAM)

    def _gen_cols(self, inter_df, **kwargs):
        return inter_df.pipe(
            lambda df: df.pct_change().fillna(0) * 100,
            *self._args,
            **self._kwargs
        )


class Index(_ColGeneration):

    def __init__(self,
                 index_at,
                 *args,
                 columns=None,
                 new_cols=None,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        if not isinstance(index_at, int):
            raise ValueError(f"index must be an integer, not \
                {type(index_at)}")

        self._index_at = index_at

        super().__init__(
            columns=columns,
            new_cols=new_cols,
            drop=drop,
            reintegrate=reintegrate,
        )

    def copy(self, *args, **kwargs):
        return super().copy(
            self._index_at,
            *args,
            **kwargs,
        )

    def _gen_cols(self, inter_df, **kwargs):
        return inter_df.apply(
            index_cols,
            axis=0,
            args=self._args,
            i=self._index_at,
            **self._kwargs
        )
