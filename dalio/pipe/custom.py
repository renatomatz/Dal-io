"""Custom transformation"""
from dalio.pipe import ColSelect

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

from dalio.validator import (
    HAS_COLS,
    IS_PD_DF
)


class _ColGenerate(ColSelect):
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
                 func,
                 *args,
                 columns=None,
                 new_cols=None,
                 axis=0,
                 drop=True,
                 reintegrate=False,
                 **kwargs):

        super().__init__(columns=columns)

        if callable(func):
            self._func = func
        else:
            raise ValueError("func must be callable")

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
            self._func,
            *args,
            columns=self._columns,
            new_cols=self._new_cols,
            drop=self._drop,
            reintegrate=self._reintegrate,
            **kwargs
        )

    def _gen_cols(self, inter_df, **kwargs):
        raise NotImplementedError()


class Custom(_ColGenerate):
    """Apply custom function

    Attributes:
        strategy (str, default "pipe"): strategy for applying value function.
            One of ["apply", "transform", "pipe"]
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
            func,
            *args,
            columns=columns,
            new_cols=new_cols,
            axis=axis,
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
        )

        if strategy in ["apply", "transform", "pipe"]:
            self._strategy = strategy
        else:
            raise ValueError(f"Invalid strategy {strategy} \
                pick one of ['apply', 'transform', 'pipe']")

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
        elif self._strategy == "pipe":
            return inter_df.pipe(
                self._func,
                *self._args,
                **self._kwargs
            )

        return inter_df

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            strategy=self._strategy,
            **kwargs,
        )


class Rolling(_ColGenerate):
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
            func,
            *args,
            columns=columns,
            new_cols=new_cols,
            axis=axis,
            drop=drop,
            reintegrate=reintegrate,
            **kwargs
        )

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
            *args,
            rolling_window=self._rolling_window,
            **kwargs,
        )
