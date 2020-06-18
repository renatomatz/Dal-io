from itertools import product

import pandas as pd

from dalio.pipe import Pipe
from dalio.validator import IS_PD_DF, HAS_COLS, IS_PD_TS
from dalio.util import process_cols, process_date, unique, filter_columns


class DropNa(Pipe):
    """A pipeline stage that drops null values.

    Supports all parameter supported by pandas.dropna function.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1,4],[4,None],[1,11]], [1,2,3], ['a','b'])
        >>> pdp.DropNa().apply(df)
           a     b
        1  1   4.0
        3  1  11.0
    """

    def __init__(self, **kwargs):
        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())

        self._drop_na_kwargs(**kwargs)

    def transform(self, data, **kwargs):
        return data.dropna(**self._dropna_kwargs)


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


class _ColSelection(Pipe):

    def __init__(self, cols, **kwargs):
        super().__init__()

        if cols is not None:
            level = kwargs.get("level", None)
            self._source\
                .add_desc(HAS_COLS(cols, level=level))

        if not callable(cols):
            self._cols = process_cols(cols)
        else:
            self._cols = cols

    def copy(self):

        ret = type(self)(
            cols=self._cols
        )

        return ret


class ColSelect(_ColSelection):

    def transform(self, data, **kwargs):

        cols_to_keep = filter_columns(data.columns.to_list(), self._cols)

        return data[cols_to_keep]


class ColDrop(_ColSelection):
    """A pipeline stage that drops columns by name.

    Parameters
    ----------
    cols : str, iterable or callable
        The label, or an iterable of labels, of columns to drop. Alternatively,
        cols can be assigned a callable returning bool values for
        pandas.Series objects; if this is the case, every column for which it
        return True will be dropped.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[8,'a'],[5,'b']], [1,2], ['num', 'char'])
        >>> pdp.ColDrop('num').apply(df)
          char
        1    a
        2    b
    """

    def transform(self, data, **kwargs):

        if callable(self._cols):
            cols_to_drop = [
                col for col in df.cols
                if self._cols(df[col])
            ]
        else:
            cols_to_drop = self._cols

        return data.drop(cols_to_drop, axis=1)


class RowDrop(_ColSelection):
    """A pipeline stage that drop rows by callable conditions.

    Parameters
    ----------
    conditions : list-like or dict
        The list of conditions that make a row eligible to be dropped. Each
        condition must be a callable that take a cell value and return a bool
        value. If a list of callables is given, the conditions are checked for
        each column value of each row. If a dict mapping column labels to
        callables is given, then each condition is only checked for the column
        values of the designated column.
    reduce : 'any', 'all' or 'xor', default 'any'
        Determines how row conditions are reduced. If set to 'all', a row must
        satisfy all given conditions to be dropped. If set to 'any', rows
        satisfying at least one of the conditions are dropped. If set to 'xor',
        rows satisfying exactly one of the conditions will be dropped. Set to
        'any' by default.
    columns : str or iterable, optional
        The label, or an iterable of labels, of columns. Optional. If given,
        input conditions will be applied to the sub-dataframe made up of
        these columns to determine which rows to drop.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1,4],[4,5],[5,11]], [1,2,3], ['a','b'])
        >>> pdp.RowDrop([lambda x: x < 2]).apply(df)
           a   b
        2  4   5
        3  5  11
        >>> pdp.RowDrop({'a': lambda x: x == 4}).apply(df)
           a   b
        1  1   4
        3  5  11
    """

    _REDUCERS = {
        'all': all,
        'any': any,
        'xor': lambda x: sum(x) == 1
    }

    def __init__(self, conditions, columns=None, reduce_strat=None, **kwargs):

        self._conditions = conditions

        if reduce_strat not in RowDrop._REDUCERS.keys():
            raise ValueError((
                "{} is an unsupported argument for the 'reduce' parameter of "
                "the RowDrop constructor!").format(reduce))

        self._reducer = RowDrop._REDUCERS.get(reduce_strat, "any")

        if isinstance(conditions, dict):
            valid = all([callable(cond) for cond in conditions.values()])
            if not valid:
                raise ValueError(
                    "Condition dicts given to RowDrop must map to callables!")
            self._columns = list(conditions.keys())
        elif hasattr(conditions, "__iter__"):
            valid = all([callable(cond) for cond in conditions])
            if not valid:
                raise ValueError(
                    "RowDrop condition lists can contain only callables!")
        elif not callable(conditions):
                raise ValueError(
                    "RowDrop condition must be callable!")

        self._conditions = conditions

        super().__init__(self._columns)


    def _transform(self, df, verbose):

        cols_to_check = filter_columns(data.columns.to_list(), self._cols)

        i_to_keep = set()
        for col in cols_to_check:
            if isinstance(conditions, dict):
            elif hasattr(conditions, "__iter__"):
            else:

            i_to_keep.update(data[col].isin(vals_to_keep))

        return data.copy().iloc[[*i_to_keep]]


class _ColValSelection:

    def __init__(self, values, cols=None):
        super().__init__(cols)

        self._values = values

    def copy(self):
        ret = super().copy()
        ret._values = self._values
        return ret


class ValDrop(_ColValSelection):
    """A pipeline stage that drops rows by value.

    Parameters
    ----------
    values : list-like
        A list of the values to drop.
    cols : str or list-like, default None
        The name, or an iterable of names, of columns to check for the given
        values. If set to None, all columns are checked.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1,4],[4,5],[18,11]], [1,2,3], ['a','b'])
        >>> pdp.ValDrop([4], 'a').apply(df)
            a   b
        1   1   4
        3  18  11
        >>> pdp.ValDrop([4]).apply(df)
            a   b
        3  18  11
    """

    def transform(self, data, **kwargs):

        inter_df = data.copy()

        cols_to_check = self._cols if self._cols is not None else data.cols

        for col in cols_to_check:
            # keep those values that are not (~) in self._values
            inter_df = inter_df[~inter_df[col].isin(self._values)]

        return inter_df


class ValKeep(_ColValSelection):
    """A pipeline stage that keeps rows by value.

    Parameters
    ----------
    values : list-like
        A list of the values to keep.
    columns : str or list-like, default None
        The name, or an iterable of names, of columns to check for the given
        values. If set to None, all columns are checked.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1,4],[4,5],[5,11]], [1,2,3], ['a','b'])
        >>> pdp.ValKeep([4, 5], 'a').apply(df)
           a   b
        2  4   5
        3  5  11
        >>> pdp.ValKeep([4, 5]).apply(df)
           a  b
        2  4  5
    """

    def transform(self, data, **kwargs):

        inter_df = data.copy()
        before_count = len(inter_df)

        cols_to_check = self._cols if self._cols is not None else data.cols

        for col in cols_to_check:
            # keep those values that are in self._values
            inter_df = inter_df[inter_df[col].isin(self._values)]

        return inter_df


class FreqDrop(_ColValSelection):
    """A pipeline stage that drops rows by value frequency.

    Parameters
    ----------
    threshold : int
        The minimum frequency required for a value to be kept.
    column : str
        The name of the colums to check for the given value frequency.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[1,4],[4,5],[1,11]], [1,2,3], ['a','b'])
        >>> pdp.FreqDrop(2, 'a').apply(df)
           a   b
        1  1   4
        3  1  11
    """

    def _transform(self, data, **kwargs):

        cols_to_check = filter_columns(data.columns.to_list(), self._cols)

        i_to_keep = set()
        for col in cols_to_check:
            valcount = data[col].value_counts()
            vals_to_keep = valcount[valcount >= self._value].index
            i_to_keep.update(data[col].isin(vals_to_keep))

        return data.copy().iloc[[*i_to_keep]]


class _ColMapSelection:

    def __init__(self, map_dict, **kwargs):
        super().__init__([*map_dict.keys()], **kwargs)

        self._map_dict = map_dict

    def copy(self):
        ret = super().copy()
        ret._map_dict = self._map_dict
        return ret


class ColRename(_ColMapSelection):
    """A pipeline stage that renames a column or columns.

    Parameters
    ----------
    rename_map : dict
        Maps old column names to new ones.

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[8,'a'],[5,'b']], [1,2], ['num', 'char'])
        >>> pdp.ColRename({'num': 'len', 'char': 'initial'}).apply(df)
           len initial
        1    8       a
        2    5       b
    """

    def transform(self, data, **kwargs):
        return data.rename(columns=self._map_dict)


class ColReorder(_ColMapSelection):
    """A pipeline stage that reorders columns.

    Parameters
    ----------
    positions : dict
        A mapping of column names to their desired positions after reordering.
        Columns not included in the mapping will maintain their relative
        positions over the non-mapped colums.

    level: int, None

    Example
    -------
        >>> import pandas as pd; import pdpipe as pdp;
        >>> df = pd.DataFrame([[8,4,3,7]], columns=['a', 'b', 'c', 'd'])
        >>> pdp.ColReorder({'b': 0, 'c': 3}).apply(df)
           b  a  d  c
        0  4  8  7  3
    """

    def __init__(self, map_dict, level=None):
        super().__init__(map_dict, level=level)

        if self._level is None or isinstance(level, int):
            self._level = level
        else:
            TypeError(f"level attribute must be None or of type {int}, \
                    not {type(level)}")

    def transform(self, data, **kwargs):

        cols = data.columns.to_list()

        if self._level is not None:
            # create dict of unique elements in each level
            levels = {i: unique(elems) for i, elems in enumerate(zip(*cols))}
            # select elements of the chosen level
            cols = levels[self._level]
        
        # reposition elements of columns or chosen levels
        for pos, col in self._map_dict:
            cols.remove(col)
            # this works for edge cases like last cols because of the remove
            cols = cols[:pos] + [col] + cols[pos:]

        if self._levels is not None:
            levels[self._level] = col
            cols = [*product(*levels.values())]
        
        return data[cols]

    def copy(self):
        ret = super().copy()
        ret._level = self._level
        return ret
