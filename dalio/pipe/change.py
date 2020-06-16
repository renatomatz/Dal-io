from numpy import nan

from typing import List, Union, Callable

from pandas import Series

from dalio.base.constants import RETURNS
from dalio.pipe import Pipe
from dalio.validator import IS_PD_DF, HAS_COLS, STOCK_STREAM
from dalio.util import process_cols, process_new_colnames, process_new_df


class Change(Pipe):

    _PANDAS_PRESETS = ["pct_change", "diff"]

    _cols: List[str]
    _strategy: Union[str, Callable[[Series], Series]]
    _new_cols: Union[List[str], str]

    def __init__(self, strategy="pct_change", cols=None, new_cols=None):
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

    def copy(self):
        ret = type(self)(
            cols=self._cols,
            strategy=self._strategy,
            new_cols=self._new_cols
        )

        return ret


class StockReturns(Change):

    def __init__(self, cols=None, new_cols=False):
        super().__init__(
            cols=cols,
            strategy="pct_change",
            new_cols=RETURNS if new_cols else None
        )

        self._source.clear_desc()
        self._source.add_desc(STOCK_STREAM)

    def transform(self, data, **kwargs):
        data = super().transform(data, **kwargs)

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        data.loc(axis=1)[new_col_names] = \
            data[col_names].apply(lambda x: x * 100)

        return data

    def copy(self):
        return type(self)(cols=self._cols)


class Rolling(Pipe):

    _rolling_func: Callable
    _first_func: Callable
    _window: int

    def __init__(self,
                 window=2,
                 rolling_func=lambda x: x,
                 first_func=lambda x: nan,
                 cols=None,
                 new_cols=None):

        super().__init__()

        self._source\
            .add_desc(IS_PD_DF())\
            .add_desc(HAS_COLS(cols))

        self._window = window

        # Place additional checks on these
        self._rolling_func = rolling_func
        self._first_func = first_func

        self._cols = process_cols(cols)

        if isinstance(new_cols, list) and len(new_cols) != len(self._cols):
            raise ValueError(f"argument new_cols must either be a string or\
                a list with {len(self._cols)} elements")

        self._new_cols = new_cols

    def transform(self, data, **kwargs):

        data = data.copy()

        first = data.iloc[0]

        col_names = self._cols if self._cols is not None \
            else data.columns.to_list()

        new_col_names = process_new_colnames(col_names, self._new_cols)

        data.loc(axis=1)[new_col_names] = \
            data[col_names]\
            .rolling(window=self._window)\
            .apply(self._rolling_func)

        data.iloc[0] = self._rolling_func(first)

        return data

    def copy(self):
        ret = type(self)(
                 window=self._window,
                 rolling_func=self._rolling_func,
                 first_func=self._first_func,
                 cols=self._cols,
                 new_cols=self._new_cols)

        return ret
