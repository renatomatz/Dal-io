import pandas as pd

from dalio.pipe import Pipe
from dalio.util import process_cols, _filter_cols
from dalio.validator import HAS_COLS, IS_PD_DF


class _ColSelection(Pipe):

    def __init__(self, cols, level=None):
        super().__init__()

        if cols is not None:
            self._source\
                .add_desc(HAS_COLS(cols, level=level))
        else:
            self._source\
                .add_desc(IS_PD_DF())

        self._cols = process_cols(cols)

        if level is None or isinstance(level, int):
            # TODO: add support to level names
            self._level = level
        else:
            raise TypeError(f"level attribute must be None or of type {int}, \
                    not {type(level)}")

    def copy(self):
        return type(self)(
            cols=self._cols,
            level=self._level
        )

    def _extract_col_names(self, data, filter_cols=True):
        cols = data.columns
        levels = None

        if self._level is not None:
            # create dict of unique elements in each level
            levels = {i: cols.unique(level=i).to_list()
                      for i in range(cols.nlevels)}
            # select elements of the chosen level
            cols = levels[self._level]

        cols = _filter_cols(cols, self._cols) if filter_cols else cols
        return cols, levels

    def _extract_col(self, data, col_name):
        sl = [slice(None)]*data.columns.nlevels
        sl[self._level if self._level is not None else 0] = col_name
        # probably there's an easier way of doing this
        return data.loc(axis=1).__getitem__(tuple(sl))

    def _insert_col(self, data, new_data, col_name):
        sl = [slice(None)]*data.columns.nlevels
        sl[self._level if self._level is not None else 0] = col_name
        # probably there's an easier way of doing this too
        data.loc(axis=1).__setitem__(tuple(sl), new_data)

    def _add_col(self, data, new_data):
        # TODO: propper checks
        # merge both indexes
        mi = data.columns.to_list() + new_data.columns.to_list()
        # create sorted multiindex
        mi = pd.MultiIndex.from_tuples(sorted(mi))
        # join data and reindex
        return data.join(new_data).reindex(mi, axis=1)

    def _drop_col(self, data, col_name):
        return data.drop(col_name, axis=1) \
            if self._level is None \
            else data.drop(col_name, axis=1, level=self._level)


class _ColValSelection(_ColSelection):

    def __init__(self, values, cols=None, level=None):
        super().__init__(cols, level=level)

        self._values = values

    def copy(self):
        return type(self)(
            values=self._values,
            cols=self._cols,
            level=self._level
        )


class _ColMapSelection(_ColSelection):

    def __init__(self, map_dict, level=None):
        super().__init__([*map_dict.keys()], level=level)

        self._map_dict = map_dict

    def copy(self):
        return type(self)(
            map_dict=self._map_dict,
            level=self._level
        )

    def _extract_col_names(self, data, filter_cols=False):
        return super()._extract_col_names(data, filter_cols=filter_cols)
