import pandas as pd

from typing import Any, List

from dalio.validator import Validator
from dalio.util import process_cols, unique


class IS_PD_TS(Validator):

    def __init__(self):
        super().__init__()
        self.test_desc = "Check if data has a DatetimeIndex"

    def validate(self, data):
        if isinstance(data.index, pd.DatetimeIndex):
            return True
        elif self._fatal:
            raise self._error_report(ValueError, data)
        else:
            return self._warn_report(data)

    def _warn_report(self, data):
        return f"Index is of type {type(data)}, not pandas.DatetimeIndex"


class IS_PD_DF(Validator):

    def __init__(self):
        super().__init__()
        self.test_desc = "Check if data is of type pandas.DataFrame"

    def validate(self, data):
        if isinstance(data, pd.DataFrame):
            return True
        elif self._fatal:
            raise self._error_report(ValueError, data)
        else:
            return self._warn_report(data)

    def _warn_report(self, data):
        return f"This is of type {type(data)}, not pandas.DataFrame"


class HAS_COLS(IS_PD_DF):
    # TODO: Accept regex

    _cols: List[str]
    _level: int

    def __init__(self, cols, level=None):
        super().__init__()
        
        self._cols = process_cols(cols)
        self._level = level

        self.test_desc = "Check if specified columns are present in \
                pd.DataFrame"

    def validate(self, data):
        df_res = super().validate(data)

        if isinstance(df_res, str):
            return df_res

        if self._level is None:
            all_cols = data.columns
        else:
            all_cols = [unique(elems) for elems 
                        in zip(*data.columns)][self._level]

        if callable(self._cols):
            cols_to_check = [
                col for col in all_cols
                if self._cols(col)
            ]
        else:
            cols_to_check = self._cols

        missing_cols = [col for col in self._cols
                        if col not in cols_to_check]

        if len(missing_cols) == 0:
            return True
        elif self._fatal:
            raise self._error_report(KeyError, data, missing_cols)
        else:
            return self._warn_report(data, missing_cols)

    def _warn_report(self, data, missing):
        return f"The following columns are missing: {missing}"


class HAS_IN_COLS(HAS_COLS):

    _items: List[Any]

    def __init__(self, items, cols=None):
        super().__init__(cols)
        self._items = process_cols(items)
        self.test_desc = "Check if items are present in specified columns"

    def validate(self, data):

        cols_to_check = self._cols
        if self._cols is not None:
            cols_res = super().validate(data)
        else:
            cols_to_check = data.columns.to_list()
            
        if isinstance(cols_res, str):
            return cols_res
        
        not_in_cols = [item for item in self._items
                       if item not in data[cols_to_check].to_numpy()]
        
        if len(not_in_cols) == 0:
            return True
        elif self._fatal:
            raise self._error_report(KeyError, data, not_in_cols)
        else:
            return self._warn_report(data, not_in_cols)

    def _warn_report(self, data, missing):

        checked_cols = self._cols if self._cols is not None \
                else data.columns.to_list()

        return f"The following items are missing from columns {checked_cols}: \
                {missing}"


class HAS_INDEX_NAMES(IS_PD_DF):

    _names: List[str]
    _axis: int

    def __init__(self, names, axis=0):
        super().__init__()

        self._names = process_cols(names)
        
        if axis not in [0, 1]:
            raise ValueError("argument axis must be either 0 or 1")
        else:
            self._axis = axis

        self.test_desc = "Check if data index has specified names"

    def validate(self, data):

        axis_names = data.columns.names if self._axis else data.index.names
        
        not_in_index = [name for name in self._names 
                        if name not in axis_names]

        if len(not_in_index) == 0:
            return True
        elif self._fatal:
            raise self._error_report(KeyError, data, not_in_index)
        else:
            return self._warn_report(data, not_in_index)

    def _warn_report(self, data, missing):

        return f"The following items are missing from axis {self._axis}: \
                {missing}"


# TODO: Make a mean value checker

