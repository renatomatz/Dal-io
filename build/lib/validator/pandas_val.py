from typing import Any, List

import pandas as pd

from dalio.validator import IS_TYPE
from dalio.util import process_cols


class IS_PD_DF(IS_TYPE):
    """Checks if data is a pandas dataframe

    Attributes:
        See base class
    """

    def __init__(self):
        super().__init__(pd.DataFrame)
        self.test_desc = "Check if data is of type pandas.DataFrame"


class IS_PD_TS(IS_TYPE):
    """Checks if data is a pandas time series
    """

    def __init__(self):
        super().__init__((pd.DataFrame, pd.Series,))
        self.test_desc = "Check if data has a DatetimeIndex"

    def validate(self, data):
        """Validates data if it's index is of type pandas.DateTimeIndex"""
        super_err = super().validate(data)
        if super_err is not None:
            return super_err

        if isinstance(data.index, pd.DatetimeIndex):
            return None
        else:
            return f"Index is of type {type(data)}, not pandas.DatetimeIndex"


class HAS_COLS(IS_PD_DF):
    """Checks if data has certain column names

    Attributes:
        _cols: list of column names to check
    """

    _cols: List[str]
    _level: int

    def __init__(self, cols, level=None):
        """Initialize instance

        Args:
            _cols (list): column names to check for
        """
        super().__init__()

        self._cols = process_cols(cols)
        self._level = level

        self.test_desc = "Check if specified columns are present in \
                pd.DataFrame"

    def validate(self, data):
        """Validates data if all the columns in self._cols is present in the
        dataframe
        """
        df_res = super().validate(data)

        if isinstance(df_res, str):
            return df_res

        if self._level is None:
            all_cols = data.columns
        else:
            all_cols = data.columns.unique(level=self._level)

        if callable(self._cols):
            cols_to_check = [
                col for col in all_cols
                if self._cols(col)
            ]
        else:
            cols_to_check = self._cols

        missing_cols = [col for col in self._cols
                        if col not in cols_to_check] \
                            if self._cols is not None \
                            else []

        if len(missing_cols) == 0:
            return None
        else:
            return f"The following columns are missing: {missing_cols}"


class HAS_IN_COLS(HAS_COLS):
    """Check if certain items are present in certain columns

    Attributes:
        _cols: See base class
        _items: items that must be present in each of the specified columns
    """

    _items: List[Any]

    def __init__(self, items, cols=None):
        """Initialize instance

        Args:
            _items (list): list of items to check for
            _cols (list): list of column names to check items for
        """
        super().__init__(cols)
        self._items = process_cols(items)
        self.test_desc = "Check if items are present in specified columns"

    def validate(self, data):
        """Validates data if items in self._items are not present in specified
        columns. Specified columns are all columns if self._cols is None.
        """
        super_err = super().validate(data)
        if super_err is not None:
            return super_err

        cols_to_check = self._cols if self._cols is not None \
            else data.columns.to_list()

        not_in_cols = [item for item in self._items
                       if item not in data[cols_to_check].to_numpy()]

        if len(not_in_cols) == 0:
            return None
        else:
            checked_cols = self._cols if self._cols is not None \
                else data.columns.to_list()

            return f"The following items are missing from columns \
                {checked_cols}: {not_in_cols}"


class HAS_INDEX_NAMES(IS_PD_DF):
    """Checks if an axis has specified names

    Attributes:
        _names: names to check for
        _axis: axis to check for names
    """

    _names: List[str]
    _axis: int

    def __init__(self, names, axis=0):
        """Initialize instance and check if axis arg.

        Args:
            axis (int): axis to check for names. Either 0 or 1.
        """
        super().__init__()

        self._names = process_cols(names)

        if axis not in [0, 1]:
            raise ValueError("argument axis must be either 0 or 1")
        else:
            self._axis = axis

        self.test_desc = "Check if data index has specified names"

    def validate(self, data):
        """Validates data if specified axis has the specified names"""

        axis_names = data.columns.names if self._axis else data.index.names

        not_in_index = [name for name in self._names
                        if name not in axis_names]

        if len(not_in_index) == 0:
            return None
        else:
            return f"The following items are missing from axis \
                {self._axis}: {not_in_index}"


class HAS_LEVELS(IS_PD_DF):

    def __init__(self, levels, axis=0, comparisson="<="):
        super().__init__()

        if levels is None or isinstance(levels, int):
            # TODO: add support to level names
            self._levels = levels
        else:
            raise TypeError(f"level attribute must be None or of type {int}, \
                    not {type(levels)}")
        
        if axis not in [0, 1]:
            raise ValueError("argument axis must be either 0 or 1")
        else:
            self._axis = axis

        if comparisson in [">=", "==", "<="]:
            self._comparission = comparisson
        else:
            raise ValueError(f"argument 'comparisson' must be one of \
                ['>=', '==', '<=']")

    def validate(self, data):

        nlevels = (data.index if self._axis == 0 else data.columns).nlevels
        passed = False

        # TODO: use dicts and function operators, this is ugly
        if self._comparisson == ">=":
            passed = nlevels >= self._levels
        elif self._comparisson == "==":
            passed = nlevels == self._levels
        elif self._comparisson == "<=":
            passed = nlevels <= self._levels

        if passed:
            return True
        elif self._fatal:
            raise self._error_report(ValueError, data, None)
        else:
            return self._warn_report(data, None)

    def _warn_report(self, data, missing):
        return f"Dataframe does not meet the required number of levels \
            (levels {self._comparisson} {self._levels}"

# TODO: Make a mean value checker

