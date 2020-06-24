"""Indexing transformations"""
from typing import List

from dalio.pipe import Pipe
from dalio.validator import IS_PD_TS, IS_PD_DF
from dalio.util import process_cols
from dalio.ops import index_cols


class Index(Pipe):
    """Index data at a specified value

    Args:
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
