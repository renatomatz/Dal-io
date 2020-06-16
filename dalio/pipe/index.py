from dalio.base.constants import TICKER
from dalio.pipe import Pipe
from dalio.validator import IS_PD_TS, IS_PD_DF, HAS_COLS
from dalio.util import process_cols, index_cols

from typing import List


class Index(Pipe):

    index_at: int
    _cols: List[str]
    _groupby: List[str]

    def __init__(self, index_at, cols=None, groupby=None):
        super().__init__()

        self._source\
            .add_desc(IS_PD_TS())\
            .add_desc(IS_PD_DF())

        self.index_at = index_at
        self._cols = process_cols(cols)
        self._groupby = process_cols(groupby)

    def transform(self, data, **kwargs):

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

    def copy(self):
        ret = type(self)(
            self.index_at,
            cols=self._cols,
            groupby=self._groupby
        )
        return ret


class IndexStock(Index):

    def __init__(self, cols=None):
        super().__init__(
                index_at=100,
                cols=cols
        )

        self._source\
            .add_desc(HAS_COLS(TICKER))
