from dalio.pipe import Pipe
from dalio.validator import HAS_COLS
from dalio.util import process_cols


class ColSelect(Pipe):
    
    def __init__(self, cols=None):
        super().__init__()
        
        self._source\
            .add_desc(HAS_COLS(cols))

        self._cols = process_cols(cols)

    def transform(self, data, **kwargs):
        return data[self._cols] if self._cols is not None else data

    def copy(self):
        ret = type(self)(
            cols=self._cols
        )
        return ret
