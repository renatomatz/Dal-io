"""Classes for common Pipe functionalities"""

from dalio.pipe import Pipe

from dalio.util import process_cols

from dalio.validator import HAS_COLS, IS_PD_DF


class _ColSelection(Pipe):

    def __init__(self, cols):
        super().__init__()

        if isinstance(cols, dict):
            for level, col in cols.items():
                self._source\
                    .add_desc(HAS_COLS(col, level=level))
        elif isinstance(cols, (list, str)):
            self._source\
                .add_desc(HAS_COLS(cols))
        else:
            self._source\
                .add_desc(IS_PD_DF())

        self._cols = process_cols(cols)

    def copy(self, *args, **kwargs):
        return super().copy(
            *args,
            cols=self._cols,
            **kwargs
        )


class _ColValSelection(_ColSelection):

    def __init__(self, values, cols=None):
        super().__init__(cols)

        self._values = values

    def copy(self, *args, **kwargs):
        return super().copy(
            values=self._values,
            cols=self._cols,
        )


class _ColMapSelection(_ColSelection):

    def __init__(self, map_dict, level=None):
        cols = [*map_dict.keys()] if level is None \
            else {level: [*map_dict.keys()]}

        super().__init__(cols)

        self._map_dict = map_dict

    def copy(self, *args, **kwargs):
        return super().copy(
            map_dict=self._map_dict,
        )
