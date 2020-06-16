from typing import Dict, List, Union, Any


class _Builder():

    _piece: Dict[str, Union[List[Any], Dict[str, Any]]]

    def build_model(self, data):
        raise NotImplementedError()

    def set_piece(self, param, name, *args, **kwargs):

        self.check_name(param, name)

        self._piece[param] = {
            "name": name,
            "args": args,
            "kwargs": kwargs
        }

        return self

    def with_piece(self, param, name, *args, **kwargs):

        return self.copy().set_piece(param, name, *args, **kwargs)

    def _init_piece(self, params):
        self._piece = {
            p: {"name": None, "args": None, "kwargs": None}
            for p in params
        }

    def check_name(self, param, name):
        if param not in self._piece:
            raise KeyError(f"invalid parameter {param}, select one of \
                {self._piece.keys()}")

    def copy(self):
        ret = type(self)()
        ret._piece = self._piece
        return ret
