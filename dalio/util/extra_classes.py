from typing import Dict, List, Union, Any


class _Builder():

    _base_type: type
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

    def _init_piece(self, params):
        self._piece = {
            p: {"name": None, "args": None, "kwargs": None}
            for p in params
        }

    def check_name(self, param, name):
        if param not in self._piece:
            raise KeyError(f"invalid parameter {param}, select one of \
                {self._piece.keys()}")
