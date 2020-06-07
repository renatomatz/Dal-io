from dalio.validator import Validator


class HAS_DIMS(Validator):

    _dims: int

    def __init__(self, dims):
        super().__init__()

        if isinstance(dims, int):
            self._dims = dims
        else:
            raise TypeError(f"argument dims must be of type {int}, \
                not {type(dims)}")
