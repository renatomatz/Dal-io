"""Definte validators applied to array-like inputs
"""


from dalio.validator import Validator


class HAS_DIMS(Validator):
    """Check if an array has a number of dimensions

    Attributes:
        _dims (int): number of dimensions
        _comparisson (str): which comparisson to perform
    """

    _dims: int

    def __init__(self, dims, comparisson="=="):
        """Initialize Validator"""
        super().__init__()

        if isinstance(dims, int):
            self._dims = dims
        else:
            raise TypeError(f"argument dims must be of type {int}, \
                not {type(dims)}")

        self._comparrison = comparisson

    def validate(self, data):
        # TODO: implement this
        return None
