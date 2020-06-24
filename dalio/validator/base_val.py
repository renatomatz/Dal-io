"""Define Validators used for general python objects"""


from typing import Union, Tuple
from dalio.validator import Validator


class IS_TYPE(Validator):
    """Checks if data is of a certain type

    Attribute:
        t (type): type of data to check for
    """

    _t: type

    def __init__(self, t):
        """Initialize instance

        Raises:
            ValueError: argument "t" is not of type type
        """
        super().__init__()

        if isinstance(t, (type, tuple)):
            self._t = t
        else:
            ValueError(f"Invalid input type {type(t)} specify input of type\
                    {type(type)} instead")

    def validate(self, data):
        """Validates data if it is of type self._t"""
        if isinstance(data, self._t):
            return None
        else:
            return f"Data is of type {type(data)}, not {self._t}."


class HAS_ATTR(Validator):
    """Checks if data has an attribute

    Attributes:
        _attr (str): attribute to check for
    """

    _attr: str

    def __init__(self, attr):
        """Initialize instance

        Raises:
            ValueError: argument "attr" is not a string or list of strings
        """
        super().__init__()

        if isinstance(attr, str):
            self._attr = [attr]
        elif isinstance(attr, list):
            self._attr = attr
        else:
            ValueError(f"Invalid input type {type(attr)} specify input of \
                type {str} instead")

    def validate(self, data):
        """Validates data if it contains attribute self._attr"""
        for attr in self._attr:
            if hasattr(data, attr):
                return None
            else:
                return f"Data does not have attribute {self._attr}."


class ELEMS_TYPE(HAS_ATTR):
    """Checks if all elements of an iterator is of a certain type.

    Attributes:
        _t (type, tuple): type to check iterator's elements for
    """

    _t: Union[type, Tuple[type]]

    def __init__(self, t):
        """Initialize instance, set default super initializer

        Raises:
            ValueError: argument "t" is not of type type
        """
        super().__init__("__iter__")

        if isinstance(t, (type, tuple)):
            self._t = t
        else:
            ValueError(f"Invalid input type {type(t)} specify input of type\
                    {type(type)} instead")

    def validate(self, data):
        """Validates data if it is an iterable with all elements of type
        self._t
        """
        super_err = super().validate(data)
        if super_err is not None:
            return super_err

        for elem in data:
            if not isinstance(elem, self._t):
                return f"Data contains element of not of type {type(self._t)}"
        return None
