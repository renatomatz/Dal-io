from dalio.validator import Validator


class IS_TYPE(Validator):

    _t: type

    def __init__(self, t):
        super().__init__()

        if isinstance(t, type):
            self._t = t
        else:
            ValueError(f"Invalid input type {type(t)} specify input of type\
                    {type(type)} instead")

    def validate(self, data):
        if isinstance(data, self._t):
            return True
        elif self._fatal:
            return self._error_report(TypeError, data)
        else:
            return self._warn_report(data)

    def _warn_report(self, data):
        return f"Data is of type {type(data)}, not {self._t}."


class ELEMS_TYPE(Validator):

    _t: type

    def __init__(self, t):
        super().__init__()

        if isinstance(t, type):
            self._t = t
        else:
            ValueError(f"Invalid input type {type(t)} specify input of type\
                    {type(type)} instead")

    def validate(self, data):
        for elem in data:
            if not isinstance(elem, self._t):
                if self._fatal:
                    return self._error_report(TypeError, elem)
                else:
                    return self._warn_report(elem)
        return True

    def _warn_report(self, data):
        return f"Data contains element of not of type {type(self._t)}"


class HAS_ATTR(Validator):

    _attr: str

    def __init__(self, attr):
        super().__init__()

        if isinstance(attr, str):
            self._attr = attr
        else:
            ValueError(f"Invalid input type {type(attr)} specify input of type\
                    {str} instead")

    def validate(self, data):
        if hasattr(data, self._attr):
            return True
        elif self._fatal:
            return self._error_report(TypeError, data)
        else:
            return self._warn_report(data)

    def _warn_report(self, data):
        return f"Data does not have attribute {self._attr}."
