"""Define the Interpreter abstract class"""


class _Interpreter:
    """The interpreter abstract class initializes the engine parameter as
    well as getter and setter methods for it.

    Subclasses should implements the mechanics based around the idea that
    interpreters should take in commands that interact with some engine in a
    standardized way.

    Attributes:
        _engine (any): engine which has actions performed on.
    """

    def __init__(self):
        self._engine = None

    def set_engine(self, engine):
        """Explicit setter for engine for chaining"""
        self.engine = engine
        return self

    @property
    def engine(self):
        """Getter method for engine. Checks if engine is set, if not, returns
        the default initializer
        """
        raise NotImplementedError()

    @engine.setter
    def engine(self, engine):
        """Setter method for engine. Checks if the new engine is of the
        correct type and sets it if it is.

        Args:
            engine (self._eng_type): new engine to be set.

        Raises:
            TypeError: if engine is not of the correct type.
        """
        raise NotImplementedError()

    def clear(self):
        """Clear interpreter"""
        raise NotImplementedError()
