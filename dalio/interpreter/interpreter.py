"""Define the Interpreter abstract class"""

import warnings


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

    def __getattr__(self, name):
        """Implement functionality for when users try finding attributes that
        are not implemented by the interpreter but are by the engine.

        This will always throw a warning if the attribute is in fact part of
        the engine and an AttributeError if it doesn't exist in the engine
        either.

        Args:
            name (str): attribute name.

        Throws:
            AttributeError: if attribute does not exist in engine.
        """
        try:
            attr = self.engine.__getattribute__(name)
            warnings.warn(f"Attribute {name} is not implemented by \
                {type(self)} , returning attribute {name} from engine \
                {type(self.engine)}. This might not work with other engines, \
                consider implementing this attribute in the Interpreter",
                          RuntimeWarning)
            return attr
        except AttributeError:
            raise AttributeError(f"Attribute {name} is not implemented by \
                {type(self)} and not available in engine \
                {type(self.engine)}, either implement this attribute in the \
                Interpreter or use an engine which does")

    def init_attr(self, name, base_val, *args, data=None, **kwargs):
        """Universal way of initializing an interpreted attribute.

        This is necessary for packages that compose an engine of other package
        objects unknown to the user. It looks for any method implemented by
        the interpreter named init_{name} and calls it if it exists. If it
        doesn't, look for the attribute name and set it to {base_val}.

        Attribute initializers are not required in the abstract Interpreter
        instances as they are often a part of certain types of packages and
        not others and should thus be left as an option if that is the case.

        Args:
            name (str): attribute name
            base_val (any): base value for the attribute. Fallback in case
                attribute initialization method does not exist.
            data (any): data optionally used in attribute initialization
            *args, **kwargs: attribute initialization arguments.

        Throws:
            AttributeError: if neither attribute initialization or name
                exists.
        """
        try:
            self.__getattribute__("init_"+name)(base_val,
                                                *args,
                                                data=data,
                                                **kwargs)
        except AttributeError:
            # This will throw an error if attribute is not existent
            self.__getattribute__(name)
            self.__setattr__(name, base_val)

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
