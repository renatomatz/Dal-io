"""Define extra utility classes used throughout the package

These classes implement certain interfaces used in specific cases and are not
constrained an object's parent class.
"""

from typing import Dict, List, Union, Any

from collections import namedtuple

from dalio.interpreter import _Interpreter


class _Factory:
    """Interface for setting and assembling pieces onto an _Interpreter.

    Builder instances are meant, as the name suggests, to be set and built at
    command. This is necessary in the context of graphs as the vast majority
    of settings (pieces) should be specified before actually running the
    code, as these running parameters would get overwhelming as the graph is
    built to some arbitrary length. This is different from contexts where
    various running parameters can be set for predictable classes or
    functions. Builders are commonly used to choose from possible functions
    and set arguments for it to be ran with before actually running them

    Attributes:
        piece (type): nametuple singleton for piece generation
        _interpreter (Interperter): object implementing all building functions
            over some engine.
        _pieces (dict): dictionary containing a piece's name, positional
            arguments and keyword arguments.
    """

    piece = namedtuple("piece",
                       "name, args, kwargs",
                       module="_Factory",
                       defaults=[str(), list(), dict()])

    _pieces: Dict[str, Union[List[Any], Dict[str, Any]]]

    def __init__(self):
        self._interpreter = None
        self._pieces = None

    def build(self, data, **kwargs):
        """Assemble pieces into a model given some data

        The data will opten be optional, but several builder models will
        require it to be fitted on initialization. Which further shows why
        builders are necessary for context-agnostic graphs.

        Args:
            data: data that might be used to build the model.
            **kwargs: any additional argument used in building
        """
        raise NotImplementedError()

    @property
    def interpreter(self):
        if self._interpreter is not None:
            return self._interpreter

        raise AttributeError("Interpreter is not set")

    @interpreter.setter
    def interpreter(self, interpreter):
        """Set the interpreter to be built on

        Args:
            interpreter (Interpreter): interpreter to build on engine.

        Raises:
            TypeError: when interpreter is not of type <class type> or
                <class _Interpreter>.
        """
        if isinstance(interpreter, _Interpreter):
            self._interpreter = interpreter
        else:
            raise TypeError(f"new interpreter must be of type {type} or \
                    {_Interpreter}, not {type(interpreter)}")

    def set_interpreter(self, interpreter):
        """Wrapper for chaining"""
        self.interpreter = interpreter
        return self

    def set_piece(self, param, name, *args, **kwargs):
        """Set a piece name, positional arguments and keyword arguments

        Names and parameters are checked before this is performed and might
        cause exceptions to be raised. See the .check_name() method for more
        information on this process.

        Args:
            param (str): piece name set as the piece dict key. Not to be
                confused with the name parameter, which is a selection from
                available options for a certain piece of name {param}.
            name (str): name of a piece option.
            *args: piece positional arguments.
            **kwargs: piece keyword arguments.
        """

        if param not in self._pieces:
            raise KeyError(f"invalid parameter {param}, select one of \
                {self._pieces.keys()}")
        if name is None:
            raise ValueError("Please specify a valid name")

        self._pieces[param] = self.piece(name, args, kwargs)

        return self

    def update_pieces(self, new_pieces):
        """Update pieces by specifying a dictionary

        Args:
            new_pieces (dict): dictionary containing only keys in the
                current object and _Factory.piece values.

        Raises:
            ValueError: if any of the above characteristics of
                the new_pieces argument is violated.
        """
        if not all([key in self._pieces.keys()
                    and isinstance(val, _Factory.piece)
                    for key, val in new_pieces.items()]):

            raise ValueError(f"new pieces must have only {_Factory.piece} \
                instances as values with keys matching this object's")

        self._pieces.update(new_pieces)
        return self

    def _init_piece(self, params):
        """Initialize piece dictionary given a list of piece names.

        This is done upon initialization of a builder instance and sets all
        of the piece dictionary names and default dictionary.

        Args:
            params (list): list of piece names to be initialized.
        """
        self._pieces = {p: self.piece() for p in params}
