"""Define Model class

Models are transformers that take in multiple inputs and has a single output.
Model instance can be much more flexible with additional options for differen
strategies of data processing and collection.
"""

from typing import Dict
from dalio.base import _Transformer, _DataDef


class Model(_Transformer):
    """Models represent data modification with multiple internal inputs and a
    single internal output.

    Attributes:
        _source: dictionary of input data definitions
    """

    _source: Dict[str, _DataDef]

    def __init__(self):
        """Initializes base transformer and sets up source dict.

        In Model instance initialization, the source keys are initialized and
        their respective data definitions are described.
        """
        super().__init__()
        self._source = {}

    def run(self, **kwargs):
        """Run model.

        This will be the bulk of subclass functionality. It is where all
        data is sourced and processed.
        """
        raise NotImplementedError()

    def copy(self, *args, **kwargs):
        ret = type(self)(*args, **kwargs)
        for name, datadef in self._source.items():
            ret.set_input(name, datadef.get_connection())
        return ret

    def set_input(self, source_name, new_input):
        """Set a new connection to a data definition in dictionary entry
        matching the key name.

        Args:
            source_name (str): initialized item in sources dict.
            new_input: new source connection.

        Raise:
            KeyError: if input name is not present in sources dict.
        """
        if source_name in self._source:
            if isinstance(new_input, _Transformer) or new_input is None:
                self._source[source_name].set_connection(new_input)
            else:
                raise TypeError(f"new input must be a _Transformer \
                    instance, not {type(new_input)}")
        else:
            raise KeyError(f"{source_name} is not a valid source")

        return self

    def with_input(self, source_name, new_input):
        """Return a copy of this model with the specified data definition
        connection changed

        Args:
            source_name (str): initialized item in sources dict.
            new_input: new source connection.
        """
        return self.copy().set_input(source_name, new_input)

    def _source_from(self, source_name, **kwargs):
        """Helper function to get data from a specified source

        Args:
            source_name (str): initialized item in sources dict.

        Raises:
            KeyError: if input name is not present in sources dict.
        """
        if source_name in self._source:
            return self._source[source_name].request(**kwargs)
        else:
            raise KeyError(f"{source_name} not present in source dict, pick \
            one of {self._source.keys()}")

    def _get_source(self, source_name):
        """Get a source data definition

        Args:
            source_name (str): initialized item in sources dict.

        Raises:
            KeyError: if input name is not present in sources dict.
        """
        if source_name in self._source:
            return self._source[source_name]
        else:
            raise KeyError(f"specified source not in source list, specify \
                    one of: {self._source.keys()}")

    def _init_source(self, sources):
        """Initialize sources

        This internal method takes in an iterable of source names and
        initializes the Model instance's source dict. Only sources
        initialized this way, explicitly on initialization can be accessed by
        other methods.

        Args:
            sources (iterable): contains keys for the source dict.

        Raises:
            TypeError: if the sources argument is not an iterable
        """
        if hasattr(sources, "__iter__"):
            for source in sources:
                self._source[source] = _DataDef()
        else:
            raise TypeError("please specify an iterable to the \
                    sources argument")

    def __call__(self, source_name, new_input):
        """Alternative interface for with_input()."""
        return self.with_input(source_name, new_input)
