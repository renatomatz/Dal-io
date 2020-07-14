"""Define Transformer class

Transformers are a base class that represents any kind of data modification.
These interact with DataOrigin instances as they are key to their input and
output integrity. A set_source() method sets the source of the input, the
.run() method cannot be executed if the input"s source is not set.
"""
from typing import Set
from dalio.base import _Node


class _Transformer:
    """Represents transformations of data

    Characterized by the number and origin of inputs and outputs.

    Attributes:
        _source (_Node): The source _Node instance used to request data used
            in transformation. This attribute represents data input, though
            we will refer to "input" as the _Node instance`s input.
        _req_args (set): A set of unique required keyword arguments for this
            and previous transformers. This represents any value you want
            users to have full and flexible control over. This will often
            only be set for early _Transformer instances.
        _tags (set)
    """

    _source: _Node
    _req_args: Set[str]
    _tags: Set[str]

    def __init__(self):
        """Initializes Transformer object by creating necessary _Node
        instances and defining their data.
        """
        self._source = None
        self._req_args = set()
        self._tags = set()

    def run(self, **kwargs):
        """Gets data from source and run the transformation

        Returns:
            Sourced data transformed.
        """
        raise NotImplementedError()

    def copy(self, *args, **kwargs):
        """Makes a copy of transformer, copying its attributes to a new
        instance.

        This copy should essentially create a new transformation node, not an
        entire new graph, so the _source attribute of the returned instance
        should be assigned without being copied. This is also made to be built
        upon by subclasses, such that only new attributes need to be added to
        a class' copy method.

        Arguments:
            *args: Positional arguments to be passed to initialize copy
            **kwargs: Keyword arguments to be passed to initialize copy

        Returns:
            A copy of this _Transformer instance with copies of necessary
            attributes and empty input.
        """
        raise NotImplementedError()

    def add_tag(self, new_tag):
        """Add new tag(s) to set

        Arguments:
            new_tag (str, iterable): new tag or iterable of tags.
        """
        if isinstance(new_tag, str):
            self._tags.add(new_tag)
        elif hasattr(new_tag, "__iter__"):
            self._tags.update(new_tag)
        else:
            raise TypeError(f"new tags must be strings or an iterable of \
                strings, not {type(new_tag)}")

        return self

    def get_input(self):
        """Get input connection"""
        return self._source.get_connection()

    def set_input(self, new_input):
        """Set the input connection to the _source attribute.

        Returns:
            Self with new input.
        """
        raise NotImplementedError()

    def with_input(self, new_input):
        """Return copy of this transformer with the new input connection.

        Returns:
            Copy of self with new input.
        """
        raise NotImplementedError()

    def req_args(self):
        """Return the unique arguments needed for this and input`s .run()
        methods.

        Returns:
            A set of keyword argument needed.
        """
        return set(self._req_args).union(self.get_input().req_args())

    def __call__(self, new_input):
        """Alternative interface for with_input()."""
        return self.with_input(new_input)
