"""Defines the Pipe and PipeLine classes

Pipes are perhaps the most common classes in graphs and represent any
transformation with one input and one output. Pipes` main functionality
revolves around the .transform() method, which actually applies a
transformation to data retrieved from a source. Pipes must also implement
propper data checks by adding descriptions to their source.
"""

from typing import List

from dalio.base import _Transformer, _DataDef


class Pipe(_Transformer):
    """Pipes represend data modifications with one internal input and one 
    internal output.

    Attributes:
        _source (_DataDef): input data definition
    """

    _input: _DataDef

    def __init__(self):
        """Initialize instance and set up input DataDef.

        In Pipe instance initializations, data definitions are described
        and attributes are checked.
        """
        super().__init__()
        self._source = _DataDef()

    def run(self, **kwargs):
        """Get data from source, transform it, and return it

        This will often be left alone unless there are specific keyword
        arguments or checks done in addition to the actual transformation.
        Keep in mind this is rare, as keyword arguments are often required
        by Translators, and checks are performed by DataDefs.
        """
        return self.transform(self._source.request(**kwargs), **kwargs)

    def transform(self, data, **kwargs):
        """Apply a transformation to data returned from source.

        This is where the bulk of funtionality in a Pipe lies. And allows it
        to be highly customizable. This will often be the only method needed
        to be overwriten in subclasses.

        Args:
            data: data returned by source.
        """
        return data

    def pipeline(self, *args):
        """Returns a PipeLine instance with self as the input source and any
        other Pipe instances as part of its pipeline.

        Args:
            *args: any additional Pipe to be added to the pipeline, in that
                order.
        """
        return PipeLine(self, *args)

    def __add__(self, other):
        """Returns a Pipe object with this as its first stage.

        Keep in mind that order matters in this addition, and that while you
        will have access to the RHS`s attributes, your input will be set to
        the LHS`s inputs.
        """
        return other.with_input(self)

    def set_input(self, new_input):
        """Set the input data source in place.

        Args:
            new_input (_Transformer): new transformer to be set as input to
                source connection.

        Raises:
            TypeError: if new_input is not an instance of _Transformer.
        """
        if isinstance(new_input, _Transformer):
            self._source.set_connection(new_input)
        else:
            raise TypeError(f"new input must be an instance _Transformer \
                not {type(new_input)}")

        return self

    def with_input(self, new_input):
        """Return copy of this transformer with the new data source.
        """
        return self.copy().set_input(new_input)


class PipeLine(Pipe):
    """Collection of Pipe transformations.

    PipeLine instances represent multiple Pipe transformations being
    performed consecutively. Pipelines essentially execute multiple
    transformations one after the other, and thus do not check for data
    integrity in between them; so keep in mind that order matters and only
    the first data definition will be enforced.

    Attributes:
        pipeline (list): list of Pipe instaces this pipeline is composed of
    """

    pipeline: List[Pipe]

    def __init__(self, first, *args):
        """Initialize PipeLine with initial Pipe instances.

        Args:
            first (Pipe): first PipeLine pipe, which will form the basis for
                data input and checking for the rest of the PipeLine.
            *args: additional Pipe instances to be added to PipeLine.
        """
        super().__init__()
        self._source = first._source

        self._pipeline = [first]
        self.extend(*args)

    def transform(self, data, **kwargs):
        """Pass data sourced from first pipe through every Pipe`s
        .transform() method in order.

        Args:
            data: data sourced and checked from first source.
        """
        for pipe in self._pipeline:
            data = pipe.transform(data)

        return data

    def copy(self, *args, **kwargs):
        """Make a copy of this Pipeline"""
        ret = super().copy(*args, **kwargs)
        ret.extend(self)
        return ret

    def extend(self, *args):
        """Extend existing pipeline with one or more Pipe instances
        """
        for pipe in args:

            if isinstance(pipe, PipeLine):
                self._pipeline.extend(pipe.pipeline)
            elif isinstance(pipe, Pipe):
                self._pipeline.append(pipe)
            else:
                raise TypeError(f"arguments passed must be either of \
                    type Pipe or PipeLine, not {type(pipe)}")

        return self

    def __add__(self, other):
        """Add another Pipe or PipeLine to a copy of this instance

        Args:
            other (Pipe, PipeLine): instance to extend this.
        """
        return self.copy().extend(other)
