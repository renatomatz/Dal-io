"""Define the Application class

While Models are normally the last stage of the processing chain, it
still has a single output, which might have limited value in itself.
Applications are tools used for the interpretation of some input and
outisde outputs. These can have a broad range of uses, from graphing to
real-time trading. The main functionality is in the .run() method, which
gets input data and interprets it as needed.
"""

from typing import Dict
from dalio.model import Model
from dalio.external import External


class Application(Model):
    """Represent final representation of graph data through external entities.

    Applications are transformations with one or more internal inputs and one
    or more external outputs.

    Attributes:
        _out (dict): dictionary of outisde output connections
    """
    _out: Dict[str, External]

    def __init__(self):
        """Initialize instance.

        Application instance initializations serve for the same purposes as
        parent classes as well as initializing outputs. This is done the same
        way one would initialize sources thorugh the _init_output() method.
        """
        super().__init__()
        self._out = {}

    def run(self, **kwargs):
        """Run application.

        This will be the bulk of subclass functionality. It is where all
        data is sourced, processed and output.
        """
        raise NotImplementedError()

    def copy(self, *args, **kwargs):
        ret = super().copy(*args, **kwargs)
        ret._out = self._out.copy()
        return ret

    def _get_output(self, output_name):
        """Get the External instance set to an output name.

        Args:
            output_name (str): the name of the output from the output dict.

        Raises:
            KeyError: if name is not in the output dict.
        """
        if output_name in self._out:
            return self._out[output_name]
        else:
            raise KeyError(f"{output_name} is not a valid output, select one \
                of {self._out.keys()}")

    def set_output(self, output_name, new_output):
        """Set a new output to data definition in dictionary entry
        matching the name

        Args:
            output_name (str): the name of the output from the output dict.
            new_output: new External source to be set as the output.

        Raises:
            KeyError: if name is not in the output dict.
            ValueError: if the new output is not an instance of External.
        """
        if output_name not in self._out:
            raise KeyError(f"{output_name} is not a valid output, select one \
                of {self._out.keys()}")
        elif not isinstance(new_output, External):
            raise ValueError(f"new output must be an instance of type \
                External, not {type(new_output)}")
        else:
            self._out[output_name] = new_output
        return self

    def with_output(self, output_name, new_output):
        """Return a copy of this model with the specified data definition
        output changed

        Args:
            output_name (str): the name of the output from the output dict.
            new_output: new External source to be set as the output.
        """
        return self.copy().set_output(output_name, new_output)

    def _init_output(self, outputs):
        """Initialize outputs

        This internal method takes in a list of output names and initializes
        the Application instance`s output dict. Only outputs initialized this
        way, explicitly on initialization can be accessed by other methods.

        Args:
            outputs (list): list of strings that will serve as keys in the
                output dict.

        Raises:
            TypeError: if the outputs argument is not a list containing only
                strings.
        """
        if isinstance(outputs, list):
            s_dict = {}
            for out in outputs:
                if isinstance(out, str):
                    s_dict[out] = None
                else:
                    raise TypeError("output names must be strings")
            self._out = s_dict
        else:
            raise TypeError("please specify a list of strings to the \
                    outputs argument")

# TODO: better way of informing of missing output
