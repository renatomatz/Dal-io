'''Define Application class'''

from typing import Dict
from dalio.model import Model
from dalio.external import External

# TODO: better way of informing of missing output

class Application(Model):
    '''While Models are normally the last stage of the processing chain, it 
    still has a single output, which might have limited value in itself. 
    Applications are tools used for the interpretation of some input and 
    outisde outputs. These can have a broad range of uses, from graphing to 
    real-time trading. The main functionality is in the .run() method, which 
    gets input data and interprets it as needed. 

    === Attributes ===

    _out: dictionary of outisde output connections

    === Methods ===

    run: execute application returning whether or not it was successful
    - return: True if execution was soccessful, False if not

    set_output: set the specified output connection

    with_output: return a copy of this instance with a given External as
    the output to the specified dictionary entry
    - return: Transformer

    '''
    _out: Dict[str, External]

    def __init__(self):
        super().__init__()
        self._out = {}

    def copy(self):
        ret = super().copy()
        ret._out = self._out.copy()
        return ret

    def _get_output(self, output_name):
        try:
            return self._out[output_name]
        except KeyError:
            KeyError(f"{output_name} is not a valid output, select one of\
                {self._out.keys()}")

    def set_output(self, output_name, new_output):
        '''Set a new output to data definition in dictionary entry
        matching the name
        '''
        if output_name not in self._out:
            raise KeyError()  # TODO: Make better exceptions
        elif not isinstance(new_output, External):
            raise ValueError()
        else:
            self._out[output_name] = new_output
        return self

    def with_output(self, output_name, new_output):
        '''Return a copy of this model with the specified data definition
        output changed
        '''
        return self.copy().set_output(output_name, new_output)

    def _init_output(self, outputs):
        if isinstance(outputs, list):
            s_dict = {}
            for out in outputs:
                if isinstance(out, str):
                    s_dict[out] = None
                else:
                    raise ValueError("output names must be strings")
            self._out = s_dict
        else:
            raise ValueError("please specify a list of strings to the \
                    outputs argument")
