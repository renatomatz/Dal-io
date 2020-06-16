'''Define Translator class'''

from typing import Dict

from dalio.base import _Transformer
from dalio.external import External


class Translator(_Transformer):
    '''Translators are the root of all data that feeds your graph. Objects of 
    this take in data from some external source then "translates" it into a 
    format that can be used universaly by other elements in this package.
    Please consult the translation manual to make this as usabel as possible 
    and make extensive use of the base tools to build translations.

    === Attributes ===

    _source: Connection used to retrieve raw data from outide source

    translations: dictionary of translations from vocabulaary used in the
    data source to base constants. These should be created from initialization
    and kept unmodified. This is to ensure data coming throug ha translator
    is though of before usage to ensure integrity.

    === Methods ===

    make: factory for a new translator that inherits this translator's 
    _input and translations

    '''

    _input: External
    translations: Dict[str, str]

    def __init__(self):
        super().__init__()
        self.translations = {}

    def make(self, name):
        '''Make a new instance of an object instance of the Translator class
        
        === Parameters ===
        name: name of the new subclass to be creted.
        '''
        raise NotImplementedError()

    def set_input(self, new_input):
        if isinstance(new_input, External):
            self._source = new_input
        else:
            raise ValueError("new input must be of type External")
        return self

    def with_input(self, new_input):
        if isinstance(new_input, External):
            ret = type(self)()
            ret._source = new_input
            ret.update_translations(self.translations)
        else:
            raise ValueError("new input must be of type External")
        return ret

    def update_translations(self, new_translations):
        if isinstance(new_translations, dict):
            self.translations.update(new_translations)
        else:
            raise ValueError("new translations must be of type dict")

    def translate_item(self, item):
        '''Translate all items of an iterable and return a list with the modified
        names
        '''

        if hasattr(item, "__iter__"):
            return [self.translations.get(elem, elem) for elem in item]
        else:
            return self.translations.get(item, item)
