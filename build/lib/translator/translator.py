"""Define Translator class

Translators are the root of all data that feeds your graph. Objects of this
take in data from some external source then "translates" it into a format that
can be used universaly by other elements in this package. Please consult the
translation manual to make this as usabel as possible and make extensive use
of the base tools to build translations.
"""

from typing import Dict

from dalio.base import _Transformer
from dalio.external import External


class Translator(_Transformer):
    """

    Attributes:
        _source: Connection used to retrieve raw data from outide source.
        translations: dictionary of translations from vocabulaary used in the
            data source to base constants. These should be created from
            initialization and kept unmodified. This is to ensure data coming
            through a translator is though of before usage to ensure
            integrity.
    """

    _source: External
    translations: Dict[str, str]

    def __init__(self):
        """Initialize instance"""
        super().__init__()
        self.translations = {}

    def copy(self, *args, **kwargs):
        ret = type(self)(*args, **kwargs)
        ret.set_input(self.get_input())
        ret.add_tag(self._tags)
        return ret

    def set_input(self, new_input):
        """See base class"""
        if isinstance(new_input, External):
            self._source = new_input
        else:
            raise ValueError("new input must be of type External")
        return self

    def with_input(self, new_input):
        """See base class"""
        if isinstance(new_input, External):
            ret = type(self)()
            ret._source = new_input
            ret.update_translations(self.translations)
        else:
            raise ValueError("new input must be of type External")
        return ret

    def update_translations(self, new_translations):
        """Update translations dictionary with new dictrionary"""
        if isinstance(new_translations, dict):
            self.translations.update(new_translations)
        else:
            raise ValueError("new translations must be of type dict")

    def translate_item(self, item):
        """Translate all items of an iterable

        Args:
            item (dict, any): item or iterator of items to translate.

        Return:
            A list with the translated names.
        """
        if hasattr(item, "__iter__"):
            return [self.translations.get(elem, elem) for elem in item]
        else:
            return self.translations.get(item, item)
