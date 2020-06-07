'''Defines DataDef base class
'''

import concurrent.futures

from typing import Any, List
from dalio.base import _Node
from dalio.validator import Validator
from dalio.util import _print_warn_report, _print_error_report


class _DataDef(_Node):
    '''DataDef types are perhaps the most important piece of models, as they
    describe data inputs throughout the graph and ensure the integrity of
    data continuously.


    === Attributes ===

    - _connection: connection to data. DataDef instances only use these as
    data inputs and they must be Transformer instances

    - _desc: list of Validator instances

    === Methods ===

    request: request data from an input transformer

    copy: make a deep copy of the validator dictionary and any other attributes

    __add__: add another DataDef instance validators to your own

    '''

    _input: _Node
    _desc: List[Validator]
    tags: List[str]

    def __init__(self, tags=None):
        super().__init__()
        self._desc = []

    def add_desc(self, desc):
        desc = desc if hasattr(desc, "__iter__") else [desc]
        for d in desc:
            if isinstance(d, Validator):
                # TODO: implement check to ensure desc uniqueness
                self._desc.append(d)
            else:
                raise ValueError()  # TODO: propper exceptions
        return self

    def clear_desc(self):
        ret, self._desc = self._desc, []
        return ret

    def request(self, **kwargs):
        '''Get input data and check for validity
        Return data if all is good
        Validators will throw errors if needed
        '''

        in_data = self._connection.run(**kwargs)
        if self.check(in_data):
            return in_data

    def check(self, data):
        '''Pass data through validator list, return if all validators return
        True
        '''

        warning_report = {}
        exception_report = {}
        passed = True

        # TODO: parallel checking is not working as it seems like a copy of the 
        # [LARGE] dataset is being assigned to each process, make the parallelization
        # work with large datasets please

        # with concurrent.futures.ProcessPoolExecutor() as executor:

        #     # create a futures instance for every validator to run in parallel
        #     future_to_desc = {
        #         executor.submit(desc.validate, data): desc
        #         for desc in self._desc
        #     }

        #     for future in concurrent.futures.as_completed(future_to_desc):
        #         desc = future_to_desc[future]
        #         desc_name = type(desc).__name__
        #         try:
        #             warn = future.result()
        #             if isinstance(warn, str):
        #                 if desc_name in warning_report:
        #                     warning_report[desc_name].append(warn)
        #                 else:
        #                     warning_report[desc_name] = [warn]
        #         except Exception as exc:
        #             if desc_name in exception_report:
        #                 exception_report[desc_name].append(exc)
        #             else:
        #                 exception_report[desc_name] = [exc]

        for desc in self._desc:
            if desc.is_on:
                desc_name = type(desc).__name__
                try:
                    warn = desc.validate(data)
                    if isinstance(warn, str):
                        if desc_name in warning_report:
                            warning_report[desc_name].append(warn)
                        else:
                            warning_report[desc_name] = [warn]
                except Exception as exc:
                    if desc_name in exception_report:
                        exception_report[desc_name].append(exc)
                    else:
                        exception_report[desc_name] = [exc]

        if len(warning_report) > 0:
            _print_warn_report(warning_report)
            passed = False

        if len(exception_report) > 0:
            _print_error_report(exception_report)
            raise Exception()  # TODO: Create propper exceptions

        return passed

    def describe(self):
        '''Print out validator descriptions and tags
        '''
        super().describe()
        print("Validators:\n")
        for val in self._desc:
            print(f"- {type(val)}: {val.test_desc}\n")

    def copy(self, keep_input=True):
        '''Make deep copy of instance, with option to remove original input
        '''
        ret = type(self)()
        ret.set_connection(self._input if keep_input else None)
        ret.add_desc(self._desc.copy())
        ret.tags = self.tags.copy()
        return ret

    def add_tags(self, tags):
        if isinstance(tags, str):
            self.tags.append(tags)
        elif hasattr(tags, "__iter__"):
            self.tags.extend(tags)
        else:
            raise ValueError("Argument tags must be either a string or\
                iterable with strings")
        return self

    def __add__(self, other):
        '''Add Validators to own
        '''

        ret = self.copy()

        if isinstance(other, Validator):
            ret._desc.append(other)
        elif isinstance(other, _DataDef):
            ret._desc.extend(other._desc)
        else:
            raise TypeError()

        return ret
