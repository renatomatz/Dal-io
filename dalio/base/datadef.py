"""Defines DataDef base class

DataDef instances describe data inputs throughout the graph and ensure the
integrity of data continuously. These are composed of various validators that
serve both to describe approved data and check for whether data passes a test.
"""

import concurrent.futures

from typing import List
from dalio.base import _Node
from dalio.validator import Validator


class _DataDef(_Node):
    """Define input data

    Node used to represent input data coming from a _Transformer instance.
    Used to check for the integrity of this data and for any characteristics
    necessary for a particular analysis.

    Attributes:
        _connection (_Transformer): Transformer instance which outputs data to
            be checked.
        _desc (list): list of Validator instances that describe approved data and
            tests input data for certain characteristics.
    """

    def __init__(self, parallel=False):
        """Initialize DataDef instance"""
        super().__init__()
        self.parallel = parallel
        self._desc = []

    def add_desc(self, desc):
        """Add single or multiple description validator(s)

        Arguments:
            desc (Validator, iterable): validator or list of validators to add

        Returns:
            Self with the new description. Allows for layering.
        """
        desc = desc if hasattr(desc, "__iter__") else [desc]
        for description in desc:
            if isinstance(description, Validator):
                self._desc.append(description)
            else:
                raise TypeError(f"New description must be of \
                    type {Validator} or an iterator of such, not \
                    {type(desc)}")

        return self

    def clear_desc(self):
        """Clear descriptions

        Returns:
            Old description Validator list.
        """
        ret, self._desc = self._desc, []
        return ret

    def request(self, **kwargs):
        """Get input data and check for validity

        Returns:
            Data if no fatal validators fail.

        Raises:
            Exception: Error thrown by specific Validator in case of invalid
                data and being set to "fatal"
        """

        if self._connection is None:
            return None

        return self.check(run_kwargs=kwargs)

    def check(self, **kwargs):
        """Pass data through validator list.

        Used to check for data integrity or create a detailed report on
        specific failed tests.

        Returns:
            Nothing. Prints warnings if data is invalid and description is not
            fatal.

        Raises:
            Exception: Error thrown by specific Validator in case of invalid
                data and being set to "fatal"
        """
        data = self._connection.run(**kwargs.get("run_kwargs", {}))

        report = {
            "warning": [],
            "exception": []
        }
        errors = 0

        if self.parallel:
            with concurrent.futures.ProcessPoolExecutor() as executor:

                # create a futures instance for every validator
                future_to_desc = {
                    executor.submit(desc.validate, data): desc
                    for desc in self._desc
                }

                for future in concurrent.futures.as_completed(future_to_desc):
                    desc = future_to_desc[future]

                    err_kind = "exception" if desc.fatal else "warning"
                    err = future.result()

                    if err is not None:
                        report[err_kind].append(err)
                        errors += 1

        else:
            for desc in self._desc:
                err_kind = "exception" if desc.fatal else "warning"
                err = desc.validate(data)

                if err is not None:
                    report[err_kind].append(err)
                    errors += 1

        if errors > 0:
            _print_report(report)

        if len(report["exception"]) > 0:
            raise Exception()  # TODO: Create custom exception

        return data

    def describe(self):
        """Print out validator descriptions
        """
        super().describe()
        print("Validators:\n")
        for val in self._desc:
            print(f"- {type(val)}: {val.test_desc}\n")

    def copy(self, keep_connection=True):
        """Make deep copy of instance, with option to remove original input

        Returns:
            Another instance of this class with same descriptions.
        """
        ret = type(self)()
        ret.set_connection(self._connection if keep_connection else None)
        ret.add_desc(self._desc.copy())
        return ret

    def __add__(self, other):
        """Add two instance's descriptions or one description to an instance

        Returns:
            Instance of class with descriptions from self and other
        """

        ret = self.copy()

        if isinstance(other, Validator):
            ret._desc.append(other)
        elif isinstance(other, _DataDef):
            ret._desc.extend(other._desc)
        else:
            raise TypeError(f"Other must be either of type {Validator} or \
                {_DataDef}, not {type(other)}")

        return ret


def _print_report(report):
    print(report)
