"""Define File IO classes

Files are external sources of data that can be processed in several ways as
raw data used in a graph.
"""
import sys
import io
import os

import pandas as pd

from dalio.interpreter import _Interpreter


class FileWriter(_Interpreter):
    """File string writer

    Attributes:
        _engine: any file instance that can be written on
    """

    engine: io.TextIOWrapper

    def __init__(self, file_io_eng=sys.stdout):
        """Initialize instance and set file

        Args:
            file_io_eng: output file or path to an existing file
        """
        super().__init__()
        self.engine = file_io_eng

    def write(self, data):
        """Write a request string onto a file"""

        self.engine.write(str(data))

    def clear(self, data):
        """Deletes engine"""
        del(self.engine)

    def __del__(self):
        """Close connections when deleted"""
        del(self.engine)
        super().__dell__()

    @property
    def engine(self):
        if self.engine is not None:
            return self.engine

        raise AttributeError("file io engine not set")

    @engine.setter
    def engine(self, newengine):
        """Set file io engine

        Set connection to opened file or open a new file given the path to
        one.

        Args:
            newengine: open file instance or path to an existing file.
        Raises:
            IOError: if specified path does not exist.
            TypeError: if specified "newengine" argument is of an
                invalid type
        """
        del(self.engine)

        if isinstance(newengine, io.TextIOWrapper) \
                or newengine is None:
            self.engine = newengine
        elif isinstance(newengine, str):
            if os.path.exists(newengine):
                self.engine = open(newengine)
            else:
                raise IOError(f"specified path {newengine}\
                    does not exist")
        else:
            raise TypeError(f"invalid connection type {type(newengine)}")

    @engine.deleter
    def engine(self):
        try:
            self.engine.close()
            self.engine = None
        except AttributeError:
            pass


class PandasInFile(External):
    """Get data from a file using the pandas package

    Attributes:
        engine (str): path to a file that can be read by some pandas
            function.
    """

    engine: str

    def __init__(self, in_file):
        """Initialize instance and check in_file

        Args:
            in_file: path to input file.

        Raises:
            IOError: if specified path does not exist.
            TypeError: if specified "in_file" argument is not a string
        """
        super().__init__()
        if isinstance(in_file, str):
            if os.path.exists(in_file):
                self.engine = in_file
            else:
                raise IOError(f"specified path {in_file} \
                    does not exist")
        else:
            raise TypeError(f"in_file must be a string with a path to the \
                input file")

    def request(self, **kwargs):
        """Get data input from a file according to its extension

        Args:
            **kwargs: arguments to the inport function.
        """
        ext = self.engine.split(".")[-1]
        if ext in ["xls", "xlsx"]:
            return pd.read_excel(
                self.engine,
                engine="xlrd",
                **kwargs
            )
        elif ext == "csv":
            return pd.read_csv(self.engine, **kwargs)

    def check(self):
        return self.engine is not None
