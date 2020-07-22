"""Define File IO classes

Files are external sources of data that can be processed in several ways as
raw data used in a graph.
"""
import sys
import io
import os

import pandas as pd

from dalio.interpreter import _Interpreter


class FileInterpreter(_Interpreter):
    """File string writer

    Attributes:
        _engine: any file instance that can be written on
    """

    def __init__(self, file_io_eng=sys.stdout):
        """Initialize instance and set file

        Args:
            file_io_eng: output file or path to an existing file
        """
        super().__init__()
        self.engine = file_io_eng

    def write(self, data, *args, **kwargs):
        """Write a request string onto a file"""
        self.engine.write(str(data), *args, **kwargs)

    def read(self, *args, **kwargs):
        """Read contents of the file"""
        return self.engine.read(*args, **kwargs)

    def clear(self):
        """Deletes engine"""
        del self.engine

    def __del__(self):
        """Close connections when deleted"""
        del self.engine

    @property
    def engine(self):
        if self._engine is not None:
            return self._engine

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
        del self._engine

        if isinstance(newengine, io.TextIOWrapper) \
                or newengine is None:
            self._engine = newengine
        elif isinstance(newengine, str):
            if os.path.exists(newengine):
                self._engine = open(newengine)
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


class PandasInterpreter(FileInterpreter):
    """Get data from a file using the pandas package"""

    def read(self, *args, **kwargs):

        ext = self.engine.name.split(".")[-1]

        if ext in ["xls", "xlsx"]:
            return pd.read_excel(
                self.engine,
                engine="xlrd",
                *args,
                **kwargs
            )
        elif ext == "csv":
            return pd.read_csv(self.engine, **kwargs)

        raise IOError(f"Invalid file extension, {ext}")

    def write(self, data, *args, **kwargs):

        name = self.engine.name
        ext = name.split(".")[-1]

        if ext in ["xls", "xlsx"]:
            data.to_excel(name, *args, **kwargs)
        elif ext == "csv":
            data.to_csv(name, *args, **kwargs)

        raise IOError(f"Invalid file extension, {ext}")
