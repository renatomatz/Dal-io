"""Define File IO classes

Files are external sources of data that can be processed in several ways as
raw data used in a graph.
"""
import sys
import io
import os

import pandas as pd

from dalio.external import External


class FileWriter(External):
    """File string writer

    Attributes:
        _connection: any file instance that can be written on
    """

    _connection: io.TextIOWrapper

    def __init__(self, out_file=sys.stdout):
        """Initialize instance and set file

        Args:
            out_file: output file or path to an existing file
        """
        super().__init__()
        self.set_connection(out_file)

    def request(self, **kwargs):
        """Write a request string onto a file"""
        if not self.check():
            raise ValueError("Please set a connection before making a\
                request")

        self._connection.write(str(kwargs["query"]))

    def check(self):
        """Check if there is an open file as the connection"""
        # TODO: make this more solid to ensure file can be written to
        return self._connection is not None

    def __del__(self):
        """Close connections when deleted"""
        if self._connection is not None:
            self._connection.close()

    def set_connection(self, new_connection):
        """Set current connection

        Set connection to opened file or open a new file given the path to
        one.

        Args:
            new_connection: open file instance or path to an existing file.
        Raises:
            IOError: if specified path does not exist.
            TypeError: if specified "new_connection" argument is of an
                invalid type
        """
        old = self._connection

        if isinstance(new_connection, io.TextIOWrapper) \
                or new_connection is None:
            self._connection = new_connection
        elif isinstance(new_connection, str):
            if os.path.exists(new_connection):
                self._connection = open(new_connection)
            else:
                raise IOError(f"specified path {new_connection}\
                    does not exist")
        else:
            raise TypeError(f"invalid connection type {type(new_connection)}")

        if old is not None:
            old.close()


class PandasInFile(External):
    """Get data from a file using the pandas package

    Attributes:
        _connection (str): path to a file that can be read by some pandas
            function.
    """

    _connection: str

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
                self._connection = in_file
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
        ext = self._connection.split(".")[-1]
        if ext in ["xls", "xlsx"]:
            return pd.read_excel(
                self._connection,
                engine="xlrd",
                **kwargs
            )
        elif ext == "csv":
            return pd.read_csv(self._connection, **kwargs)

    def check(self):
        return self._connection is not None
