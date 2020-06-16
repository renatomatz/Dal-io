import sys
import io
import os

import pandas as pd

from dalio.external import External


class FileWriter(External):

    _connection: io.TextIOWrapper

    def __init__(self, out_file=sys.stdout):
        super().__init__()
        self.set_connection(out_file)

    def request(self, **kwargs):
        if self._connection is None:
            raise ValueError("Please set a connection before making a\
                request")

        self._connection.write(str(kwargs["query"]))

    def __del__(self):
        if self._connection is not None:
            self._connection.close()

    def set_connection(self, new_connection):
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
            raise ValueError(f"invalid connection type {type(new_connection)}")

        if old is not None:
            old.close()


class PandasInFile(External):

    _connection: str
    ext: str

    def __init__(self, in_file):
        if isinstance(in_file, str):
            self.set_connection(in_file)
            self.ext = in_file.split(".")[-1]

    def request(self, **kwargs):
        if self.ext in ["xls", "xlsx"]:
            return pd.read_excel(self._connection, **kwargs)
