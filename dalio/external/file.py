import sys
import io
import os

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
            raise ValueError("invalid connection type {type(new_connection)}")

        if old is not None:
            old.close()
