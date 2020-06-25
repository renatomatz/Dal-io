"""Print data onto an external output"""

from dalio.application import Application
from dalio.validator import HAS_ATTR


class FilePrinter(Application):
    """Application to print data onto a file

    This application has one source: data_in. The data_in source is the data
    to be printed.

    This application has one output: data_out. The data_out output is the
    external output to print the data to.
    """

    def __init__(self):
        """Initialize instance.

        Defines data_in source as having a "__str__" attribute.
        """
        super().__init__()

        self._init_source([
            "data_in"
        ])

        self._init_output([
            "data_out"
        ])

        self._get_source("data_in")\
            .add_desc(HAS_ATTR("__str__"))

    def run(self, **kwargs):
        """Gets data and prints it"""
        data = self._source_from("data_in", **kwargs)
        self._get_output("data_out").request(query=data, **kwargs)
