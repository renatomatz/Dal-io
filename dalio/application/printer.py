import sys

from dalio.external import FileWriter
from dalio.application import Application
from dalio.validator import HAS_ATTR


class Printer(Application):

    def __init__(self, out_file=sys.stdout):
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
        data = self._source_from("data_in", **kwargs)
        self._get_output("data_out").request(query=data, **kwargs)
