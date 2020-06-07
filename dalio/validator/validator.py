'''Define Validator class'''


class Validator:
    '''Validators are the building blocks of data integrity in the model. As
    modularity is key, validators ensure that the data that leaves a node is
    what it is mean to be or that errors are targeted to make debugging easier.
    Validators can have any attribute needed, but functionality is stored in u
    the .validate function, which either passes data on or stops execution with
    an error.

    === Attributes ===

    fatal: whether to throw an exception and stop the program if data is found
    not to be valid.
    False by default.
    Non-fatal errors will be reported as warnings on DataDef and will not stop
    the graph's execution. Fatal errors will stop the whole graph from being
    executed and be repored as an exception in DataDef.

    is_on: whether validator is turned on

    test_desc: description of tests performed on data

    === Methods ===

    validate: validate input
    - returns False if input is valid and a warn_report if it is not

    warn_report: return a string reporting a warning to be included in the
    DataDef warning report

    '''

    _fatal: bool
    is_on: bool
    test_desc: str

    def __init__(self):
        self._fatal = False
        self.is_on = True
        self.test_desc = "Validates condition"

    def validate(self, data):
        '''Validate data to see if it fits a description
        '''
        return data is not None

    def _error_report(self, exception, data, *args, **kwargs):
        return exception(self._warn_report(data, *args, **kwargs))

    def _warn_report(self, data):
        return f"Invalid data: {data}"

    def val_on(self):
        self.is_on = True

    def val_off(self):
        self.is_on = False

    def fatal_on(self):
        self._fatal = True

    def fatal_off(self):
        self._fatal = False

    def is_fatal(self):
        return self._fatal
