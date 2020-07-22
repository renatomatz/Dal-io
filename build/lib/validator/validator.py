"""Define Validator class

Validators are the building blocks of data integrity in the graph. As
modularity is key, validators ensure that the data that enters a node is what
it is mean to be or that errors are targeted to make debugging easier.
"""


class Validator:
    """Check for some characteristic of a piece of data

    Validators can have any attribute needed, but functionality is stored
    in u the .validate function, which returns any errors in the data.

    Attributes:
        fatal (bool): Whether if invalid data is fatal. Decides whether
            invalid data can still be passed on (with a warning) or if it is
            grounds to stop the execution of the graph. False by default.
        test_desc (str): Description of tests performed on data
    """

    fatal: bool
    is_on: bool
    test_desc: str

    def __init__(self, fatal=True):
        """Initialize validator and set default attributes"""
        self.fatal = fatal
        self.is_on = True
        self.test_desc = "Validates condition"

    def validate(self, data):
        """Validate data

        Check if data fits a certain description.

        Returns:
            A description of any errors in the data according to this
            specific validation condition, and None if data is valid.
        """
        raise NotImplementedError()

    def fatal_on(self):
        """Turn fatal on and return self"""
        self.fatal = True
        return self

    def fatal_off(self):
        """Turn fatal off and return self"""
        self.fatal = False
        return self
