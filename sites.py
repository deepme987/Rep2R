

class Site:
    def __init__(self, id):
        self.id = id
        self.data = {}  # list of variables in the site
        self.status = 1  # 1 for up, 0 for down
        self.locks = {}

    def read_data(self):
        """ Returns data on var x in the site """
        ...

    def write_data(self):
        """ Commit data into the file/ storage """
        ...

    def failure(self):
        """ Simulate a site failure """
        # Make all replicated variables unavailable
        ...

    def recovery(self):
        """ Recover a site from failure """
        ...

    def dump(self):
        """ Returns the data in the site s """
        return self.data
