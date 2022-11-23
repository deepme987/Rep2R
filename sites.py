
import os
import json


SITES_PATH = "data/"
if not os.path.exists(SITES_PATH):
    os.mkdir(SITES_PATH)


class Site:
    def __init__(self, _id):
        self.id = _id
        self.data = {}  # list of variables in the site
        self.status = 1  # 1 for up, 0 for down
        self.locks = {}
        self.path = SITES_PATH + str(self.id) + ".json"

        self.flush()

        with open(self.path, 'r') as fil:
            self.data = json.load(fil)

    def read_data(self, var):
        """ Returns data on var x in the site """
        if var in self.data:
            return self.data[var]
        return False

    def write_data(self, var, value):
        """ Commit data into the file/ storage """
        self.data[var] = value
        with open(self.path, 'w') as fil:
            json.dump(self.data, fil)
        return True

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

    def flush(self):
        data = {var: 10 * var for var in range(2, 21, 2)}
        if self.id % 2 == 0:
            odd_data = {(self.id + i * 10) - 1: ((self.id + i * 10) - 1) * 10 for i in range(2)}
            data.update(odd_data)
            data = {k: v for k, v in sorted(data.items(), key=lambda x: x[0])}
        with open(self.path, "w+") as fil:
            json.dump(data, fil)

    def __repr__(self):
        return f"{self.id}"

    def __eq__(self, other):
        return self.id == other
