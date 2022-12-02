#from __future__ import annotations

import os
import json

SITES_PATH = "data/"
if not os.path.exists(SITES_PATH):
    os.mkdir(SITES_PATH)


class Site:
    def __init__(self, _id):
        self.id = _id
        self.data = {}  # list of variables in the site
        self.locks = {}
        self.status = 1
        self.var_status = {}
        self.path = SITES_PATH + str(self.id) + ".json"

        self.flush()

        with open(self.path, 'r') as fil:
            self.data = json.load(fil)

    def read_data(self, var: str) -> int | bool:
        """ Returns data on var x in the site
        :param var: variable to read from
        :return: value of variable if found, else False
        """
        if var in self.data:
            return self.data[var]
        return False

    def write_data(self, var: str, value: int) -> bool:
        """ Commit data into the file/ storage
        :param var: variable to write data into
        :param value: value of updated data
        :return: success/ failure
        """
        self.data[var] = int(value)
        if self.var_status[var] == "down":
            self.var_status[var] = "up"
        with open(self.path, 'w') as fil:
            json.dump(self.data, fil)
        return True

    def failure(self) -> None:
        """ Simulate a site failure """
        # Make all replicated variables unavailable
        self.status = 0
        for var in self.var_status:
            self.var_status[var] = "down"
        for var in range(2, 21, 2):
            if (var % 2 == 0) and str(var) in self.data:
                self.data.pop(str(var))
        print(f"sites fail - {self.id} - {self.data}")

    def recovery(self) -> None:
        """ Recover a site from failure """
        for var in self.var_status:
            if int(1 + int(var) % 10) == self.id:
                self.var_status[var] = "up"

    def dump(self) -> dict:
        """ Returns the data in the site s """
        return self.data

    def flush(self) -> None:
        """ Reset a site and it's data storage """
        # Flush data
        data = {var: 10 * var for var in range(2, 21, 2)}
        if self.id % 2 == 0:
            odd_data = {(self.id + i * 10) - 1: ((self.id + i * 10) - 1) * 10 for i in range(2)}
            data.update(odd_data)
            data = {k: v for k, v in sorted(data.items(), key=lambda x: x[0])}
        with open(self.path, "w+") as fil:
            json.dump(data, fil)

        # Flush site status
        self.status = 1  # 1 for up, 0 for down
        self.var_status = {**{str(var): "up" for var in range(2, 21, 2)},
                           **{str(var): "up" for var in range(1, 21, 2) if (1 + var % 10 == self.id)}}

        # Flush locks
        self.locks = {str(i): 0 for i in range(2, 21, 2)}  # TBC whether to be removed
        # 0 for no lock, 1 for read lock, 2 for write lock  - Initialize with 0
        if self.id % 2 == 0:
            for i in range(1, 21, 2):
                if 1 + i % 10 == self.id:
                    self.locks[i] = 0

    def __repr__(self):
        return f"{self.id}"

    def __eq__(self, other):
        return self.id == other
