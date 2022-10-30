
import time


class Transaction:
    def __init__(self, id):
        self.id = id
        self.data = {}
        self.locks = {}
        self.RO_flag = False
        self.start_time = time.time()
        self.sites_accessed = []  # Compare these with time of commit - if anything fails, abort

    def read(self, var, sites):
        """ Read from site s """
        ...

    def write(self, var, sites):
        """ Write/ Save x in transaction T """
        ...

    def request_lock(self):
        """ Request for a new lock """
        ...

    def release_lock(self):
        """ Release locks on end """
        ...

    def commit(self):
        """ Validate and commit all updated variables into all up_sites """
        ...

    def abort(self):
        """ Release locks and abort T """
        ...
