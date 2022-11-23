
import time


class Transaction:
    def __init__(self, _id, RO_flag=False):
        self.id = _id
        self.data = {}
        self.locks = {}
        self.RO_flag = RO_flag
        self.start_time = time.time()
        self.sites_accessed = []  # Compare these with time of commit - if anything fails, abort

    def read(self, sites, var, dm_handler):
        """ Read from site s """
        if self.RO_flag:
            return {var: self.data[var]}
        result = dm_handler.read(sites, var)
        if result:
            self.data[var] = (result[var], sites)
        return result

    def ro_read(self, dm_handler):

        if self.RO_flag:
            result = dm_handler.get_ro_cache()
            if result:
                self.data = result
                return self.data
            else:
                print(f"Cannot initiate Transaction {self.id} in RO mode - no sites available")
        return False

    def write(self, sites, var, value):
        """ Write/ Save x in transaction T """
        assert not self.RO_flag, f"Transaction: {self.id} is in Read-Only mode. Failed to write: {var}: {value}"
        self.data[var] = (value, sites)  # {var: (value, sites)}
        return True

    def request_lock(self):
        """ Request for a new lock """
        ...

    def release_lock(self):
        """ Release locks on end """
        ...

    def commit(self, dm_handler):
        """ Validate and commit all updated variables into all up_sites """
        # TODO: Validate the commit
        if self.RO_flag:
            return True
        for var, (value, sites) in self.data.items():
            dm_handler.write(sites, var, value)

    def abort(self):
        """ Release locks and abort T """
        ...
