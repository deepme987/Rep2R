
import time

from global_timer import TIMER

# TODO: available copies


class Transaction:
    def __init__(self, _id, RO_flag=False):
        self.id = _id
        self.data = {}  # {var: value, {s1: time1, s2: time2}}
        self.locks = {}
        self.RO_flag = RO_flag
        self.start_time = TIMER
        self.sites_accessed = []  # Compare these with time of commit - if anything fails, abort

    def read(self, sites, var, dm_handler):
        """ Read from site s """
        if self.RO_flag:
            return {var: self.data[var]}
        result, updated_site = dm_handler.read(sites, var)
        if result:
            if var in self.data:
                if updated_site not in self.data[var][1]:
                    self.data[var][1][updated_site] = TIMER
                self.data[var][0] = result[var]
            else:
                self.data[var] = [result[var], {updated_site: TIMER}]
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
        if self.RO_flag:
            print(f"Transaction: {self.id} is in Read-Only mode. Failed to write: {var}: {value}")
        if var in self.data:
            for site in sites:
                if site not in self.data[var][1]:
                    self.data[var][1][site] = TIMER
            self.data[var][0] = value
        else:
            self.data[var] = [value, {site: TIMER for site in sites}]  # {var: (value, sites, TIMER)}
        return True

    def request_lock(self):
        """ Request for a new lock """
        ...

    def release_lock(self):
        """ Release locks on end """
        ...

    def commit(self, dm_handler):
        """ Validate and commit all updated variables into all up_sites """
        if self.RO_flag:
            return True

        result = dm_handler.validate_and_commit(self.data)
        if not result:
            print(f"Aborting Transaction {self.id}")
        return result

    def abort(self):
        """ Release locks and abort T """
        ...
