
from global_timer import timer


class Transaction:
    def __init__(self, _id, RO_flag=False):
        self.id = _id

        self.data = {}  # {var: value, {s1: time1, s2: time2}}
        self.locks = {}
        self.wait_for_vars = {}  # List of variables {var: lock_requested} tx is waiting for
        self.RO_flag = RO_flag
        self.start_time = timer.time
        self.sites_accessed = []  # Compare these with time of commit - if anything fails, abort

    def read(self, sites, var, dm_handler):
        """ Read from site s """
        if self.RO_flag:
            return {var: self.data[var]}
        result, updated_site = dm_handler.read(sites, var)
        if result:
            if var in self.data:
                if updated_site not in self.data[var][1]:
                    self.data[var][1][updated_site] = timer.time
                self.data[var][0] = result[var]
            else:
                self.data[var] = [result[var], {updated_site: timer.time}]
        return result

    def ro_read(self, dm_handler):
        """ Init Read-Only Transaction - fetch all current values """
        if self.RO_flag:
            flag, result = dm_handler.get_ro_cache()
            if flag:
                self.data = result
                return True
            else:
                self.wait_for_vars = {k: 1 for k in result}
                print(f"Cannot initiate Transaction {self.id} in RO mode - no sites available")
        return False

    def write(self, sites, var, value):
        """ Write/ Save x in transaction T - THIS DOES NOT COMMIT """
        if self.RO_flag:  # Cannot commit on RO locks
            print(f"Transaction: {self.id} is in Read-Only mode. Failed to write: {var}: {value}")
        if var in self.data:
            for site in sites:
                if site not in self.data[var][1]:
                    self.data[var][1][site] = timer.time
            self.data[var][0] = value
        else:
            self.data[var] = [value, {site: timer.time for site in sites}]  # {var: (value, sites, TIMER)}
        return True

    def request_lock(self, sites, var, lock_type, dm_handler):
        """ Request for a new lock """
        if lock_type == 1:
            valid_status = [0, 1]
        else:
            valid_status = [0]
        if dm_handler.read_lock_status(var) in valid_status:
            locks = dm_handler.set_lock(sites, var, lock_type, self.id)
            self.locks[var] = {s: locks[s][var] for s in sites}
            print(f"in transaction self.locks {self.locks}")
        else:
            return False
        return True

    def release_lock(self, dm_handler):
        """ Release locks on end """
        for var in self.locks.keys():
            sites = [x for x in dict(self.locks[var]).keys() if x in dm_handler.up_sites]
            locks = dm_handler.set_lock(sites, var, 0, None)
            print(f"Released locks for Transaction {self.id} and variables {var} at sites {sites} ")
        self.locks = {}

    def commit(self, dm_handler):
        """ Validate and commit all updated variables into all up_sites """
        if self.RO_flag:
            return True

        result = dm_handler.validate_and_commit(self.data)
        if not result:
            print(f"Aborting Transaction {self.id}")
        return result

    def abort(self, dm_handler):
        """ Release locks and abort T """
        self.data = {}
        self.release_lock(dm_handler=dm_handler)
