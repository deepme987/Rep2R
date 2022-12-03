from global_timer import timer


class Transaction:
    def __init__(self, _id, RO_flag=False):
        self.id = _id

        self.data = {}  # {var: value, {s1: time1, s2: time2}}
        self.write_data = {}
        self.locks = {}
        self.wait_for_vars = {}  # List of variables {var: lock_requested} tx is waiting for
        self.RO_flag = RO_flag
        self.start_time = timer.time
        self.sites_accessed = []  # Compare these with time of commit - if anything fails, abort

        print(f"Began transaction {self.id}")

    def read(self, sites: list, var: str, dm_handler) -> tuple[dict, list]:
        """ Read from site s
        :param sites: [sites_to_read_from]
        :param var: variable in context
        :param dm_handler: DataManager handler
        :return: read data {var: value}
        """
        if self.RO_flag:
            return {var: self.data[var]}, []
        result, updated_site = dm_handler.read(sites, var)
        if result:
            if var in self.data:
                if updated_site not in self.data[var][1]:
                    self.data[var][1][updated_site] = timer.time
                self.data[var][0] = result[var]
            else:
                self.data[var] = [result[var], {updated_site[0]: timer.time}]
        return result, updated_site

    def ro_read(self, dm_handler) -> bool:
        """ Init Read-Only Transaction - fetch all current values
        :param dm_handler: DM Handler
        :return: bool
        """
        if self.RO_flag:
            flag, result = dm_handler.get_ro_cache
            if flag:
                self.data = result
                return True
            else:
                self.wait_for_vars = {k: 1 for k in result}
                print(f"Cannot initiate Transaction {self.id} in RO mode - no sites available")
        return False

    def write(self, sites: list, var: str, value: int) -> bool:
        """ Write/ Save x in transaction T - THIS DOES NOT COMMIT
        :param sites: [sites_to_read_from]
        :param var: variable in context
        :param value: integer value to update var
        :return: bool
        """
        if self.RO_flag:  # Cannot commit on RO locks
            print(f"Transaction: {self.id} is in Read-Only mode. Failed to write: {var}: {value}")
            return False
        if var in self.write_data:
            for site in sites:
                if site not in self.write_data[var][1]:
                    self.data[var][1][site] = timer.time
                    self.write_data[var][1][site] = timer.time
            self.data[var][0] = value
            self.write_data[var][0] = value
        else:
            self.data[var] = [value, {site: timer.time for site in sites}]  # {var: (value, sites, TIMER)}
            self.write_data[var] = self.data[var]
        return True

    def request_lock(self, sites: list, var: str, lock_type: int, dm_handler) -> bool:
        """ Request for a new lock
        :param sites: [sites_to_read_from]
        :param var: variable in context
        :param lock_type: integer value of lock
                        0: No lock
                        1: Read Lock
                        2: Write Lock
        :param dm_handler: DM Handler
        :return: bool
        """
        if lock_type == 1:
            valid_status = [0, 1]
        else:
            valid_status = [0]

        lock_status = dm_handler.read_lock_status(var)
        if lock_status[0] in valid_status or (
                lock_status[1] and self.id in lock_status[1] and len(lock_status[1]) == 1):
            locks = dm_handler.set_lock(sites, var, lock_type, self.id)
            self.locks[var] = {s: locks[s][var] for s in sites}
        else:
            return False
        return True

    def release_lock(self, dm_handler) -> None:
        """ Release locks on end
        :param dm_handler: DM Handler
        """
        for var in self.locks.keys():
            sites = [x for x in dict(self.locks[var]).keys() if x in dm_handler.up_sites]
            locks = dm_handler.set_lock(sites, var, 0, self.id)
            print(f"{timer.time}: Released locks for Transaction {self.id} and variables {var} at sites {sites} ")
        self.locks = {}

    def erase_lock(self, site):
        """Erase locks on a site failure"""
        for var in self.data.keys():
            if site in self.locks[var].keys():
                # self.locks[var][site] = (0, [])
                self.locks[var].pop(site)

    def commit(self, dm_handler) -> bool:
        """ Validate and commit all updated variables into all up_sites
        :param dm_handler: DM Handler
        :return: success/ failure
        """
        if self.RO_flag:
            print(f"Ending RO Transaction {self.id}")
            return True

        read_data = {k: v for k, v in self.data.items() if k not in self.write_data}
        result = dm_handler.validate_and_commit(self.write_data, read_data)
        if result:
            print(f"Committed Transaction {self.id}")
        else:
            print(f"Aborting Transaction {self.id}")
        return result

    def abort(self, dm_handler) -> None:
        """ Release locks and abort T
        :param dm_handler: DM Handler
        """
        self.data = {}
        self.release_lock(dm_handler=dm_handler)
