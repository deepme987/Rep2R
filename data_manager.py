from sites import Site

from global_timer import timer


class DataManager:
    def __init__(self):
        self.sites = [None]  # filler for 0 index (NOTE: use self.sites[1:] for enumerator
        self.sites.extend([Site(i) for i in range(1, 11)])
        self.up_sites = self.sites[1:]
        self.locks = {}  # store all locks on all vars in all sites
        # self.site_status = {site:("up",-1) for site in self.up_sites}
        self.RO_sites = {}  # dictionary of sites to lookup for RO data


        for var in range(2, 21, 2):
            self.RO_sites[var] = {*range(1, 11)}
        for var in range(1, 21, 2):
            self.RO_sites[var] = {1 + var % 10}


        self.RO_sites = {str(k): v for k, v in sorted(self.RO_sites.items(), key=lambda x: x[0])}
        self.last_failure = {site.id: -1 for site in self.up_sites}

    def read(self, sites, var, ro_flag=False):
        """ Validate tx, locks and read if allowed """
        for site in sites:
            if site not in self.up_sites:
                print(f"Skipping Read; Site {site} is down")
                continue
            if not ro_flag and var in self.locks[site].keys() and self.locks[site][var][0] != 1:  # should txn id be verified?
                continue
            data = self.sites[site].read_data(var)
            if data:
                return {var: data}, [site]
        return False

    def validate_and_commit(self, data):
        for var, (value, sites) in data.items():
            for site, time_stamp in sites.items():
                if site not in self.up_sites or self.last_failure[site] > time_stamp or \
                        (site in self.locks and var in self.locks[site].keys() and self.locks[site][var][0] != 2):
                    return False

        for var, (value, sites) in data.items():
            self.write(sites, var, value)

        return True

    def write(self, sites, var, value):
        """ Validate tx, locks and write if allowed """
        # Update RO_sites accordingly
        for site in sites:
            if site in self.up_sites:
                self.sites[site].write_data(var, value)
                self.RO_sites[var].add(site)

    def get_ro_cache(self):
        data = {}
        locked_vars = []
        for var in self.RO_sites:
            if len(self.RO_sites[var]) > 0:
                result = self.read(self.RO_sites[var], var, ro_flag=True)
                if result:
                    data[var] = result[0][var]
                    continue
                locked_vars.append((var, 0))
        if data:
            return True, data
        else:
            return False, locked_vars

    def set_lock(self, sites, var, lock_type, tx_id):
        """" Update lock status on site s for var x """
        for s in sites:
            if s in self.up_sites:
                self.locks[s][var] = (lock_type, tx_id)
        # print(f"self.locks {self.locks}")
        return self.locks

    def read_lock_status(self, var):
        """ Return lock status  """
        max_lock = 0
        txn=""
        for x in self.locks:
            if x in self.up_sites and var in self.locks[x].keys():
                if self.locks[x][var][0] > max_lock:
                    max_lock = self.locks[x][var][0]
                    txn = self.locks[x][var][1]

        return (max_lock,txn)

    def handle_failure(self, site):
        """ Simulate failure in site s """

        # Remove site from list of up_sites
        for site_id in self.up_sites:
            if int(site) == site_id:
                self.up_sites.remove(site_id)
        self.last_failure[site] = timer.time
        # Update status in site_status
        # Remove site from the RO list of sites
        for var in self.RO_sites:
            if site in self.RO_sites[var]:
                self.RO_sites[var].remove(site)
        # Make all the locks for the site unlockable
        for var in self.locks[site]:
            self.locks[site][var] = (-1, None)
        # Implement site related failure
        self.sites[site].failure()

    def handle_recovery(self, site):
        """
            Simulate recovery in site s;
        """
        site = int(site)
        self.up_sites.append(self.sites[site])
        # added site to RO_sites
        for var in self.RO_sites:
            self.RO_sites[var].add(site)
        # Make all the locks for the site lockable
        for var in self.locks[site]:
            self.locks[site][var] = (0, None)
        self.sites[site].recovery()

    def dump(self):
        """ Get all variables from all sites and dump """
        dump_data = {}
        for site in self.sites[1:]:
            dump_data[site.id] = site.dump()
        return dump_data

    def flush_sites(self):
        for site in self.sites[1:]:
            site.flush()
        #Flush the locks for each site
        # lock initialize to 0, value  0 : when no lock present, 1: when read lock 2: when write lock
        even_replicated_var = {str(v): (0, None) for v in range(2, 21, 2)}
        for site in range(1, 11):
            odd_unreplicated_var = {str(v): (0, None) for v in range(1, 21, 2) if site == 1 + v % 10}
            self.locks[site] = {**even_replicated_var, **odd_unreplicated_var}
        print(f"locks2 - {self.locks}")
        return True
