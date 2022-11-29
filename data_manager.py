
from sites import Site

from global_timer import TIMER


class DataManager:
    def __init__(self):
        self.sites = [None]  # filler for 0 index (NOTE: use self.sites[1:] for enumerator
        self.sites.extend([Site(i) for i in range(1, 11)])
        self.up_sites = self.sites[1:]
        self.locks = {}  # store all locks on all vars in all sites
        self.RO_sites = {}  # dictionary of sites to lookup for RO data
        for var in range(2, 21, 2):
            self.RO_sites[var] = {*range(1, 11)}
            self.locks[var]={*range(1, 11)}
        for var in range(1, 21, 2):
            self.RO_sites[var] = {1 + var % 10}
            self.locks[var]={1 + var % 10}

        self.RO_sites = {str(k): v for k, v in sorted(self.RO_sites.items(), key=lambda x: x[0])}
        self.locks =  {str(k):v for k, v in sorted(self.locks.items())}
        # lock initialize to 0, value  0 : when no lock present, 1: when read lock 2: when write lock
        for i in range(1,21):
            self.locks[str(i)] = {v:0 for v in self.locks.get(str(i))}
        self.last_failure = {site.id: -1 for site in self.up_sites}


    def read(self, sites, var):
        """ Validate tx, locks and read if allowed """
        for site in sites:
            if site not in self.up_sites:
                print(f"Skipping Read; Site {site} is down")
                continue
            data = self.sites[site].read_data(var)
            if data:
                return {var: data}, [site]
        return False

    def validate_and_commit(self, data):
        for var, (value, sites) in data.items():
            for site, time_stamp in sites.items():
                if site not in self.up_sites or self.last_failure[site] > time_stamp:
                    return False

        for var, (value, sites) in data.items():
            self.write(sites, var, value)

    def write(self, sites, var, value):
        """ Validate tx, locks and write if allowed """
        # Update RO_sites accordingly
        for site in sites:
            if site in self.up_sites:
                self.sites[site].write_data(var, value)
                self.RO_sites[var].add(site)

    def get_ro_cache(self):
        data = {}
        for var in self.RO_sites:
            if len(self.RO_sites[var]) > 0:
                result = self.read(self.RO_sites[var], var)
                if result:
                    data[var] = result[var]
                    continue
        if data:
            return data
        else:
            return False

    def set_lock(self,sites,var,lock_type):
        """" Update lock status on site s for var x """
        for s in sites:
            if s in self.up_sites:
                self.locks[var][s]=lock_type


    def read_lock_status(self,var):
        """ Return lock status  """
        max_lock = 0
        for x in self.locks[var]:
            if x in self.up_sites:
                if self.locks[var][x] >max_lock:
                    max_lock = self.locks[var][x]


        return max_lock

    def handle_failure(self, site):
        """ Simulate failure in site s """

        # Remove site from list of up_sites
        for site_id in self.up_sites:
            if int(site) == site_id:
                self.up_sites.remove(site_id)
        self.last_failure[site] = TIMER
        # Remove site from the RO list of sites
        for var in self.RO_sites:
            if site in self.RO_sites[var]:
                self.RO_sites[var].remove(site)

        # TODO: remaining failure
        ...

    def handle_recovery(self, site):
        """
            Simulate recovery in site s;
        """
        self.up_sites.append(self.sites[site])

    def dump(self):
        """ Get all variables from all sites and dump """
        dump_data = {}
        for site in self.sites[1:]:
            dump_data[site.id] = site.dump()
        return dump_data

    def flush_sites(self):
        for site in self.sites[1:]:
            site.flush()
        return True
