
from sites import Site

from global_timer import timer


class DataManager:
    def __init__(self):
        self.sites = [None]  # filler for 0 index (NOTE: use self.sites[1:] for enumerator
        self.sites.extend([Site(i) for i in range(1, 11)])
        self.up_sites = self.sites[1:]
        self.locks = {}  # store all locks on all vars in all sites
        self.RO_sites = {}  # dictionary of sites to lookup for RO data
        for var in range(2, 21, 2):
            self.RO_sites[var] = {*range(1, 11)}
        for var in range(1, 21, 2):
            self.RO_sites[var] = {1 + var % 10}
        self.RO_sites = {str(k): v for k, v in sorted(self.RO_sites.items(), key=lambda x: x[0])}

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
        for var in self.RO_sites:
            if len(self.RO_sites[var]) > 0:
                result = self.read(self.RO_sites[var], var)
                if result:
                    data[var] = result[0][var]
                    continue
        if data:
            return data
        else:
            return False

    def set_lock(self):
        """" Update lock status on site s for var x """
        ...

    def read_lock_status(self):
        """ Return lock status  """
        ...

    def handle_failure(self, site):
        """ Simulate failure in site s """

        # Remove site from list of up_sites
        for site_id in self.up_sites:
            if int(site) == site_id:
                self.up_sites.remove(site_id)
        self.last_failure[site] = timer.time
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
