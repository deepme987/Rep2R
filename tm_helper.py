from __future__ import annotations

from data_manager import DataManager
from global_timer import timer

from typing import Union, Tuple, Dict, Any, List


class TMHelper:
    def __init__(self):
        self.sites = [None]  # filler for 0 index (NOTE: use self.sites[1:] for enumerator
        self.sites.extend([DataManager(i) for i in range(1, 11)])
        self.up_sites = self.sites[1:]
        self.locks = {}  # store all locks on all vars in all sites
        self.site_status = {}
        self.RO_sites = {}  # dictionary of sites to lookup for RO data

        for var in range(2, 21, 2):
            self.RO_sites[var] = {*range(1, 11)}
        for var in range(1, 21, 2):
            self.RO_sites[var] = {1 + var % 10}

        self.RO_sites = {str(k): v for k, v in sorted(self.RO_sites.items(), key=lambda x: x[0])}
        self.last_failure = {site.id: -1 for site in self.up_sites}

    def read(self, sites: list, var: str, ro_flag: bool = False) -> tuple[dict[Any, Any], list[Any]] | tuple[bool, Any]:
        """ Validate tx, locks and read if allowed
        :param sites: list of sites to read from
        :param var: variable in context
        :param ro_flag: is tx in read-only mode? default: False
        :return: {var: value}, [site_used_to_read]
        """
        for site in sites:
            if site not in self.up_sites:
                print(f"Skipping Read; Site {site} is down")
                continue
            if not ro_flag and var in self.locks[site].keys() \
                    and self.locks[site][var][0] != 1:
                continue
            data = self.sites[site].read_data(var)
            if data:
                return {var: data}, [site]
        return False, None

    def validate_and_commit(self, data: dict, read_data: dict = None) -> bool:
        """ Validate transaction for commit
            Checks with failure sites and available locks
        :param data: data to be committed: {var: (value, {site: first_accessed})}
        :param validate_data: additional data for validation of read vals
        :return:
        """

        validate_data = data
        validate_data.update(read_data)

        for var, (value, sites) in validate_data.items():
            for site, time_stamp in sites.items():
                if site not in self.up_sites or self.last_failure[site] > time_stamp \
                        or (site in self.locks and var in self.locks[site].keys()
                            and ((var not in read_data and self.locks[site][var][0] != 2)
                                 or (var in read_data and self.locks[site][var][0] == 0))):
                    return False

        for var, (value, sites) in data.items():
            self.write(sites, var, value)

        return True

    def write(self, sites: list, var: str, value: int) -> None:
        """ Validate tx, locks and write if allowed.
            We write only after commit and validation of the commit
        :param sites: list of sites to write into
        :param var: variable in context
        :param value: updated integer value of var
        """
        # Update RO_sites accordingly
        for site in sites:
            if site in self.up_sites:
                self.sites[site].write_data(var, value)
                self.RO_sites[var].add(site)
                self.site_status[site][var]="up"

    @property
    def get_ro_cache(self) -> tuple[bool, dict | list]:
        """ Reads the clean read-only data at time stamp T
            Unclean data is not read
        :return: bool (success/ failure), data ({var: value}/ [variables_on_lock]}
        """
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

    def set_lock(self, sites: list, var: str, lock_type: int, tx_id: str) -> dict:
        """" Update lock status on site s for var x
        :param sites: list of sites to request lock from
        :param var: variable in context
        :param lock_type: integer value for lock
                        0: No Lock
                        1: Read Lock
                        2: Write Lock
            Note: We do not use a lock for RO-only transactions.
                However, it is validated on beginRO as read lock
        :param tx_id: transaction id
        :return: acquired locks {site: {var: (lock_status, transaction_id)}}
        """
        acquired_lock = False
        for s in sites:
            if s in self.up_sites:
                if lock_type == 1:
                    if self.site_status[s][var]=="up":
                        if self.locks[s][var][1]:
                            self.locks[s][var][1].append(tx_id)
                            self.locks[s][var] = (lock_type, self.locks[s][var][1])
                        else:
                            self.locks[s][var] = (lock_type, [tx_id])
                        acquired_lock = True
                elif lock_type == 2:
                    self.locks[s][var] = (lock_type, [tx_id])
                    acquired_lock = True
                elif lock_type == 0:
                    if self.locks[s][var][1]:
                        self.locks[s][var][1].remove(tx_id)
                        if len(self.locks[s][var][1]) == 0:
                            self.locks[s][var] = (lock_type, self.locks[s][var][1])
                    else:
                        self.locks[s][var] = (lock_type, [])
                    acquired_lock = True
        return self.locks,acquired_lock

    def read_lock_status(self, var: str) -> tuple[int, str]:
        """ Return lock status
        :param var: variable to check lock on
        :return: lock_status, transaction_id
        """
        max_lock = -1
        txn = []
        for x in self.locks:
            if x in self.up_sites and var in self.locks[x].keys():
                if self.locks[x][var][0] > max_lock:
                    max_lock = self.locks[x][var][0]
                    txn = self.locks[x][var][1]

        return max_lock, txn

    def handle_failure(self, site: int) -> None:
        """ Simulate failure in site s
            :param site: site to simulate failure
            """
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
            self.locks[site][var] = (-1, [])

        for var in self.site_status[site]:
            self.site_status[site][var] = "down"

        # Implement site related failure
        self.sites[site].failure()

    def handle_recovery(self, site: int) -> None:
        """ Simulate recovery in site s;
            :param site: site to simulate recovery
        """
        site = int(site)
        self.up_sites.append(self.sites[site])
        # added site to RO_sites
        for var in self.RO_sites:
            if int(var) % 2 == 0:
                self.RO_sites[var].add(site)
            elif int(var) % 10 + 1 == site:
                self.RO_sites[var].add(site)
        # Make all the locks for the site lockable
        for var in self.locks[site]:
            self.locks[site][var] = (0, [])
        # update variable status in recovered site
        for var in self.site_status[site]:
            if int(var) % 2 == 1:
                self.site_status[site][var] = "up"
        self.sites[site].recovery()

    def dump(self) -> dict:
        """ Get all variables from all sites and dump
        :return: return all data from all sites {site: {var: value}}
        """
        dump_data = {}
        for site in self.sites[1:]:
            dump_data[site.id] = site.dump()
        return dump_data

    def flush_sites(self) -> True:
        """ Reset sites data """
        for site in self.sites[1:]:
            site.flush()
        # Flush the locks for each site
        # lock initialize to 0, value  0 : when no lock present, 1: when read lock 2: when write lock
        even_replicated_var = {str(v): (0, []) for v in range(2, 21, 2)}
        for site in range(1, 11):
            odd_unreplicated_var = {str(v): (0, []) for v in range(1, 21, 2) if site == 1 + v % 10}
            self.locks[site] = {**even_replicated_var, **odd_unreplicated_var}

        #Flush site status for all sites
        even_rep_var_site = {str(v): "up" for v in range(2, 21, 2)}
        for site in range(1, 11):
            odd_rep_var_site = {str(v): "up" for v in range(1, 21, 2) if site == 1 + v % 10}
            self.site_status[site]= {**even_rep_var_site, **odd_rep_var_site}
        return True
