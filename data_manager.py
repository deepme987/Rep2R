
from sites import Site


class DataManager:
    def __init__(self):
        self.sites = [Site(i+1) for i in range(10)]
        self.up_sites = []
        self.locks = {}  # store all locks on all vars in all sites
        self.RO_cache = {}  # dictionary of RO data

    def read(self):
        """ Validate tx, locks and read if allowed """
        ...

    def write(self):
        """ Validate tx, locks and write if allowed """
        # Update RO_cache accordingly
        ...

    def set_lock(self):
        """" Update lock status on site s for var x """
        ...

    def read_lock_status(self):
        """ Return lock status  """
        ...

    def handle_failure(self):
        """ Simulate failure in site s """
        ...

    def handle_recovery(self):
        """
            Simulate recovery in site s;
            Update committed values for replicated data
        """
        ...

    def dump(self):
        """ Get all variables from all sites and dump """
        ...
