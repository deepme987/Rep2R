
from data_manager import DataManager
from transaction import Transaction


class TransactionManager:
    def __init__(self):
        self.transactions = [...]  # T1, T2
        self.wait_queue = [...]
        self.dm_handler = DataManager()

    def input_parser(self):
        """ Read inputs one by one execute them """
        ...

    def begin_transaction(self, tx):
        """ Create a Transaction node and add it to the list """
        transaction = Transaction(tx)
        self.transactions.append(transaction)

    def execute_transaction(self, tx):
        """ Execute transaction tx """
        ...

    def end_transaction(self, tx):
        """" Commit - if any, and delete tx from list """
        ...

    def routing(self, var):
        """ Find the site to work with for T """
        if var.id % 2 == 1:
            sites = [(1 + var.id) % 10]
        else:
            sites = self.dm_handler.up_sites
        return sites

    def deadlock_cycle(self):
        """ Somehow check for a deadlock - maybe DFS on graph """
        ...

    def printer(self, message):
        """ Print whatever you want """
        print(message)

    def dump(self):
        """ Get all variables from all sites and dump """
        ...
