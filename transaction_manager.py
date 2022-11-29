
import re

from transaction import Transaction
from data_manager import DataManager

from global_timer import *


class TransactionManager:
    def __init__(self):
        self.transactions = {}  # T1, T2
        self.wait_queue = {}
        self.dm_handler = DataManager()

        self.dm_handler.flush_sites()

        self.function_mapper = {
            "begin": self.begin_transaction,
            "beginRO": self.begin_ro_transaction,
            "R": self.execute_read_transaction,
            "W": self.execute_write_transaction,
            "end": self.end_transaction,
            "dump": self.dump,
            "recover": self.recover,
            "fail": self.fail,
        }

    def reset(self) -> None:
        self.__init__()

    def input_parser(self, file_path: str = "mini_test.txt"):
        """ Read inputs one by one execute them """
        reset_timer()
        with open(file_path, 'r') as fil:
            for line in fil.readlines():
                if line != "\n" and "//" not in line:
                    tx = line.split('(')[0]
                    if tx not in self.function_mapper:
                        print("SKIPPING -", line)
                    else:
                        args = re.findall(r'\(.*\)', line)[0][1:-1].split(",")
                        args = [arg.strip() for arg in args]
                        increment_timer()
                        if args != ['']:
                            print(f"{tx} - {self.function_mapper[tx].__name__}({','.join(args)})")
                            self.function_mapper[tx](*args)
                        else:
                            print(f"{tx} - {self.function_mapper[tx].__name__}()")
                            self.function_mapper[tx]()

    def begin_transaction(self, tx):
        """ Create a Transaction node and add it to the list """
        transaction = Transaction(tx)
        self.transactions[tx] = transaction

    def begin_ro_transaction(self, tx):
        """ Create a Read-Only Transaction node and add it to the list """
        transaction = Transaction(tx, RO_flag=True)
        result = transaction.ro_read(dm_handler=self.dm_handler)
        if result:
            self.transactions[tx] = transaction
        return False

    def execute_read_transaction(self, tx, var):
        """ Execute transaction tx """
        var = var[1]
        sites = self.routing(var)
        if self.transactions[tx].request_lock(sites,var,1,self.dm_handler):

            result = self.transactions[tx].read(sites, var, self.dm_handler)
            if result:
                self.printer(f"Read Successful: {tx}: x{var} - {result[var]}; Sites: {sites}")
            else:
                self.printer(f"Error reading at : {tx}: x{var}; Sites: {sites}")
        else:
            self.printer(f"Failed getting read locks at : {tx}: x{var}; Sites: {sites}")

    def execute_write_transaction(self, tx, var, value):
        """ Execute transaction tx """
        var = var[1]
        sites = self.routing(var)
        if self.transactions[tx].request_lock(sites, var, 2,self.dm_handler):
            self.printer(f"Acquiring Write Lock Successful: {tx}: x{var} - {value}; Sites: {sites}")
            result = self.transactions[tx].write(sites, var, value)
            if result:
                self.printer(f"Write Successful: {tx}: x{var} - {value}; Sites: {sites}")
            else:
                self.printer(f"Error writing at : {tx}: x{var} - {value}; Sites: {sites}")
        else:
            self.printer(f"Failed getting write locks at : {tx}: x{var} - {value}; Sites: {sites}")


    def end_transaction(self, tx):
        """" Commit - if any, and delete tx from list """
        self.transactions[tx].commit(self.dm_handler)
        self.transactions[tx].release_lock(self.dm_handler)

    def routing(self, var):
        """ Find the site to work with for T """
        var = int(var)
        if var % 2 == 1:
            sites = [1 + var % 10]
        else:
            sites = [site.id for site in self.dm_handler.up_sites]
        return sites

    def deadlock_cycle(self):
        """ Somehow check for a deadlock - maybe DFS on graph """
        ...

    def fail(self, site):
        """ Simulate failure in site S """
        self.dm_handler.handle_failure(site)

    def recover(self, site):
        """ recover site S from failure """
        ...

    def printer(self, message):
        """ Print whatever you want """
        print(message)

    def dump(self):
        """ Get all variables from all sites and dump """
        dump_data = self.dm_handler.dump()
        for site, var_val in sorted(dump_data.items(), key=lambda x: x[0]):
            self.printer(f"Site {site}: {', '.join([f'{var}: {val}' for var, val in var_val.items()])}")


if __name__ == "__main__":
    tm = TransactionManager()
    tm.input_parser()
    print("Done")
