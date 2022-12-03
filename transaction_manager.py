# from __future__ import annotations

import re
from collections import defaultdict

from transaction import Transaction
from tm_helper import TMHelper
from global_timer import timer

DEBUG = False  # Verbose flag


class TransactionManager:
    def __init__(self):
        self.transactions = {}  # T1, T2
        self.wait_queue = []  # (Tx, function, args, wait_for_vars)
        self.dm_handler = TMHelper()

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
        """ Reset data values """
        self.__init__()

    def input_parser(self, file_path: str = "mini_test.txt") -> None:
        """ Read inputs one by one execute them
        :param file_path: path to test file
        """
        timer.reset_timer()
        with open(file_path, 'r') as fil:
            for line in fil.readlines():
                if line != "\n":
                    uncomment = line.strip().split("//")[0]
                    tx = uncomment.split('(')[0]
                    if len(tx) == 0 or "//" in uncomment:
                        continue
                    if tx not in self.function_mapper:
                        print("SKIPPING -", line)
                    else:
                        print(f"{timer.time}: ", end="")
                        self.deadlock_cycle()
                        if len(self.wait_queue) > 0:
                            temp_wait_queue = self.wait_queue
                            self.wait_queue = []
                            for transaction, function, args, _ in temp_wait_queue:
                                print(f"Attempting transaction from wait queue:")
                                if args != ['']:
                                    print(f"{transaction.id} - {function.__name__}({','.join(args)})")
                                    function(*args)
                                else:
                                    print(f"{transaction.id} - {function.__name__}()")
                                    function()
                            print("Wait queue traversed, continuing with new input")
                            print(f"{timer.time}: ", end="")

                        args = re.findall(r'\(.*\)', line)[0][1:-1].split(",")
                        args = [arg.strip() for arg in args]
                        timer.increment_timer()
                        if args != ['']:
                            if DEBUG:
                                print(f"{timer.time}: {tx} - {self.function_mapper[tx].__name__}({','.join(args)})")
                            try:
                                self.function_mapper[tx](*args)
                            except TypeError:
                                print("INVALID ARGUMENT in input, skipping line")
                        else:
                            if DEBUG:
                                print(f"{timer.time}: {tx} - {self.function_mapper[tx].__name__}()")
                            self.function_mapper[tx]()

    def begin_transaction(self, tx: str) -> None:
        """ Create a Transaction node and add it to the list
        :param tx: transaction_id
        """
        transaction = Transaction(tx)
        self.transactions[tx] = transaction

    def begin_ro_transaction(self, tx: str) -> bool:
        """ Create a Read-Only Transaction node and add it to the list
        :param tx: transaction_id
        :return: bool to indicate success/ failure
        """
        transaction = Transaction(tx, RO_flag=True)
        result = transaction.ro_read(dm_handler=self.dm_handler)
        if result:
            self.transactions[tx] = transaction
            return True
        self.wait_queue.append((transaction, self.begin_ro_transaction, [tx], transaction.wait_for_vars))
        return False

    def execute_read_transaction(self, tx: str, var: str) -> bool:
        """ Execute read transaction tx
        :param tx: transaction_id
        :param var: variable in context
        :return: flag to indicate success/ failure
        """
        if tx not in self.transactions:
            print(f"Transaction {tx} not found")
            return False
        _var = var
        var = var[1]
        sites = self.routing(var)
        if self.transactions[tx].RO_flag:
            result = self.transactions[tx].read(sites, var, self.dm_handler)
            if result:
                print(f"Read Successful: {tx}: x{var} - {result[var]}; Sites: {sites}")
            else:
                print(f"Error reading at : {tx}: x{var}; Sites: {sites}")
        elif self.transactions[tx].request_lock(sites, var, 1, self.dm_handler):
            result = self.transactions[tx].read(sites, var, self.dm_handler)
            if result:
                print(f"Read Successful: {tx}: x{var} - {result[var]}; Sites: {sites}")
            else:
                self.wait_queue.append((self.transactions[tx], self.execute_read_transaction,
                                        [tx, _var], {_var: 1}))
                print(f"Error reading at : {tx}: x{var}; Sites: {sites}")
        else:
            self.wait_queue.append((self.transactions[tx], self.execute_read_transaction,
                                    [tx, _var], {_var: 1}))
            print(f"Failed getting read locks at : {tx}: x{var}; Sites: {sites}")

    def execute_write_transaction(self, tx: str, var: str, value: int) -> bool:
        """ Execute write transaction tx
        :param tx: transaction_id
        :param var: variable in context
        :param value: value to update for var
        :return: flag to indicate success/ failure
        """
        if tx not in self.transactions:
            print(f"Transaction {tx} not found")
            return False
        _var = var
        var = var[1]
        sites = self.routing(var)
        if self.transactions[tx].request_lock(sites, var, 2, self.dm_handler):
            print(f"Acquiring Write Lock Successful: {tx}: x{var} - {value}; Sites: {sites}")
            result = self.transactions[tx].write(sites, var, value)
            if result:
                print(f"Write Successful: {tx}: x{var} - {value}; Sites: {sites}")
            else:
                print(f"Error writing at: {tx}: x{var} - {value}; Sites: {sites}")
        else:
            self.wait_queue.append((self.transactions[tx], self.execute_write_transaction,
                                    [tx, _var, value], {_var: 2}))
            print(f"Failed getting write locks at : {tx}: x{var} - {value}; Sites: {sites}")

    def end_transaction(self, tx: str) -> bool:
        """" Validate and commit - if any, and delete tx from list
        :param tx: transaction_id
        :return: flag to indicate success/ failure
        """
        if tx not in self.transactions:
            print(f"Transaction {tx} not found")
            return False
        flag = self.transactions[tx].commit(self.dm_handler)
        self.transactions[tx].release_lock(self.dm_handler)
        if not flag:
            self.abort_transaction(tx)
        del self.transactions[tx]
        return True

    def abort_transaction(self, tx: str) -> None:
        """ Abort transaction tx
        :param tx: transaction_id
        """
        self.transactions[tx].abort(self.dm_handler)
        # del self.transactions[tx]

    def routing(self, var: str) -> list:
        """ Find the site to route execution of T
        :param var: variable in context
        :return: [sites_avialable]
        """
        var = int(var)
        if var % 2 == 1:
            site_id = 1 + var % 10
            sites = [site_id for site in self.dm_handler.up_sites if site_id == site.id]
        else:
            sites = [site.id for site in self.dm_handler.up_sites]
        return sites

    def deadlock_cycle(self) -> None:
        """ Create a tree from wait_queue and run DFS to find a cycle - Aborts youngest tx """
        conflicts = defaultdict(set)

        for i in range(len(self.wait_queue)):
            i_locks = self.wait_queue[i][3]
            for tx in self.transactions:
                if self.wait_queue[i][0].id == self.transactions[tx].id:
                    continue
                j_locks = {'x' + str(var): max([status for _, (status, _) in sites.items()])
                           for var, sites in self.transactions[tx].locks.items()
                           if len(sites) > 0
                           }

                for var in {*i_locks}.intersection({*j_locks}):
                    if i_locks[var] == 2 or j_locks[var] == 2:
                        conflicts[self.wait_queue[i][0].id].add(self.transactions[tx].id)
        path = self.dfs_handler(conflicts)

        if path:
            min_index, min_time = None, -1
            for tx in path:
                if min_time < self.wait_queue[tx][0].start_time:
                    min_time = self.wait_queue[tx][0].start_time
                    min_index = tx
            if min_index is not None:
                print(f"DEADLOCK FOUND: {[self.wait_queue[i][0].id for i in path]}; "
                      f"Aborting transaction: {self.wait_queue[min_index][0].id}")
                self.abort_transaction(self.wait_queue[min_index][0].id)
                del self.wait_queue[min_index]

    def dfs_handler(self, adj: dict) -> list | bool:
        """ DFS for deadlock_cycle
        :param adj: {parent: [children]}
        :return: path | cycle_flag
        """
        visited = defaultdict(int)

        def dfs(x):
            if x not in adj:
                return False
            if visited[x] == 1:
                return True
            if visited[x] == 2:
                return False
            path.append(x)
            visited[x] = 1
            res = False
            for y in adj[x]:
                if dfs(y):
                    res = True
            visited[x] = 2
            if not res:
                path.remove(x)
            return res

        for x in adj:
            path = []
            if dfs(x):
                return path
        return False

    def fail(self, site: str) -> None:
        """ Simulate failure in site S
        :param site: site to simulate failure
        """
        self.dm_handler.handle_failure(int(site))
        for tx in self.transactions:
            self.transactions[tx].erase_lock(int(site))

    def recover(self, site: str) -> None:
        """ Recover site S from failure
        :param site: site to simulate recovery
        """
        self.dm_handler.handle_recovery(int(site))

    def dump(self) -> None:
        """ Get all variables from all sites and dump """
        print("Dump data")
        dump_data = self.dm_handler.dump()
        for site, var_val in sorted(dump_data.items(), key=lambda x: x[0]):
            print(f"Site {site}: {', '.join([f'{var}: {val}' for var, val in var_val.items()])}")


if __name__ == "__main__":
    tm = TransactionManager()
    tm.input_parser()
    print("Done")
