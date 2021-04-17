from beem import Steem
from beem.nodelist import NodeList
from beem.blockchain import Blockchain
import os
import json
import time
from steembi.transfer_ops_storage import AccountTrx
from steembi.storage import AccountsDB
import dataset

if __name__ == "__main__":
    config_file = 'config.json'
    if not os.path.isfile(config_file):
        raise Exception("config.json is missing!")
    else:
        with open(config_file) as json_data_file:
            config_data = json.load(json_data_file)
        databaseConnector = config_data["databaseConnector"]
        databaseConnector2 = config_data["databaseConnector2"]
        other_accounts = config_data["other_accounts"]
        hive_blockchain = config_data["hive_blockchain"]
    start_prep_time = time.time()
    db = dataset.connect(databaseConnector)
    db2 = dataset.connect(databaseConnector2)
    accountStorage = AccountsDB(db2)
    accounts = accountStorage.get()

    # Update current node list from @fullnodeupdate
    nodes = NodeList()
    nodes.update_nodes()
    stm = Steem(node=nodes.get_nodes(hive=hive_blockchain))

    print("Check account history ops.")

    blockchain = Blockchain(steem_instance=stm)

    accountTrx = {}
    for account in accounts:
        accountTrx[account] = AccountTrx(db, account)
        if not accountTrx[account].exists_table():
            accountTrx[account].create_table()
    # temp
    accountTrx["sbi"] = AccountTrx(db, "sbi")

    ops1 = accountTrx["steembasicincome"].get_all(op_types=["transfer", "delegate_vesting_shares"])

    ops2 = accountTrx["sbi"].get_all(op_types=["transfer", "delegate_vesting_shares"])
    print("ops loaded: length: %d - %d" % (len(ops1), len(ops2)))

    index = 0
    while index < len(ops1) and index < len(ops2):
        op1 = ops1[index]
        op2 = ops2[index]

        start_block = op1["block"]
        virtual_op = op1["virtual_op"]
        trx_in_block = op1["trx_in_block"]
        op_in_trx = op1["op_in_trx"]

        start_block = op2["block"]
        virtual_op = op2["virtual_op"]
        trx_in_block = op2["trx_in_block"]
        op_in_trx = op2["op_in_trx"]
        dict1 = json.loads(op1["op_dict"])
        dict2 = json.loads(op2["op_dict"])
        if dict1["timestamp"] != dict2["timestamp"]:
            print("%s - %s" % (dict1["timestamp"], dict2["timestamp"]))
            print("block: %d - %d" % (op1["block"], op2["block"]))
            print("index: %d - %d" % (op1["op_acc_index"], op2["op_acc_index"]))
        index += 1
