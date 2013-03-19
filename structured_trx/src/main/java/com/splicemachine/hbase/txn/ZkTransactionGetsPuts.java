package com.splicemachine.hbase.txn;

import com.splicemachine.constants.ITransactionGetsPuts;
import com.splicemachine.constants.TxnConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Implements logic for 'flagging' gets/scans/puts as neeeded to be part of the transaction. Does this under the
 * old 'zookeeper' style of transactions (i.e. not snapshot isolation)
 */
public class ZkTransactionGetsPuts implements ITransactionGetsPuts {
    @Override
    public void prepDelete(String transactionId, Delete delete) {
        delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepPut(String transactionId, Put put) {
        put.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepGet(String transactionId, Get get) {
        get.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepScan(String transactionId, Scan scan) {
        scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }
}
