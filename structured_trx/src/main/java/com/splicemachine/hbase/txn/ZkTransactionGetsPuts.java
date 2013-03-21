package com.splicemachine.hbase.txn;

import com.splicemachine.constants.ITransactionGetsPuts;
import com.splicemachine.constants.TxnConstants;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Implements logic for 'flagging' gets/scans/puts as neeeded to be part of the transaction. Does this under the
 * old 'zookeeper' style of transactions (i.e. not snapshot isolation)
 */
public class ZkTransactionGetsPuts implements ITransactionGetsPuts {
    @Override
    public void prepDelete(String transactionId, Delete delete) {
        if(transactionId!=null)
            delete.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepPut(String transactionId, Put put) {
        if(transactionId!=null)
            put.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepGet(String transactionId, Get get) {
        if(transactionId!=null)
            get.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public void prepScan(String transactionId, Scan scan) {
        if(transactionId!=null)
            scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionId.getBytes());
    }

    @Override
    public String getTransactionIdForPut(Put put) {
        return Bytes.toString(put.getAttribute(TxnConstants.TRANSACTION_ID));
    }

    @Override
    public String getTransactionIdForDelete(Delete delete) {
        return Bytes.toString(delete.getAttribute(TxnConstants.TRANSACTION_ID));
    }
}
