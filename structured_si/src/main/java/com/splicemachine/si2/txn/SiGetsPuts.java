package com.splicemachine.si2.txn;

import com.splicemachine.constants.ITransactionGetsPuts;
import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.hbase.HGet;
import com.splicemachine.si2.data.hbase.HScan;
import com.splicemachine.si2.si.api.ClientTransactor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Plugin for the derby code to 'flag' HBase operations for snashot isolation treatment.
 */
public class SiGetsPuts implements ITransactionGetsPuts {
    private final ClientTransactor transactor;

    public SiGetsPuts(ClientTransactor transactor) {
        this.transactor = transactor;
    }

    @Override
    public void prepDelete(String transactionId, Delete delete) {
        throw new RuntimeException("not yet implemented");
    }

    @Override
    public void prepPut(String transactionId, Put put) {
        transactor.initializePut(transactor.transactionIdFromString(transactionId), put);
    }

    @Override
    public void prepGet(String transactionId, Get get) {
        HGet sGet = new HGet(get);
        transactor.initializeGet(transactor.transactionIdFromString(transactionId), sGet);
    }

    @Override
    public void prepScan(String transactionId, Scan scan) {
        HScan hScan = new HScan(scan);
        transactor.initializeScan(transactor.transactionIdFromString(transactionId), hScan);
    }

    @Override
    public String getTransactionIdForPut(Put put) {
        return transactor.getTransactionIdFromPut(put).getTransactionID();
    }

    @Override
    public String getTransactionIdForDelete(Delete delete) {
        return transactor.getTransactionIdFromDelete(delete).getTransactionID();
    }
}
