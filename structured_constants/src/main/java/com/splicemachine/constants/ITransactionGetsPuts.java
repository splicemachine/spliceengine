package com.splicemachine.constants;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Capture the calls needed to get transactional behavior on HBase gets/scans/puts.
 */
public interface ITransactionGetsPuts {
    void prepPut(String transactionId, Put put);
    void prepGet(String transactionId, Get get);
    void prepScan(String transactionId, Scan scan);
    void prepDelete(String transactionId, Delete delete);
    String getTransactionIdForPut(Put put);
}
