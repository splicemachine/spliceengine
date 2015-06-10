package com.splicemachine.si.data.hbase;

import com.splicemachine.async.AsyncScanner;
import com.splicemachine.async.HBaseClient;
import com.splicemachine.async.QueueingAsyncScanner;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.collections.ForwardingCloseableIterator;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.metrics.Metrics;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 7/29/14
 */
public class TxnTable extends HbTable {
    private final HBaseClient hbaseClient;

    public TxnTable(HTableInterface table, HBaseClient hbaseClient) {
        super(table);
        this.hbaseClient = hbaseClient;
    }

    @Override
    public CloseableIterator<Result> scan(Scan scan) throws IOException {
        final AsyncScanner scanner = new QueueingAsyncScanner(SIAsyncUtils.convert(scan, hbaseClient, SIConstants.TRANSACTION_TABLE_BYTES),Metrics.noOpMetricFactory());
        scanner.open();
        return new ForwardingCloseableIterator<Result>(scanner.iterator()) {
            @Override
            public void close() throws IOException {
                scanner.close();
            }
        };
    }
}