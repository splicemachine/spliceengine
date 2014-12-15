package com.splicemachine.si.data.hbase;

import com.splicemachine.async.QueueingAsyncScanner;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.collections.ForwardingCloseableIterator;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.async.AsyncScanner;
import com.splicemachine.metrics.Metrics;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

import com.splicemachine.async.HBaseClient;

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

    private static final RowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(new RowKeyDistributorByHashPrefix.Hasher() {
        @Override public byte[] getHashPrefix(byte[] originalKey) { return new byte[]{originalKey[0]}; }

        @Override
        public byte[][] getAllPossiblePrefixes() {
            byte[][] bytes = new byte[16][];
            for(int i=0;i<16;i++){
                bytes[i] = new byte[]{(byte)i};
            }
            return bytes;
        }

        @Override
        public int getPrefixLength(byte[] adjustedKey) {
            return 1;
        }
    });

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
