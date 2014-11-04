package com.splicemachine.async;

import com.google.common.base.Function;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.hbase.ScanDivider;
import com.splicemachine.metrics.Metrics;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.SimpleLayout;

public class SortedGatheringScannerTest {

    public static void main(String... args) throws Exception {
        Logger.getRootLogger().addAppender(new ConsoleAppender(new SimpleLayout()));
        Logger.getRootLogger().setLevel(Level.INFO);
        Scan baseScan = new Scan();
        byte[] startRow = org.apache.hadoop.hbase.util.Bytes.toBytesBinary("\\x90\\xF4y\\x1D\\xBF\\xE9\\xF0\\x01");
        baseScan.setStartRow(startRow);
        baseScan.setStopRow(BytesUtil.unsignedCopyAndIncrement(startRow));

        RowKeyDistributorByHashPrefix.Hasher hasher = new RowKeyDistributorByHashPrefix.Hasher() {

            @Override
            public byte[] getHashPrefix(byte[] originalKey) {
                throw new UnsupportedOperationException();
            }

            @Override
            public byte[][] getAllPossiblePrefixes() {
                byte[][] buckets = new byte[16][];
                for (int i = 0; i < buckets.length; i++) {
                    buckets[i] = new byte[]{(byte) (i * 0xF0)};
                }
                return buckets;
            }

            @Override
            public int getPrefixLength(byte[] adjustedKey) {
                return 1;
            }
        };
        RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(hasher);

        final HBaseClient client = SimpleAsyncScanner.HBASE_CLIENT;
        try {
            AsyncScanner scanner = SortedGatheringScanner.newScanner(1024,
                    Metrics.noOpMetricFactory(), new Function<Scan, Scanner>() {
                        @Override
                        public Scanner apply(Scan scan) {
                            Scanner scanner = client.newScanner(SpliceConstants.TEMP_TABLE_BYTES);
                            scanner.setStartKey(scan.getStartRow());
                            byte[] stop = scan.getStopRow();
                            if (stop.length > 0)
                                scanner.setStopKey(stop);
                            return scanner;
                        }
                    }, ScanDivider.divide(baseScan, keyDistributor), null);
            scanner.open();

            Result r;
            while ((r = scanner.next()) != null) {
                System.out.println(org.apache.hadoop.hbase.util.Bytes.toStringBinary(r.getRow()));
            }
        } finally {
            client.shutdown().join();
        }
    }

}