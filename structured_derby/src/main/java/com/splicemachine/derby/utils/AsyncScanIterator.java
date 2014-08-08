package com.splicemachine.derby.utils;

import com.google.common.base.Function;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.AsyncDistributedScanner;
import com.splicemachine.derby.impl.storage.DerbyAsyncScannerUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.async.AsyncScanner;
import com.splicemachine.hbase.async.SimpleAsyncScanner;
import com.splicemachine.hbase.async.SortedGatheringScanner;
import com.splicemachine.metrics.MetricFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/14/14
 */
public class AsyncScanIterator implements StandardIterator<ExecRow> {
    private final AsyncScanner scanner;
    private final PairDecoder rowDecoder;

    public AsyncScanIterator(AsyncScanner scanner, PairDecoder rowDecoder) {
        this.scanner = scanner;
        this.rowDecoder = rowDecoder;
    }

    @Override public void open() throws StandardException, IOException { scanner.open(); }

    @Override
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        try {
            List<KeyValue> kvs = scanner.nextKeyValues();
            if(kvs==null) return null;
            return rowDecoder.decode(kvs.get(0)); //there should only ever be a single entry
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        scanner.close();
    }

    public static StandardIterator<ExecRow> create(final byte[] tableName, Scan baseScan,
                                                   PairDecoder pairDecoder,
                                                   RowKeyDistributor rowKeyDistributor,
                                                   MetricFactory metricFactory) throws IOException {
       AsyncScanner scanner = SortedGatheringScanner.newScanner(baseScan, SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory, new Function<Scan, Scanner>() {
           @Nullable
           @Override
           public Scanner apply(@Nullable Scan input) {
               return DerbyAsyncScannerUtils.convertScanner(input,tableName, SimpleAsyncScanner.HBASE_CLIENT);
           }
       },rowKeyDistributor, Bytes.BYTES_COMPARATOR);
        return new AsyncScanIterator(scanner,pairDecoder);
    }
}
