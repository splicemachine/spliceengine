package com.splicemachine.derby.utils;

import com.splicemachine.async.*;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.DerbyAsyncScannerUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.ScanDivider;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

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
        if (rowDecoder != null)
            rowDecoder.close();
    }

    public static StandardIterator<ExecRow> create(final byte[] tableName, Scan baseScan,
                                                   PairDecoder pairDecoder,
                                                   RowKeyDistributor rowKeyDistributor,
                                                   MetricFactory metricFactory) throws IOException {
        List<Scan> dividedScans = ScanDivider.divide(baseScan, rowKeyDistributor);
        List<Scanner> scanners = DerbyAsyncScannerUtils.convertScanners(dividedScans, tableName, AsyncHbase.HBASE_CLIENT, true);
        AsyncScanner scanner = new SortedMultiScanner(scanners,
                SpliceConstants.DEFAULT_CACHE_SIZE,
                Bytes.BYTES_COMPARATOR,
                metricFactory);
        return new AsyncScanIterator(scanner,pairDecoder);
    }
}
