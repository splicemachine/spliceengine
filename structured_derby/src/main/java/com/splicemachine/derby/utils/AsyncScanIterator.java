package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.AsyncDistributedScanner;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.stats.MetricFactory;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Scan;
import org.hbase.async.KeyValue;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/14/14
 */
public class AsyncScanIterator implements StandardIterator<ExecRow> {
    private final AsyncDistributedScanner scanner;
    private final PairDecoder rowDecoder;

    public AsyncScanIterator(AsyncDistributedScanner scanner, PairDecoder rowDecoder) {
        this.scanner = scanner;
        this.rowDecoder = rowDecoder;
    }

    @Override public void open() throws StandardException, IOException { scanner.open(); }

    @Override
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        List<KeyValue> kvs = scanner.nextKvs();
        if(kvs==null) return null;
        return rowDecoder.decode(kvs.get(0)); //there should only ever be a single entry
    }

    @Override
    public void close() throws StandardException, IOException {
        scanner.close();
    }

    public static StandardIterator<ExecRow> create(byte[] tableName, Scan[] baseScans, PairDecoder pairDecoder,MetricFactory metricFactory) {
        return new AsyncScanIterator(AsyncDistributedScanner.create(tableName,baseScans,metricFactory),pairDecoder);
    }
}
