package com.splicemachine.derby.utils;

import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class ScanIterator implements StandardIterator<ExecRow>{
    private final SpliceResultScanner scanner;
    private final PairDecoder rowDecoder;

    public ScanIterator(SpliceResultScanner scanner,
                        PairDecoder rowDecoder) {
        this.scanner = scanner;
        this.rowDecoder = rowDecoder;
    }

    @Override
    public void open() throws StandardException, IOException {
        scanner.open();
    }

    @Override
    public ExecRow next() throws StandardException, IOException {
        Result result = scanner.next();
        if(result==null) return null;
        return rowDecoder.decode(KeyValueUtils.matchDataColumn(result.raw()));
    }

    @Override
    public void close() throws StandardException, IOException {
        scanner.close();
    }
}
