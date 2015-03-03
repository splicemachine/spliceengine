package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.client.Result;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 11/2/13
 */
public class ScanIterator implements StandardIterator<ExecRow>{
	private static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
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
    public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        Result result = scanner.next();
        if(result==null) return null;
        return rowDecoder.decode(dataLib.matchDataColumn(result));
    }

    @Override
    public void close() throws StandardException, IOException {
        scanner.close();
    }
}
