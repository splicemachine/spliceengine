package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
class ScalarAggregateScan implements ScalarAggregateSource{

    private final RowDecoder scanDecoder;
    private List<KeyValue> keyValues;
    private final RegionScanner regionScanner;

    ScalarAggregateScan(RowDecoder scanDecoder, RegionScanner regionScanner) {
        this.scanDecoder = scanDecoder;
        this.regionScanner = regionScanner;
    }

    @Override
    public ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
        if(keyValues==null)
            keyValues = Lists.newArrayListWithExpectedSize(2);
        keyValues.clear();
        regionScanner.next(keyValues);

        if(keyValues.isEmpty())
            return null;
        return (ExecIndexRow)scanDecoder.decode(keyValues);
    }
}
