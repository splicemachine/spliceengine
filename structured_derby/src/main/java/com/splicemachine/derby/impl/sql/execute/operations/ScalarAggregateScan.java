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
        /*
         * We are merging intermediate results against the TEMP table, or this implementation wouldn't be used.
         *
         * However, that can happen in one of two cases--a scan or a sink. A sink occurs when there is a sink
         * oepration in the plan above us, and we have been issued as a sink, while a scan occurs when the
         * final results are to be returned.
         *
         * When sinking, it's okay to fail with a NotServingRegionException, because the Task framework will
         * roll back the task, and resubmit it as two separate tasks against the correct region.
         *
         * When scanning, though, it's not okay to fail that way. The easiest way to ensure that doesn't
         * happen (with a side benefit of less locking) is to use nextRaw instead of next.
         */
        if(spliceRuntimeContext.isSinkOp())
            regionScanner.next(keyValues);
        else
            regionScanner.nextRaw(keyValues,null);
        if(keyValues.isEmpty())
            return null;
        return (ExecIndexRow)scanDecoder.decode(keyValues);
    }
}
