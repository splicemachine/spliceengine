package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
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
         * We use nextRaw() because it avoids region availability checks--once
         * we get as far as calling this.nextRow(SpliceRuntimeContext), we should
         * ensure that we are fully locked and the region's availability has been checked,
         * but then we avoid making that check again until we've finished reading our aggregate
         * data.
         */
        regionScanner.nextRaw(keyValues,null);

        if(keyValues.isEmpty())
            return null;
        return (ExecIndexRow)scanDecoder.decode(keyValues);
    }
}
