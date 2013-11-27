package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.SimpleRegionAwareRowProvider;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 10/11/13
 */
public class DistinctScalarAggregateScan implements ScalarAggregateSource {
    private final RowProvider regionAwareProvider;
    private final ExecIndexRow toIndexRow;

    public DistinctScalarAggregateScan(HRegion region,
                                       Scan scan,
                                       PairDecoder decoder,
                                       final int[] keyCols,
                                       ExecIndexRow toIndexRow,
                                       SpliceRuntimeContext spliceRuntimeContext,
                                       final byte[] uniqueId) throws StandardException {
        this.toIndexRow = toIndexRow;
        final DataValueDescriptor[] cols = toIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow(), SpliceDriver.getKryoPool());
                fieldDecoder.seek(uniqueId.length+1);

                int adjusted = DerbyBytesUtil.skip(fieldDecoder,keyCols,cols);
                fieldDecoder.reset();
                return fieldDecoder.slice(adjusted+uniqueId.length+1);
            }

            @Override
            public byte[] getStopKey(Result result) {
                byte[] start = getStartKey(result);
                BytesUtil.unsignedIncrement(start, start.length - 1);
                return start;
            }
        };
        this.regionAwareProvider  = new SimpleRegionAwareRowProvider(
                "groupedAggregateRowProvider",
                SpliceUtils.NA_TRANSACTION_ID,
                region,
                scan,
                SpliceConstants.TEMP_TABLE_BYTES,
                SpliceConstants.DEFAULT_FAMILY_BYTES,
                decoder,
                boundary,
                spliceRuntimeContext); // Make sure the partitioner (Region Aware) worries about group by keys, not the additonal unique keys
        regionAwareProvider.open();
    }

    @Override
    public ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(!regionAwareProvider.hasNext()){
            return null;
        }
        ExecRow row = regionAwareProvider.next();
        return (ExecIndexRow)row;
    }
}
