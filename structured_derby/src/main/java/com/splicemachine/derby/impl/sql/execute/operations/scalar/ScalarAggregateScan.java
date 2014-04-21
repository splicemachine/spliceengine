package com.splicemachine.derby.impl.sql.execute.operations.scalar;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.RegionAwareScanner;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.stats.MetricFactory;

/**
 * @author Scott Fines
 * Created on: 10/8/13
 */
public class ScalarAggregateScan implements ScalarAggregateSource{

    private final PairDecoder scanDecoder;
    private final RegionAwareScanner regionScanner;

    public ScalarAggregateScan(PairDecoder scanDecoder, MetricFactory metricFactory, String txnId, HRegion region, Scan localScan) throws StandardException {
        this.scanDecoder = scanDecoder;
				//TODO -sf- is this safe?
				final byte bucket = localScan.getStartRow()[0];
				final byte[] table = region.getTableDesc().getName();

				int prefixOffset = scanDecoder.getKeyPrefixOffset();
				final byte[] prefix = new byte[prefixOffset];
				BytesUtil.slice(prefix,0,prefixOffset);
				this.regionScanner = RegionAwareScanner.create(txnId,region,localScan,table,
								new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES) {
						@Override
						public byte[] getStartKey(Result result) {
								return prefix;
						}

						@Override
						public byte[] getStopKey(Result result) {
								return BytesUtil.unsignedCopyAndIncrement(prefix);
						}
				},metricFactory);
				regionScanner.open();
    }


    @Override
    public ExecIndexRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException {
//        if(keyValues==null)
//            keyValues = Lists.newArrayListWithExpectedSize(2);
//        keyValues.clear();
        /*
         * We use nextRaw() because it avoids region availability checks--once
         * we get as far as calling this.nextRow(SpliceRuntimeContext), we should
         * ensure that we are fully locked and the region's availability has been checked,
         * but then we avoid making that check again until we've finished reading our aggregate
         * data.
         */
				Result next = regionScanner.next();
				if(next==null || next.size()<=0)
						return null;

//				if(keyValues.isEmpty())
//            return null;
        return (ExecIndexRow)scanDecoder.decode(CellUtils.matchDataColumn(next.rawCells()));
    }
}
