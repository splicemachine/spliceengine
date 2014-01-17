package com.splicemachine.derby.impl.sql.execute.operations.framework;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.store.access.hbase.ByteArraySlice;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.storage.EntryDecoder;
import com.yammer.metrics.core.Timer;
/**
 * Iterator over the source provided utilizing the sources nextRow(SpliceRuntimeContext) method.
 * 
 * The opens and closes are no-ops.
 *
 */
public class RegionScannerIterator implements StandardIterator<ExecRow> {
	protected RegionScanner regionScanner;
		protected List<KeyValue> keyValues;
		protected long rowsRead;
		protected GroupedRow groupedRow;
		protected EntryDecoder rowDecoder;
		protected ExecRow currentRow;
		protected RowLocation currentRowLocation;
		protected int[] baseColumnMap;
		protected boolean isIndex;
		protected ByteArraySlice slice;
		protected Timer timer;

		public RegionScannerIterator(RegionScanner regionScanner, List<KeyValue> keyValues, ExecRow currentRow, RowLocation currentRowLocation, int[] baseColumnMap, boolean isIndex, Timer timer) {
			this.regionScanner = regionScanner;
			this.keyValues = keyValues;
			this.groupedRow = new GroupedRow();
			this.currentRow = currentRow;
			this.currentRowLocation = currentRowLocation;
			this.baseColumnMap = baseColumnMap;
			this.isIndex = isIndex;
			this.timer = timer;
		}
		/**
		 * No-op
		 */
        @Override public void open() throws StandardException, IOException { }
        /**
         * No-Op
         */
        @Override public void close() throws StandardException, IOException { }
        /**
         * Retrieve the nextrow from the source.
         */
        @Override
        public ExecRow next(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        	   long start = System.nanoTime();
               try{
                   keyValues.clear();
                   regionScanner.next(keyValues);
       			if (keyValues.isEmpty()) {
       				currentRowLocation = null;
       				currentRow = null;
       				return currentRow;
       			} else {
       				if(rowDecoder==null)
       					rowDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
                       currentRow.resetRowArray();
                       DataValueDescriptor[] fields = currentRow.getRowArray();
                       if (fields.length != 0) {
                       	for(KeyValue kv:keyValues){
                               //should only be 1
                       		RowMarshaller.sparsePacked().decode(kv,fields,baseColumnMap,rowDecoder);
                       	}
                       }
                       if(isIndex && currentRow.nColumns() > 0 && currentRow.getColumn(currentRow.nColumns()).getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID){
                           /*
                            * If indexName !=null, then we are currently scanning an index,
                            *so our RowLocation should point to the main table, and not to the
                            * index (that we're actually scanning)
                            */
                           currentRowLocation = (RowLocation) currentRow.getColumn(currentRow.nColumns());
                       } else {
                       	slice.updateSlice(keyValues.get(0).getBuffer(), keyValues.get(0).getRowOffset(), keyValues.get(0).getRowLength());
                       	currentRowLocation.setValue(slice);
                       }
                       rowsRead++;
           			SpliceBaseOperation.checkInterrupt(rowsRead,SpliceConstants.interruptLoopCheck);
           			
       			}
       		}finally{
                   if(SpliceConstants.collectStats)
                       timer.update(System.nanoTime()-start,TimeUnit.NANOSECONDS);
               }
			return currentRow;

        }
}
