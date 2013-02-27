package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.UpdateConstantAction;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.Arrays;

/**
 * @author jessiezhang
 * @author Scott Fines
 */
public class UpdateOperation extends DMLWriteOperation{
	private static final Logger LOG = Logger.getLogger(UpdateOperation.class);

	public UpdateOperation() {
		super();
	}

	public UpdateOperation(NoPutResultSet source, GeneratedMethod generationClauses,
												 GeneratedMethod checkGM, Activation activation)
			throws StandardException {
		super(source, generationClauses, checkGM, activation);
		init(SpliceOperationContext.newContext(activation));
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
		heapConglom = ((UpdateConstantAction)constants).getConglomerateId();
	}

	@Override
	public SinkStats sink() {
		SpliceLogUtils.trace(LOG,"sink on transactionID="+transactionID);
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();

		ExecRow nextRow;
		int[] colPositionMap = null;
		FormatableBitSet heapList = ((UpdateConstantAction)constants).getBaseRowReadList();

		//Use HTable to do inserts instead of HeapConglomerateController - see Bug 188
		HTableInterface htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(""+heapConglom));
        Serializer serializer = new Serializer();
		try {
            do{
                long start = System.nanoTime();
                nextRow = source.getNextRowCore();
                if(nextRow==null)continue;

                if(colPositionMap==null){
					/*
					 * heapList is the position of the columns in the original row (e.g. if cols 2 and 3 are being modified,
					 * then heapList = {2,3}). We have to take that position and convert it into the actual positions
					 * in nextRow.
					 *
					 * nextRow looks like {old,old,...,old,new,new,...,new,rowLocation}, so suppose that we have
					 * heapList = {2,3}. Then nextRow = {old2,old3,new2,new3,rowLocation}. Which makes our colPositionMap
					 * look like
					 *
					 * colPositionMap[2] = 2;
					 * colPositionMap[3] = 3;
					 *
					 * But if heapLiast = {2}, then nextRow looks like {old2,new2,rowLocation}, which makes our colPositionMap
					 * look like
					 *
					 * colPositionMap[2] = 1
					 *
					 * in general, then
					 *
					 * colPositionMap[i= heapList.anySetBit()] = nextRow[heapList.numSetBits()]
					 * colPositionMap[heapList.anySetBit(i)] = nextRow[heapList.numSetBits()+1]
					 * ...
					 *
					 * and so forth
					 */
                    colPositionMap = new int[heapList.size()];
                    for(int i = heapList.anySetBit(),pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++){
                        colPositionMap[i] = pos;
                    }

                }
                stats.processAccumulator().tick(System.nanoTime()-start);

                start = System.nanoTime();
                RowLocation location= (RowLocation)nextRow.getColumn(nextRow.nColumns()).getObject(); //the location to update is always at the end
                SpliceLogUtils.trace(LOG, "UpdateOperation sink, nextRow=%s, validCols=%s,colPositionMap=%s", nextRow, heapList, Arrays.toString(colPositionMap));
                htable.put(Puts.buildUpdate(location, nextRow.getRowArray(), heapList, colPositionMap,this.transactionID.getBytes(),serializer));

                stats.sinkAccumulator().tick(System.nanoTime()-start);
            }while(nextRow!=null);
			htable.flushCommits();
			htable.close();
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
        return stats.finish();
	}

	
	@Override
	public String toString() {
		return "Update{destTable="+heapConglom+",source=" + source + "}";
	}
}
