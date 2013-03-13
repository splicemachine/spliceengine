package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.util.Arrays;
import java.util.NavigableMap;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.execute.UpdateConstantAction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.google.common.base.Throwables;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.utils.SpliceLogUtils;

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
		recordConstructorTime(); 
	}

	@Override
	public void init(SpliceOperationContext context){
		SpliceLogUtils.trace(LOG,"init with regionScanner %s",regionScanner);
		super.init(context);
        UpdateConstantAction constantAction = (UpdateConstantAction)constants;
		heapConglom = constantAction.getConglomerateId();

        int[] pkCols = constantAction.getPkColumns();
        if(pkCols!=null)
            pkColumns = fromIntArray(pkCols);

	}

	@Override
	public SinkStats sink() throws IOException {
		SpliceLogUtils.trace(LOG,"sink on transactionID="+transactionID);
        SinkStats.SinkAccumulator stats = SinkStats.uniformAccumulator();
        stats.start();
        SpliceLogUtils.trace(LOG, ">>>>statistics starts for sink for UpdateOperation at "+stats.getStartTime());
		ExecRow nextRow;
		int[] colPositionMap = null;
		FormatableBitSet heapList = ((UpdateConstantAction)constants).getBaseRowReadList();

		//Use HTable to do inserts instead of HeapConglomerateController - see Bug 188
		HTableInterface htable = SpliceAccessManager.getFlushableHTable(Bytes.toBytes(""+heapConglom));
        Serializer serializer = new Serializer();
        //lazily instantiated because we might not need it (only needed if we modify primary keys)
        RowSerializer rowInsertSerializer = null;

        boolean modifiedPrimaryKeys = false;
        if(pkColumns!=null){
            for(int pkCol = pkColumns.anySetBit();pkCol!=-1;pkCol= pkColumns.anySetBit(pkCol)){
                if(heapList.isSet(pkCol+1)){
                    modifiedPrimaryKeys = true;
                    break;
                }
            }
        }
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
					 * But if heapList = {2}, then nextRow looks like {old2,new2,rowLocation}, which makes our colPositionMap
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
                /*
                 * If we have primary keys, it's possible that we have modified one of them, which will change the row
                 * location. In that case, the location above isn't used for a Put, but for a delete instead.
                 *
                 * To find out, we take the intersection between heapList and pkColumns and, if it's non-empty, we
                 * deal with primary keys. Otherwise, we do an update as usual
                 */
                if(!modifiedPrimaryKeys){
                    SpliceLogUtils.trace(LOG, "UpdateOperation sink, nextRow=%s, validCols=%s,colPositionMap=%s", nextRow, heapList, Arrays.toString(colPositionMap));
                    Put put = Puts.buildUpdate(location, nextRow.getRowArray(), heapList, colPositionMap, this.transactionID.getBytes(), serializer);
                    put.setAttribute(Puts.PUT_TYPE,Puts.FOR_UPDATE);
                    htable.put(put);
                }else{
                    if(rowInsertSerializer==null)
                        rowInsertSerializer = new RowSerializer(nextRow.getRowArray(),pkColumns,colPositionMap,false);
                    SpliceLogUtils.trace(LOG,"UpdateOperation sink: primary keys modified");

                    /*
                     * Since we modified a primary key, we have to delete the data and reinsert it into the new location
                     * That means we have to do a Get to get the full row, then merge it with the update values,
                     * re-insert it in the new location, and then delete the old location.
                     */
                    Get remoteGet = new Get(location.getBytes());
                    remoteGet.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
                    Result result = htable.get(remoteGet);

                    //convert Result into put under the new row key
                    byte[] newRowKey = rowInsertSerializer.serialize(nextRow.getRowArray());
                    Put newPut = new Put(newRowKey);
                    NavigableMap<byte[],byte[]> familyMap = result.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
                    for(byte[] qualifier:familyMap.keySet()){
                        int position = Integer.parseInt(Bytes.toString(qualifier));
                        if(heapList.isSet(position+1)){
                            //put the new value into the position instead of the old one
                            DataValueDescriptor dvd = nextRow.getRowArray()[colPositionMap[position+1]];
                            newPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES, qualifier, serializer.serialize(dvd));
                        }else
                            newPut.add(HBaseConstants.DEFAULT_FAMILY_BYTES,qualifier,familyMap.get(qualifier));
                    }
                    htable.put(newPut);

                    //now delete the old entry
                    Delete delete = new Delete(location.getBytes());
                    htable.delete(delete);
                }

                stats.sinkAccumulator().tick(System.nanoTime() - start);
            }while(nextRow!=null);
			htable.flushCommits();
			htable.close();
		} catch (Exception e) {
            if(e instanceof RetriesExhaustedWithDetailsException){
                Throwable t= ((RetriesExhaustedWithDetailsException) e).getCause(0);
                if(t instanceof IOException) throw (IOException)t;
                throw new IOException(t);
            }
            Throwable t = Throwables.getRootCause(e);
            if(t instanceof IOException) throw (IOException)t;
            SpliceLogUtils.logAndThrow(LOG,new IOException(e));
		}
        //return stats.finish();
		SinkStats ss = stats.finish();
		SpliceLogUtils.trace(LOG, ">>>>statistics finishes for sink for UpdateOperation at "+stats.getFinishTime());
        return ss;
	}

	
	@Override
	public String toString() {
		return "Update{destTable="+heapConglom+",source=" + source + "}";
	}
}
