package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.*;
import com.splicemachine.job.JobStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/23/13
 */
public class DistinctScanOperation extends ScanOperation{
    private static final long serialVersionUID = 3l;
    private static final List<NodeType> nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);

    public DistinctScanOperation() {
    }

    private int hashKeyItem;
    private String tableName;
    private String indexName;
    private int[] keyColumns;
    private List<KeyValue> values;
    private boolean completed = false;
    private Serializer serializer = Serializer.get();
    protected Hasher hasher;
    protected byte[] currentByteArray;
	private HashBuffer<ByteBuffer,ExecRow> currentRows = new HashBuffer<ByteBuffer,ExecRow>(SpliceConstants.ringBufferSize); 
	private final HashBuffer.Merger<ByteBuffer,ExecRow> merger = new HashBuffer.Merger<ByteBuffer,ExecRow>() {
		@Override
		public ExecRow shouldMerge(ByteBuffer key){
			return currentRows.get(key);
		}

		@Override
		public void merge(ExecRow curr,ExecRow next){
            //throw away the second row, since we only want to keep the first
            //this is effectively a no-op
		}
	};
	
  

    public DistinctScanOperation(long conglomId,
                                 StaticCompiledOpenConglomInfo scoci, Activation activation,
                                 GeneratedMethod resultRowAllocator,
                                 int resultSetNumber,
                                 int hashKeyItem,
                                 String tableName,
                                 String userSuppliedOptimizerOverrides,
                                 String indexName,
                                 boolean isConstraint,
                                 int colRefItem,
                                 int lockMode,
                                 boolean tableLocked,
                                 int isolationLevel,
                                 double optimizerEstimatedRowCount,
                                 double optimizerEstimatedCost)
            throws StandardException
    {
        super(conglomId,
                activation,
                resultSetNumber,
                null,
                -1,
                null,
                -1,
                true,
                null,
                resultRowAllocator,
                lockMode,
                tableLocked,
                isolationLevel,
                colRefItem,
                optimizerEstimatedRowCount,
                optimizerEstimatedCost);
        this.hashKeyItem = hashKeyItem;
        this.tableName = tableName;
        this.indexName = indexName;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableName = in.readUTF();
        if(in.readBoolean())
            indexName = in.readUTF();
        hashKeyItem = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
        out.writeInt(hashKeyItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);

        FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(hashKeyItem);
        FormatableIntHolder[] fihArray = (FormatableIntHolder[])fah.getArray(FormatableIntHolder.class);

        keyColumns = new int[fihArray.length];
        for(int index=0;index<fihArray.length;index++){
            keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(accessedCols,fihArray[index].getInt());
        }
	    hasher = new Hasher(getExecRowDefinition().getRowArray(),keyColumns,null,sequence[0]);
	    values = new ArrayList<KeyValue>(currentRow.nColumns());
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        return Collections.emptyList();
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
    	if (completed) {
    		if(currentRows!=null &&currentRows.size()>0) {
    			ByteBuffer key = currentRows.keySet().iterator().next();
    			return makeCurrent(key.array(),currentRows.remove(key));
    		}
    		else 
    			return null;
    	}
        try{
            do{
                values.clear();
                regionScanner.next(values);
                if(values.isEmpty()) continue;
                SpliceUtils.populate(values,currentRow.getRowArray(),accessedCols,baseColumnMap,serializer);
                ExecRow row = currentRow.getClone();
                currentByteArray = hasher.generateSortedHashKeyWithoutUniqueKey(row.getRowArray()); 
                if (!currentRows.merge(ByteBuffer.wrap(currentByteArray), row, merger)) {
                    ExecRow finalized = currentRows.add(ByteBuffer.wrap(currentByteArray),row);
                    if(finalized!=null&&finalized!=row){
                        return finalized;
                    }
                }
            }while(!values.isEmpty());

            completed = true;
            if(currentRows.size()>0) {
    			ByteBuffer key = currentRows.keySet().iterator().next();
    			return makeCurrent(key.array(),currentRows.remove(key));
    		} else
                return null;
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        return currentRow;
    }

    @Override
    public SpliceOperation getLeftOperation() {
        return null;
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        try{
            Scan scan = Scans.buildPrefixRangeScan(sequence[0],SpliceUtils.NA_TRANSACTION_ID);
            return new ClientScanProvider(SpliceConstants.TEMP_TABLE_BYTES,scan,template,null);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, ExecRow template) throws StandardException {
        return getMapRowProvider(top,template);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        Scan scan = buildScan();
        RowProvider provider = new ClientScanProvider(Bytes.toBytes(Long.toString(conglomId)),scan,getExecRowDefinition(),null);

        SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation,this);
        return provider.shuffleRows(soi);
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        RowProvider provider = getReduceRowProvider(this,getExecRowDefinition());
        return new SpliceNoPutResultSet(activation,this,provider);
    }

    @Override
    public OperationSink.Translator getTranslator() throws IOException {
        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row,byte[] postfix) throws IOException {
                Put put = Puts.buildInsert(currentByteArray,row.getRowArray(),SpliceUtils.NA_TRANSACTION_ID,serializer);
                return Collections.<Mutation>singletonList(put);
            }

            @Override
            public boolean mergeKeys() {
                return true;
            }
        };
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Distinct"+super.prettyPrint(indentLevel);
    }	
	private <T extends ExecRow> ExecRow makeCurrent(byte[] key, T row) throws StandardException{
		setCurrentRow(row);
		currentByteArray = key;
		return row;
	}
}
