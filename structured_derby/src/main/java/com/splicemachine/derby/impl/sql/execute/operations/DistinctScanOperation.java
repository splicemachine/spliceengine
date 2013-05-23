package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
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
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
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
    private List<ExecRow> finishedValues;
    private boolean completed = false;
    private Serializer serializer;

    private final RingBuffer<ExecRow> currentRows = new RingBuffer<ExecRow>(SpliceConstants.ringBufferSize);
    private final RingBuffer.Merger<ExecRow> distinctMerger = new RingBuffer.Merger<ExecRow>() {
        @Override
        public void merge(ExecRow one, ExecRow two) {
            //throw away the second row, since we only want to keep the first
            //this is effectively a no-op
        }

        @Override
        public boolean shouldMerge(ExecRow one, ExecRow two) {
            //make sure they match on the key columns
            for(int col:keyColumns){
                try {
                    DataValueDescriptor left = one.getColumn(col+1);
                    DataValueDescriptor right = two.getColumn(col+1);

                    //if they aren't equal, then we shouldn't merge
                    if(left.compare(right)!=0)
                        return false;
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
            return true;
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
        if(finishedValues!=null &&finishedValues.size()>0)
            return finishedValues.remove(0);
        else if(completed)
            return null;
        if(values==null)
            values = new ArrayList<KeyValue>(currentRow.nColumns());
        if(serializer==null)
            serializer = new Serializer();
        try{
            do{
                values.clear();
                regionScanner.next(values);
                if(values.isEmpty()) continue;
                SpliceUtils.populate(values,currentRow.getRowArray(),accessedCols,baseColumnMap,serializer);
                ExecRow row = currentRow.getClone();
                if(!currentRows.merge(row,distinctMerger)){
                    ExecRow finalized = currentRows.add(row);
                    if(finalized!=null&&finalized!=row){
                        return finalized;
                    }
                }
            }while(!values.isEmpty());

            completed = true;
            //we've exhausted the scan. Empty the buffer and return
            finishedValues = Lists.newArrayList(currentRows);
            if(finishedValues.size()>0)
                return finishedValues.remove(0);
            else
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
        final Hasher hasher = new Hasher(currentRow.getRowArray(),keyColumns,null,sequence[0]);

        return new OperationSink.Translator() {
            @Nonnull
            @Override
            public List<Mutation> translate(@Nonnull ExecRow row) throws IOException {
                try {
                    byte[] tempRow = hasher.generateSortedHashKeyWithPostfix(row.getRowArray(),null);
                    Put put = Puts.buildInsert(tempRow,row.getRowArray(),SpliceUtils.NA_TRANSACTION_ID,serializer);
                    return Collections.<Mutation>singletonList(put);
                } catch (StandardException e) {
                    throw Exceptions.getIOException(e);
                }
            }
        };
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Distinct"+super.prettyPrint(indentLevel);
    }
}
