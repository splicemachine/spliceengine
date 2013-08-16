package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.primitives.Bytes;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.storage.ProvidesDefaultClientScanProvider;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class DistinctScalarAggregateOperation extends GenericAggregateOperation{
    private static final long serialVersionUID=1l;

    private int orderItem;
    private int maxRowSize;
    private int[] keyColumns;

//    private RingBuffer<ExecIndexRow> currentAggregations = new RingBuffer<ExecIndexRow>(1000); //TODO -sf- make configurable
    private HashBuffer<ByteBuffer,ExecIndexRow> currentAggregations = new HashBuffer<ByteBuffer, ExecIndexRow>(SpliceConstants.ringBufferSize);
    private KeyType keyHasher = KeyType.FIXED_PREFIX;
    private List<Pair<ByteBuffer,ExecRow>> finishedResults = Lists.newArrayList();
    private boolean isTemp;
    private boolean completedExecution =false;
    private List<KeyValue> keyValues;
    private static final Logger LOG = Logger.getLogger(DistinctScalarAggregateOperation.class);
    private final HashMerger<ByteBuffer,ExecIndexRow> hashMerger = new HashMerger<ByteBuffer, ExecIndexRow>() {
        @Override
        public void merge(HashBuffer<ByteBuffer, ExecIndexRow> currentRows, ExecIndexRow one, ExecIndexRow two) {
            if(!isTemp) return; //throw away the row if it's not on the temp space

           /*
             * We need to merge aggregates, but ONLY if they have different
             * keyColumns values.
             */
            boolean match =true;
            for(int keyColPos:keyColumns){
                try{
                    DataValueDescriptor dvdOne = one.getColumn(keyColPos+1);
                    DataValueDescriptor dvdTwo = two.getColumn(keyColPos+1);
                    if(dvdOne.compare(dvdTwo)!=0){
                        match = false;
                        break;
                    }
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
            if(!match){
                //merge the aggregate
                try {
                    mergeAggregates(one,two);
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
        }

        @Override
        public ExecIndexRow shouldMerge(HashBuffer<ByteBuffer, ExecIndexRow> currentRows, ByteBuffer key) {
            if(isTemp){
                //if it's the temp, merge all rows together
                if(currentRows.size()>0)
                    return currentRows.values().iterator().next();
            }

            return currentRows.get(key);
//            //merge ALL rows when we're on the temp table
//            if(isTemp) return true;
//            for(int keyColPos:keyColumns){
//                try{
//                    DataValueDescriptor dvdOne = one.getColumn(keyColPos+1);
//                    DataValueDescriptor dvdTwo = two.getColumn(keyColPos+1);
//                    if(dvdOne.compare(dvdTwo)!=0) return false;
//                } catch (StandardException e) {
//                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
//                }
//            }
//            return true;
        }
    };

    private final RingBuffer.Merger<ExecIndexRow> merger = new RingBuffer.Merger<ExecIndexRow>() {
        @Override
        public void merge(ExecIndexRow one, ExecIndexRow two) {
            if(!isTemp) return; // throw away the row if it's not on the temp space
            /*
             * We need to merge aggregates, but ONLY if they have different
             * keyColumns values.
             */
            boolean match =true;
            for(int keyColPos:keyColumns){
                try{
                    DataValueDescriptor dvdOne = one.getColumn(keyColPos+1);
                    DataValueDescriptor dvdTwo = two.getColumn(keyColPos+1);
                    if(dvdOne.compare(dvdTwo)!=0){
                        match = false;
                        break;
                    }
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
            if(!match){
                //merge the aggregate
                try {
                    mergeAggregates(one,two);
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
        }

        @Override
        public boolean shouldMerge(ExecIndexRow one, ExecIndexRow two) {
            //merge ALL rows when we're on the temp table
            if(isTemp) return true;
            for(int keyColPos:keyColumns){
                try{
                    DataValueDescriptor dvdOne = one.getColumn(keyColPos+1);
                    DataValueDescriptor dvdTwo = two.getColumn(keyColPos+1);
                    if(dvdOne.compare(dvdTwo)!=0) return false;
                } catch (StandardException e) {
                    SpliceLogUtils.logAndThrowRuntime(LOG,e);
                }
            }
            return true;
        }
    };
    private MultiFieldEncoder keyEncoder;
    private ByteBuffer emittedKey;

    private void mergeAggregates(ExecIndexRow one, ExecIndexRow two) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.merge(two,one);
        }
    }

    private RowDecoder decoder;

    public DistinctScalarAggregateOperation(){}

    public DistinctScalarAggregateOperation(SpliceOperation source,
                                            boolean isInSortedOrder,
                                            int aggregateItem,
                                            int orderItem,
                                            GeneratedMethod rowAllocator,
                                            int maxRowSize,
                                            int resultSetNumber,
                                            boolean singleInputRow,
                                            double optimizerEstimatedRowCount,
                                            double optimizerEstimatedCost) throws StandardException{
        super(source,aggregateItem,source.getActivation(),rowAllocator,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);

        this.orderItem = orderItem;
        this.maxRowSize = maxRowSize;

        init(SpliceOperationContext.newContext(source.getActivation()));
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        ExecRow clone = sourceExecIndexRow.getClone();
        SpliceUtils.populateDefaultValues(clone.getRowArray(),0);
        return clone;
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder rowDecoder) throws StandardException {
        try{
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
            //make sure that we filter out failed tasks
            SuccessFilter filter = new SuccessFilter(failedTasks,false);
            reduceScan.setFilter(filter);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        SpliceUtils.setInstructions(reduceScan,activation,top);
        return new ProvidesDefaultClientScanProvider("distinctScalarAggregateReduce",SpliceConstants.TEMP_TABLE_BYTES,reduceScan,rowDecoder);
    }

    @Override
    public void close() throws StandardException {
        super.close();
        if(reduceScan!=null)
            SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder rowDecoder) throws StandardException {
        return ((SpliceOperation)source).getMapRowProvider(top,rowDecoder);
    }

    public ExecRow getNextSinkRow() throws StandardException {
        return aggregate(false);
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        if(finishedResults.size()>0)
            return makeCurrent(finishedResults.remove(0));
        else if(completedExecution)
            return null;

        return aggregate(true);
    }

    private ExecRow aggregate(boolean isTemp) throws StandardException{
        ExecIndexRow row = isTemp? getNextRowFromTemp(): getNextRowFromSource(true);
        if(row == null){
            return finalizeResults();
        }
        do{
            if(!isTemp)
                initializeAggregation(row);

            if(keyEncoder==null){
                keyEncoder = MultiFieldEncoder.create(row.nColumns());
                keyEncoder.setRawBytes(uniqueSequenceID);
                keyEncoder.mark();
            }
            keyEncoder.reset();

            ((KeyMarshall)keyHasher).encodeKey(row.getRowArray(), keyColumns, null, null, keyEncoder);
            byte[] key = keyEncoder.build();
            ByteBuffer keyBuffer = ByteBuffer.wrap(key);
            if(!currentAggregations.merge(keyBuffer,row,hashMerger)){
                ExecIndexRow rowClone = (ExecIndexRow)row.getClone();

                Map.Entry<ByteBuffer, ExecIndexRow> entry = currentAggregations.add(keyBuffer, rowClone);
                if(entry!=null){
                    ExecIndexRow rowToEmit = entry.getValue();
                    emittedKey = entry.getKey();
                    if(rowToEmit!=null&&rowToEmit!=rowClone)
                        return makeCurrent(entry.getKey(),finishAggregation(rowToEmit));
                }
            }
            row = isTemp?getNextRowFromTemp(): getNextRowFromSource(true);
        }while(row!=null);

        return finalizeResults();
    }

    private void initializeAggregation(ExecIndexRow row) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.initialize(row);
            aggregator.accumulate(row,row);
        }
    }

    private ExecIndexRow getNextRowFromSource(boolean doClone) throws StandardException {
        ExecRow sourceRow = source.getNextRowCore();
        if(sourceRow==null) return null;
        sourceExecIndexRow.execRowToExecIndexRow(doClone? sourceRow.getClone(): sourceRow);
        return sourceExecIndexRow;
    }

    private ExecIndexRow getNextRowFromTemp() throws StandardException {
        if(keyValues==null)
            keyValues = new ArrayList<KeyValue>(sourceExecIndexRow.nColumns()+1);

        keyValues.clear();
        try{
            regionScanner.next(keyValues);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG,e);
            throw StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION);
        }
        if(keyValues.isEmpty())
            return null;
        else{
            if(decoder==null)
                decoder = getRowEncoder().getDual(sourceExecIndexRow,true);
            return (ExecIndexRow)decoder.decode(keyValues);
        }
    }

    private ExecRow finalizeResults() throws StandardException {
        completedExecution=true;
        for(Map.Entry<ByteBuffer,ExecIndexRow> row:currentAggregations.entrySet()){
            finishedResults.add(Pair.<ByteBuffer, ExecRow>newPair(row.getKey(), finishAggregation(row.getValue())));
//            finishedResults.add(finishAggregation(row));
        }
        currentAggregations.clear();
        if(finishedResults.size()>0)
            return makeCurrent(finishedResults.remove(0));
        else return null;
    }

    private ExecRow makeCurrent(Pair<ByteBuffer,ExecRow> remove) {
        setCurrentRow(remove.getSecond());
        emittedKey = remove.getFirst();
        return remove.getSecond();
    }

    private ExecRow makeCurrent(ByteBuffer key,ExecRow remove) {
        setCurrentRow(remove);
        emittedKey = key;
        return remove;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        orderItem = in.readInt();
    }

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        KeyMarshall keyType = new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
                byte[] key = BytesUtil.concatenate(emittedKey.array(),keyPostfix);
                keyEncoder.setRawBytes(key);
            }

            @Override
            public void decode(DataValueDescriptor[] data, int[] reversedKeyColumns, boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
                ((KeyMarshall)keyHasher).decode(data, reversedKeyColumns,sortOrder,rowDecoder);
            }

            @Override
            public int getFieldCount(int[] keyColumns) {
                return 1;
            }
        };
        return RowEncoder.create(sourceExecIndexRow.nColumns(),
                keyColumns,null,
                null,
//                KeyType.FIXED_PREFIX_UNIQUE_POSTFIX,
                keyType,
                RowMarshaller.packed());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(orderItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
        super.init(context);

        ExecPreparedStatement gsps = activation.getPreparedStatement();
        ColumnOrdering[] order =
                (ColumnOrdering[])
                        ((FormatableArrayHolder)gsps.getSavedObject(orderItem)).getArray(ColumnOrdering.class);
        keyColumns = new int[order.length];
        for(int index=0;index<order.length;index++){
            keyColumns[index] = order[index].getColumnId();
        }

        isTemp = !context.isSink() || context.getTopOperation()!=this;
    }


}
