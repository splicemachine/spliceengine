package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.storage.ScalarAggregateRowProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecPreparedStatement;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 5/21/13
 */
public class DistinctScalarAggregateOperation extends GenericAggregateOperation{
    private static final long serialVersionUID=1l;

    private int orderItem;
    private int[] keyColumns;

    private HashBuffer<ByteBuffer,ExecIndexRow> currentAggregations = new DelegateHashBuffer<ByteBuffer, ExecIndexRow>(SpliceConstants.ringBufferSize);
    private KeyType keyHasher = KeyType.FIXED_PREFIX;
    private List<Pair<ByteBuffer,ExecRow>> finishedResults = Lists.newArrayList();
    private boolean isTemp;
    private boolean completedExecution =false;
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
                    /*
                     * We need to update the keycolumns in the stored row, otherwise
                     * the count will be off.
                     *
                     * Because we sorted the data in TEMP, we know that all rows with the
                     * same keyColumns will be grouped together, so we can just update
                     * the row to be the same value.
                     */
                    for(int keyColPos:keyColumns){
                        one.getColumn(keyColPos+1).setValue(two.getColumn(keyColPos+1));
                    }
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
        }
    };

    private MultiFieldEncoder keyEncoder;
    private ByteBuffer emittedKey;

    private ScalarAggregateSource scanSource; //for operations against TEMP
    private ScalarAggregateSource sinkSource; //for operations on main table

    private void mergeAggregates(ExecIndexRow one, ExecIndexRow two) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.merge(two,one);
        }
    }

    @Deprecated
    @SuppressWarnings("UnusedDeclaration")
    public DistinctScalarAggregateOperation(){}

    @SuppressWarnings("UnusedParameters")
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

        init(SpliceOperationContext.newContext(source.getActivation()));
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
        ExecRow clone = sourceExecIndexRow.getClone();
        SpliceUtils.populateDefaultValues(clone.getRowArray(), 0);
        return clone;
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        buildReduceScan();
        SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
        return new ScalarAggregateRowProvider("distinctScalarAggregateReduce",
                SpliceConstants.TEMP_TABLE_BYTES,
                reduceScan,rowDecoder,
                spliceRuntimeContext,
                aggregates);
    }

    private void buildReduceScan() throws StandardException {
        if(reduceScan!=null)
            return; //nothing to do
        try{
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
            //make sure that we filter out failed tasks
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if(reduceScan!=null)
            SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return source.getMapRowProvider(top, rowDecoder, spliceRuntimeContext);
    }

    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(finishedResults.size()>0)
            return makeCurrent(finishedResults.remove(0));
        else if(completedExecution)
            return null;

        if(sinkSource==null){
            sinkSource = new OperationScalarAggregateSource(source,sourceExecIndexRow,true);
        }
        return aggregate(sinkSource,spliceRuntimeContext);
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(finishedResults.size()>0)
            return makeCurrent(finishedResults.remove(0));
        else if(completedExecution)
            return null;

        if(scanSource==null){
            SpliceRuntimeContext ctx = new SpliceRuntimeContext();
            buildReduceScan();
            scanSource = new DistinctScalarAggregateScan(
                    region,reduceScan,getRowEncoder(ctx).getDual(sourceExecIndexRow),keyColumns,sourceExecIndexRow,ctx,uniqueSequenceID);
        }
        return aggregate(scanSource, spliceRuntimeContext);
    }

    private ExecRow aggregate(ScalarAggregateSource source, SpliceRuntimeContext context) throws StandardException, IOException{
        ExecIndexRow row = source.nextRow(context);
        if(row == null){
            return finalizeResults();
        }
        do{
            if(!isTemp)
                initializeAggregation(row);

            if(keyEncoder==null){
                keyEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),row.nColumns());
                /*
                 * We can setRawBytes here, because we know how long the uniqueSequenceID is, which
                 * allows us to always safely skip the correct number of bytes past.
                 */
                keyEncoder.setRawBytes(uniqueSequenceID);
                keyEncoder.mark();
            }
            keyEncoder.reset();

            //noinspection RedundantCast
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
            row = source.nextRow(context);
        }while(row!=null);

        return finalizeResults();
    }

    private void initializeAggregation(ExecIndexRow row) throws StandardException {
        for(SpliceGenericAggregator aggregator:aggregates){
            aggregator.initialize(row);
            aggregator.accumulate(row,row);
        }
    }


    private ExecRow finalizeResults() throws StandardException {
        completedExecution=true;
        for(Map.Entry<ByteBuffer,ExecIndexRow> row:currentAggregations.entrySet()){
            finishedResults.add(Pair.<ByteBuffer, ExecRow>newPair(row.getKey(), finishAggregation(row.getValue())));
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
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        KeyMarshall keyType = new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix, MultiFieldEncoder keyEncoder) throws StandardException {
                byte[] key = BytesUtil.concatenate(emittedKey.array(),keyPostfix);
                /*
                 * the key is already encoded, so setRawBytes() is safe.
                 */
                keyEncoder.setRawBytes(key);
            }

            @Override
            public void decode(DataValueDescriptor[] data, int[] reversedKeyColumns, boolean[] sortOrder, MultiFieldDecoder rowDecoder) throws StandardException {
                //noinspection RedundantCast
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
        if(isTemp){
        }
    }


}
