package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.storage.*;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.MurmurHash;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

public class GroupedAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
	private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
	protected boolean isInSortedOrder;
	protected boolean isRollup;

    protected byte[] currentKey;

    private GroupedAggregator aggregator;
    private GroupedAggregateContext groupedAggregateContext;

    private Snowflake.Generator uuidGen;

    public GroupedAggregateOperation () {
    	super();
    	SpliceLogUtils.trace(LOG,"instantiate without parameters");
    }

    public GroupedAggregateOperation(
            SpliceOperation source,
            OperationInformation baseOpInformation,
            AggregateContext genericAggregateContext,
            GroupedAggregateContext groupedAggregateContext,
            boolean isInSortedOrder,
            boolean isRollup) throws StandardException {
        super(source,baseOpInformation,genericAggregateContext);
        this.isRollup = isRollup;
        this.isInSortedOrder = isInSortedOrder;
        this.groupedAggregateContext = groupedAggregateContext;
    }

    public GroupedAggregateOperation(
            SpliceOperation s,
			boolean isInSortedOrder,
			Activation a,
			int resultSetNumber,
		    double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isRollup,
            AggregateContext genericAggregateContext,
            GroupedAggregateContext groupedAggregateContext) throws StandardException  {
    	super(s,a,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost,genericAggregateContext);
    	this.isInSortedOrder = isInSortedOrder;
    	this.isRollup = isRollup;
        this.groupedAggregateContext = groupedAggregateContext;
    	recordConstructorTime();
    }

    public GroupedAggregateOperation(
            SpliceOperation s,
			boolean isInSortedOrder,
			int	aggregateItem,
			Activation a,
			GeneratedMethod ra,
			int maxRowSize,
			int resultSetNumber,
		    double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isRollup,
            GroupedAggregateContext groupedAggregateContext) throws StandardException  {
    	super(s, aggregateItem, a, ra, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
    	this.isInSortedOrder = isInSortedOrder;
    	this.isRollup = isRollup;
        this.groupedAggregateContext = groupedAggregateContext;
    	recordConstructorTime();
    }

    public GroupedAggregateOperation(SpliceOperation s,
			boolean isInSortedOrder,
			int	aggregateItem,
			int	orderingItem,
			Activation a,
			GeneratedMethod ra,
			int maxRowSize,
			int resultSetNumber,
		    double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			boolean isRollup) throws StandardException  {
        this(s,isInSortedOrder,aggregateItem,a,ra,maxRowSize,resultSetNumber,
                optimizerEstimatedRowCount,optimizerEstimatedCost,isRollup,new DerbyGroupedAggregateContext(orderingItem));
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		isRollup = in.readBoolean();
        groupedAggregateContext = (GroupedAggregateContext)in.readObject();
	}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeBoolean(isRollup);
        out.writeObject(groupedAggregateContext);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException{
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        source.init(context);
        groupedAggregateContext.init(context,aggregateContext);

    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if(failedTasks.size()>0){
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
        }
        if(top!=this && top instanceof SinkingOperation){
            SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
            return new DistributedClientScanProvider("groupedAggregateReduce",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
        }else{
            return RowProviders.openedSourceProvider(top,LOG,spliceRuntimeContext);
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        final RowProvider rowProvider = source.getMapRowProvider(this, getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()), spliceRuntimeContext);
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,spliceRuntimeContext);
        return rowProvider.shuffleRows(soi);
    }

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
        boolean[] groupingKeyOrder = groupedAggregateContext.getGroupingKeyOrder();
        return RowEncoder.create(sourceExecIndexRow.nColumns(), groupingKeys, groupingKeyOrder, null, new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns,
                                  int[] keyColumns,
                                  boolean[] sortOrder,
                                  byte[] keyPostfix,
                                  MultiFieldEncoder keyEncoder) throws StandardException {
                /*
                 * Build the actual row key from the currently set row key, along with a hash byte.
                 *
                 * The row key looks like
                 *
                 * <bucket byte> 0x00 <uniqueSequenceId> 0x00 <grouping keys> 0x00 <uniqueifier> 0x00 postfix
                 *
                 * where bucket byte is a murmur hash of the grouping keys.
                 *
                 * Since there are only 16 different buckets, we use only 4 bits from the murmur hash,
                 * chosen from the last byte in the int.
                 */
                byte hash = (byte)((byte) MurmurHash.murmur3_32(currentKey) & 0xf0);
                if(uuidGen==null)
                    uuidGen = operationInformation.getUUIDGenerator();
                byte[] key = BytesUtil.concatenate(hash, uniqueSequenceID, currentKey, uuidGen.nextBytes());
                /*
                 * The key is already encoded elsewhere, so it is safe to setRawBytes()
                 */
                keyEncoder.setRawBytes(key);
                //can set the postfix directly, because it has known length, and also will never be directly decoded
                keyEncoder.setRawBytes(keyPostfix);
            }

            @Override
            public void decode(DataValueDescriptor[] columns,
                               int[] reversedKeyColumns,
                               boolean[] sortOrder,
                               MultiFieldDecoder rowDecoder) throws StandardException {
                rowDecoder.seek(11); // seek past the hash and the unique identifier
                ((KeyMarshall)KeyType.BARE).decode(columns, reversedKeyColumns, sortOrder, rowDecoder);
            }

            @Override
            public int getFieldCount(int[] keyColumns) {
                return 2;
            }
        }, RowMarshaller.packed());
    }

    @Override
    public void cleanup() {

    }

    @Override
    public ExecRow getNextSinkRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(aggregator==null){
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return aggregateContext.getSortTemplateRow();
                }
            };
            AggregateBuffer buffer = new AggregateBuffer(SpliceConstants.ringBufferSize,
                    aggregateContext.getAggregators(),false,emptyRowSupplier,groupedAggregateContext,false);

            StandardIterator<ExecRow> sourceIterator = new StandardIterator<ExecRow>() {
                @Override public void open() throws StandardException, IOException { }
                @Override public void close() throws StandardException, IOException { }

                @Override
                public ExecRow next() throws StandardException, IOException {
                    return source.nextRow(spliceRuntimeContext);
                }
            };
            int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
            boolean[] groupingKeyOrder = groupedAggregateContext.getGroupingKeyOrder();
            aggregator = new GroupedAggregator(buffer,sourceIterator,
                    groupingKeys,groupingKeyOrder,isRollup);
        }

        GroupedRow row = aggregator.nextRow();
        if(row==null){
            currentKey=null;
            clearCurrentRow();
            return null;
        }

        currentKey = row.getGroupingKey();
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getNextSinkRow %s",row.getRow());
        ExecRow execRow = row.getRow();
        setCurrentRow(execRow);
        return execRow;
    }

    @Override
    public ExecRow nextRow(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if(aggregator==null){
            StandardSupplier<ExecRow> emptyRowSupplier = new StandardSupplier<ExecRow>() {
                @Override
                public ExecRow get() throws StandardException {
                    return aggregateContext.getSortTemplateRow();
                }
            };
            /*
             * When scanning from TEMP, we know that all the intermediate results with the same
             * hash key are grouped together, which means that we only need to keep a single buffer entry
             * in memory.
             */
            AggregateBuffer buffer = new AggregateBuffer(16, aggregates,true,emptyRowSupplier,groupedAggregateContext,true);

            int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
            boolean[] groupingKeyOrder = groupedAggregateContext.getGroupingKeyOrder();
            SpliceResultScanner scanner = getResultScanner(groupingKeys,spliceRuntimeContext);
            scanner.open();
            StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()));
            aggregator = new GroupedAggregator(buffer,sourceIterator,
                    groupingKeys,groupingKeyOrder,false);
        }
        GroupedRow row = aggregator.nextRow();
        if(row==null){
            clearCurrentRow();
            aggregator.close();
            return null;
        }
        currentKey = row.getGroupingKey();
        ExecRow execRow = row.getRow();
        setCurrentRow(execRow);
        return execRow;
    }

    private SpliceResultScanner getResultScanner(final int[] groupColumns,SpliceRuntimeContext spliceRuntimeContext) {
        if(!spliceRuntimeContext.isSink())
            return new ClientResultScanner(SpliceConstants.TEMP_TABLE_BYTES,reduceScan,true);

        //we are under another sink, so we need to use a RegionAwareScanner
        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
                fieldDecoder.seek(11); //skip the prefix value

                return DerbyBytesUtil.slice(fieldDecoder,groupColumns,cols);
            }

            @Override
            public byte[] getStopKey(Result result) {
                byte[] start = getStartKey(result);
                BytesUtil.unsignedIncrement(start,start.length-1);
                return start;
            }
        };
        return RegionAwareScanner.create(getTransactionID(),region,reduceScan,SpliceConstants.TEMP_TABLE_BYTES,boundary);
    }

    @Override
    public ExecRow getExecRowDefinition() {
        SpliceLogUtils.trace(LOG,"getExecRowDefinition");
        return sourceExecIndexRow.getClone();
    }

    @Override
    public String toString() {
        return "GroupedAggregateOperation {source="+source;
    }


    public boolean isInSortedOrder() {
        return this.isInSortedOrder;
    }

    public boolean hasDistinctAggregate() {
        return groupedAggregateContext.getNumDistinctAggregates()>0;
    }

//    @Override
//    public long getTimeSpent(int type)
//    {
//        long totTime = constructorTime + openTime + nextTime + closeTime;
//
//        if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
//            return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//        else
//            return totTime;
//    }
    @Override
    public void	close() throws StandardException, IOException {
//        if(hbs!=null)
//            hbs.close();
        SpliceLogUtils.trace(LOG, "close in GroupedAggregate");
        beginTime = getCurrentTimeMillis();
        if ( isOpen )
        {
            if(reduceScan!=null)
                SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
            // we don't want to keep around a pointer to the
            // row ... so it can be thrown away.
            // REVISIT: does this need to be in a finally
            // block, to ensure that it is executed?
            clearCurrentRow();
            source.close();

            super.close();
        }
        closeTime += getElapsedMillis(beginTime);

        isOpen = false;
    }

    public Properties getSortProperties() {
        Properties sortProperties = new Properties();
        sortProperties.setProperty("numRowsInput", ""+getRowsInput());
        sortProperties.setProperty("numRowsOutput", ""+getRowsOutput());
        return sortProperties;
    }

    @Override
    public String prettyPrint(int indentLevel) {
        return "Grouped"+super.prettyPrint(indentLevel);
    }

}
