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
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.hash.ByteHash32;
import com.splicemachine.utils.hash.HashFunctions;
import com.splicemachine.utils.hash.MurmurHash;
import com.splicemachine.utils.Snowflake;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Properties;

public class GroupedAggregateOperation extends GenericAggregateOperation {
    private static final long serialVersionUID = 1l;
	private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
    private static final byte[] nonDistinctOrdinal = {0x01};
    private static final byte[] distinctOrdinal = {0x00};
	protected boolean isInSortedOrder;
	protected boolean isRollup;

    protected byte[] currentKey;
    private boolean isCurrentDistinct;

    private StandardIterator<GroupedRow> aggregator;
    private GroupedAggregateContext groupedAggregateContext;

    private Snowflake.Generator uuidGen;
    private Scan baseScan;

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

    @SuppressWarnings("UnusedParameters")
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
        baseScan = context.getScan();
        groupedAggregateContext.init(context,aggregateContext);

    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        buildReduceScan();
        if(top!=this && top instanceof SinkingOperation){
            SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
            return new DistributedClientScanProvider("groupedAggregateReduce",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
        }else{
            return RowProviders.openedSourceProvider(top,LOG,spliceRuntimeContext);
        }
    }

    private void buildReduceScan() throws StandardException {
        if(reduceScan!=null) return;
        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if(failedTasks.size()>0){
            SuccessFilter filter = new SuccessFilter(failedTasks);
            reduceScan.setFilter(filter);
        }
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(aggregator!=null){
            aggregator.close();
            aggregator = null;
        }
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext ) throws StandardException {
        long start = System.currentTimeMillis();
        final RowProvider rowProvider = source.getMapRowProvider(this, OperationUtils.getPairDecoder(this,runtimeContext), runtimeContext);
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,runtimeContext);
        return rowProvider.shuffleRows(soi);
    }

		@Override
		public KeyEncoder getKeyEncoder(final SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				/*
				 * We use the cached currentKey, with some extra fun bits to determine the hash
				 * prefix.
				 *
				 * Our currentKey is also the grouping key, which makes life easy. However,
				 * our Postfix is different depending on whether or not the row is "distinct".
				 *
				 * All rows look like:
				 *
				 * <hash> <uniqueSequenceId> <grouping key> 0x00 <postfix>
				 *
				 * if the row is distinct, then <postfix> = 0x00 which indicates a distinct
 				 * row. This allows multiple distinct rows to be written into the same
 				 * location on TEMP, which will eliminate duplicates.
 				 *
 				 * If the row is NOT distinct, then <postfix> = <uuid> <taskId>
 				 * The row looks this way so that the SuccessFilter can be applied
 				 * correctly to strip out rows from failed tasks. Be careful when considering
 				 * the row as a whole to recognize that this row has a 16-byte postfix
				 */
				HashPrefix prefix = getHashPrefix();

				DataHash dataHash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								return currentKey;
						}
				}){
						@Override
						public KeyHashDecoder getDecoder() {
								return BareKeyHash.decoder(groupedAggregateContext.getGroupingKeys(),
												groupedAggregateContext.getGroupingKeyOrder());
						}
				};

				final KeyPostfix uniquePostfix = new UniquePostfix(spliceRuntimeContext.getCurrentTaskId(),operationInformation.getUUIDGenerator());
				KeyPostfix postfix = new KeyPostfix() {
						@Override
						public int getPostfixLength(byte[] hashBytes) throws StandardException {
								if(isCurrentDistinct) return distinctOrdinal.length;
								else
										return uniquePostfix.getPostfixLength(hashBytes);
						}

						@Override
						public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) {
								if(isCurrentDistinct){
										System.arraycopy(distinctOrdinal,0,keyBytes,postfixPosition,distinctOrdinal.length);
								}else{
										uniquePostfix.encodeInto(keyBytes,postfixPosition,hashBytes);
								}
						}
				};
				return new KeyEncoder(prefix,dataHash,postfix);
		}

		private HashPrefix getHashPrefix() {
				return new AggregateBucketingPrefix(new FixedPrefix(uniqueSequenceID),
										HashFunctions.murmur3(0),
										SpliceDriver.driver().getTempTable().getCurrentSpread());
		}

		private class AggregateBucketingPrefix extends BucketingPrefix{
				private MultiFieldDecoder decoder;
				private final int[] groupingKeys = groupedAggregateContext.getGroupingKeys();
				private final DataValueDescriptor[] fields = sortTemplateRow.getRowArray();

				public AggregateBucketingPrefix(HashPrefix delegate, ByteHash32 hashFunction, SpreadBucket spreadBucket) {
						super(delegate, hashFunction, spreadBucket);
				}

				@Override
				protected byte bucket(byte[] hashBytes) {
						if(!isCurrentDistinct)
								return super.bucket(hashBytes);

						/*
						 * If the row is distinct, then the grouping key is a combination of <actual grouping keys> <distinct column>
						 * So to get the proper bucket, we need to hash only the grouping keys, so we have to first strip out
						 * the excess grouping keys
						 */
						if(decoder==null)
								decoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());

						decoder.set(hashBytes);
						int offset = decoder.offset();
						int length = DerbyBytesUtil.skip(decoder, groupingKeys, fields);

						if(offset+length>hashBytes.length)
							length = hashBytes.length-offset;
						return spreadBucket.bucket(hashFunction.hash(hashBytes,offset,length));
				}
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				/*
				 * Only encode the fields which aren't grouped into the row.
				 */
				ExecRow defn = getExecRowDefinition();
				int[] nonGroupedFields = IntArrays.complement(groupedAggregateContext.getGroupingKeys(),defn.nColumns());
				return BareKeyHash.encoder(nonGroupedFields,null);
		}

		@Override
		public CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException {
				return bufferToTransform;
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
            int[] nonGroupedUniqueColumns = groupedAggregateContext.getNonGroupedUniqueColumns();
            AggregateBuffer distinctBuffer = new AggregateBuffer(SpliceConstants.ringBufferSize,
                    aggregateContext.getDistinctAggregators(),false,emptyRowSupplier,groupedAggregateContext,false);
            AggregateBuffer nonDistinctBuffer = new AggregateBuffer(SpliceConstants.ringBufferSize,
                    aggregateContext.getNonDistinctAggregators(),false,emptyRowSupplier,groupedAggregateContext,false);

            aggregator = new SinkGroupedAggregator(nonDistinctBuffer,distinctBuffer,sourceIterator,isRollup,
                    groupingKeys,groupingKeyOrder,nonGroupedUniqueColumns);
            aggregator.open();
        }

        GroupedRow row = aggregator.next();
        if(row==null){
            currentKey=null;
            clearCurrentRow();
            aggregator.close();
            return null;
        }

        currentKey = row.getGroupingKey();
        isCurrentDistinct = row.isDistinct();
        ExecRow execRow = row.getRow();
//        if (LOG.isTraceEnabled())
//            SpliceLogUtils.trace(LOG, "getNextSinkRow %s",execRow);
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
            SpliceResultScanner scanner = getResultScanner(groupingKeys,spliceRuntimeContext,getHashPrefix().getPrefixLength());
            StandardIterator<ExecRow> sourceIterator = new ScanIterator(scanner,OperationUtils.getPairDecoder(this,spliceRuntimeContext));
            aggregator = new ScanGroupedAggregator(buffer,sourceIterator,
                    groupingKeys,groupingKeyOrder,false);
            aggregator.open();
        }
        boolean shouldClose = true;
        try{
            GroupedRow row = aggregator.next();
            if(row==null){
                clearCurrentRow();
                return null;
            }
            //don't close the aggregator unless you have no more data
            shouldClose =false;
            currentKey = row.getGroupingKey();
            isCurrentDistinct = row.isDistinct();
            ExecRow execRow = row.getRow();
            setCurrentRow(execRow);

//            if(LOG.isTraceEnabled())
//                LOG.trace("nextRow = "+ execRow);
            return execRow;
        }finally{
            if(shouldClose)
                aggregator.close();
        }
    }

    private SpliceResultScanner getResultScanner(final int[] groupColumns,SpliceRuntimeContext spliceRuntimeContext, final int prefixOffset) {
        if(!spliceRuntimeContext.isSink()){
						byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
            return new ClientResultScanner(tempTableBytes,reduceScan,true);
				}

        //we are under another sink, so we need to use a RegionAwareScanner
        final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
        ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
            @Override
            public byte[] getStartKey(Result result) {
                MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
                fieldDecoder.seek(prefixOffset+1); //skip the prefix value

                byte[] slice = DerbyBytesUtil.slice(fieldDecoder, groupColumns, cols);
                fieldDecoder.reset();
                MultiFieldEncoder encoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),2);
                encoder.setRawBytes(fieldDecoder.slice(prefixOffset+1));
                encoder.setRawBytes(slice);
                return encoder.build();
            }

            @Override
            public byte[] getStopKey(Result result) {
                byte[] start = getStartKey(result);
                BytesUtil.unsignedIncrement(start,start.length-1);
                return start;
            }
        };
        return RegionAwareScanner.create(getTransactionID(),region,baseScan,SpliceConstants.TEMP_TABLE_BYTES,boundary);
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
				super.close();
				source.close();
//        SpliceLogUtils.trace(LOG, "close in GroupedAggregate");
//        beginTime = getCurrentTimeMillis();
//        if ( isOpen )
//        {
//            if(reduceScan!=null)
//                SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
//            // we don't want to keep around a pointer to the
//            // row ... so it can be thrown away.
//            // REVISIT: does this need to be in a finally
//            // block, to ensure that it is executed?
//            clearCurrentRow();
//            source.close();
//
//            super.close();
//        }
//        closeTime += getElapsedMillis(beginTime);
//
//        isOpen = false;
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
