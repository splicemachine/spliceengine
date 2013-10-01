package com.splicemachine.derby.impl.sql.execute.operations;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.datanucleus.sco.backed.Map;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.storage.BaseHashAwareScanBoundary;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.SimpleRegionAwareRowProvider;
import com.splicemachine.derby.stats.Accumulator;
import com.splicemachine.derby.stats.TimingStats;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.HashUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyMarshall;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;

public class GroupedAggregateOperation extends GenericAggregateOperation {
	private static Logger LOG = Logger.getLogger(GroupedAggregateOperation.class);
	protected boolean isInSortedOrder;
	protected boolean isRollup;
	protected int orderingItem;
	protected List<Integer> keyColumns;
	protected List<Integer> groupByColumns;
	protected List<Integer> nonGroupByUniqueColumns;
	protected List<Boolean> groupByDescAscInfo;	
	protected List<Boolean> descAscInfo;
	protected List<Integer> allKeyColumns;
	HashMap<Integer,List<DataValueDescriptor>> distinctValues;
	private int numDistinctAggs = 0;
	protected ColumnOrdering[] order;
	private HashBuffer<ByteBuffer,ExecRow> currentAggregations = new DelegateHashBuffer<ByteBuffer,ExecRow>(SpliceConstants.ringBufferSize);
	private ExecRow[] resultRows;
	private boolean completedExecution = false;
    protected KeyMarshall hasher;
    protected byte[] currentKey;
    protected MultiFieldEncoder sinkEncoder;
    protected RowProvider rowProvider;
    private Accumulator scanAccumulator = TimingStats.uniformAccumulator();

    private HashBufferSource hbs;

    private boolean isTemp;

    public GroupedAggregateOperation () {
    	super();
    	SpliceLogUtils.trace(LOG,"instantiate without parameters");
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
    	super(s,aggregateItem,a,ra,resultSetNumber,optimizerEstimatedRowCount,optimizerEstimatedCost);
    	SpliceLogUtils.trace(LOG, "instantiate with isInSortedOrder %s, aggregateItem %d, orderingItem %d, isRollup %s",isInSortedOrder,aggregateItem,orderingItem,isRollup);
    	this.isInSortedOrder = isInSortedOrder;
    	this.isRollup = isRollup;
    	this.orderingItem = orderingItem;
    	recordConstructorTime();
    }
    
	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		isInSortedOrder = in.readBoolean();
		isRollup = in.readBoolean();
		orderingItem = in.readInt();
	}

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeBoolean(isInSortedOrder);
        out.writeBoolean(isRollup);
        out.writeInt(orderingItem);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException{
        SpliceLogUtils.trace(LOG, "init called");
        context.setCacheBlocks(false);
        super.init(context);
        ((SpliceOperation)source).init(context);
        GenericStorablePreparedStatement statement = context.getPreparedStatement();
        order = (ColumnOrdering[])
                ((FormatableArrayHolder) (statement.getSavedObject(orderingItem))).getArray(ColumnOrdering.class);        
        keyColumns = new ArrayList<Integer>();
        nonGroupByUniqueColumns = new ArrayList<Integer>();
        groupByColumns = new ArrayList<Integer>();
        descAscInfo = new ArrayList<Boolean>();
        groupByDescAscInfo = new ArrayList<Boolean>();
        final int[] keyCols = new int[order.length];
        for (int index = 0; index < order.length; index++) {
            keyColumns.add(order[index].getColumnId());
            descAscInfo.add(order[index].getIsAscending());
            keyCols[index] = order[index].getColumnId();
        }
        
        for(SpliceGenericAggregator agg: aggregates){
            if(agg.isDistinct()) {
            	if (!keyColumns.contains(agg.getAggregatorInfo().getInputColNum()))
            		nonGroupByUniqueColumns.add(agg.getAggregatorInfo().getInputColNum());
            	numDistinctAggs++;
            }
        }
        // Create the Distinct Values Map
    	distinctValues = new HashMap<Integer,List<DataValueDescriptor>>();
        // Make sure the lists are clear, who is unique and who is group by
        if (numDistinctAggs > 0) {
        	groupByColumns.addAll(keyColumns.subList(0, keyColumns.size()-1));
        	nonGroupByUniqueColumns.add(nonGroupByUniqueColumns.size(),keyColumns.get(keyColumns.size()-1));
        	groupByDescAscInfo.addAll(descAscInfo.subList(0, descAscInfo.size()-1));
        	for (Integer unique: nonGroupByUniqueColumns) {
        		groupByDescAscInfo.add(true);
        	}
        } else {
        	groupByColumns.addAll(keyColumns);
        	groupByDescAscInfo.addAll(descAscInfo);        	
        }
        
        
        sinkEncoder = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),groupByColumns.size() + nonGroupByUniqueColumns.size()+1);
        sinkEncoder.setRawBytes(uniqueSequenceID).mark();
    	allKeyColumns = new ArrayList<Integer>(groupByColumns);
    	allKeyColumns.addAll(nonGroupByUniqueColumns);
        if(regionScanner==null){
            isTemp = true;
        } else {
            isTemp = !context.isOpSinking(this);
            if(isTemp){
                RowEncoder scanEncoder = RowEncoder.create(sourceExecIndexRow.nColumns(),convertIntegers(allKeyColumns),convertBooleans(groupByDescAscInfo),
                        sinkEncoder.getEncodedBytes(0),
                        new KeyMarshall() {
                            @Override
                            public int getFieldCount(int[] keyColumns) {
                                return ((KeyMarshall)KeyType.BARE).getFieldCount(keyColumns) + 1;
                            }
                            
                            @Override
                            public void encodeKey(DataValueDescriptor[] columns, int[] keyColumns, boolean[] sortOrder, byte[] keyPostfix,
                                    MultiFieldEncoder keyEncoder) throws StandardException {
                                ((KeyMarshall)KeyType.BARE).encodeKey(columns, keyColumns, sortOrder, keyPostfix, keyEncoder);
                            }
                            
                            @Override
                            public void decode(DataValueDescriptor[] data, int[] reversedKeyColumns, boolean[] sortOrder,
                                    MultiFieldDecoder rowDecoder) throws StandardException {
                                rowDecoder.seek(11);
                                ((KeyMarshall)KeyType.BARE).decode(data, reversedKeyColumns, sortOrder, rowDecoder);
                            }
                        }, RowMarshaller.packed());

                //build a ScanBoundary based off the type of the entries
                final DataValueDescriptor[] cols = sourceExecIndexRow.getRowArray();
                ScanBoundary boundary = new BaseHashAwareScanBoundary(SpliceConstants.DEFAULT_FAMILY_BYTES){
                    @Override
                    public byte[] getStartKey(Result result) {
                        MultiFieldDecoder fieldDecoder = MultiFieldDecoder.wrap(result.getRow(),SpliceDriver.getKryoPool());
                        fieldDecoder.seek(11); //skip the prefix value

                        return DerbyBytesUtil.slice(fieldDecoder,keyCols,cols);
                    }

                    @Override
                    public byte[] getStopKey(Result result) {
                        byte[] start = getStartKey(result);
                        BytesUtil.unsignedIncrement(start,start.length-1);
                        return start;
                    }
                };
                SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
                rowProvider = new SimpleRegionAwareRowProvider(
                        "groupedAggregateRowProvider",
                        SpliceUtils.NA_TRANSACTION_ID,
                        context.getRegion(),
                        context.getScan(),
                        SpliceConstants.TEMP_TABLE_BYTES,
                        SpliceConstants.DEFAULT_FAMILY_BYTES,
                        scanEncoder.getDual(sourceExecIndexRow),
                        boundary,
                        spliceRuntimeContext); // Make sure the partitioner (Region Aware) worries about group by keys, not the additonal unique keys
                rowProvider.open();
            }
        }
        hasher = KeyType.BARE;
    }

    private void createHashBufferSource(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        MultiFieldEncoder mfe = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),groupByColumns.size() + nonGroupByUniqueColumns.size()+2);
        boolean[] groupByDescAscArray = convertBooleans(groupByDescAscInfo);
        int[] keyColumnArray = convertIntegers(allKeyColumns);
        RowProviderIterator<ExecRow> sourceProvider = new SourceIterator(spliceRuntimeContext);
        hbs = new HashBufferSource(uniqueSequenceID, keyColumnArray, convertIntegers(groupByColumns), sourceProvider, merger, KeyType.BARE, mfe, groupByDescAscArray, aggregateFinisher, true);
    }
    
    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        SuccessFilter filter = new SuccessFilter(failedTasks);
        reduceScan.setFilter(filter);
        SpliceUtils.setInstructions(reduceScan, activation, top, spliceRuntimeContext);
        return new DistributedClientScanProvider("groupedAggregateReduce",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top,decoder,spliceRuntimeContext);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        final RowProvider rowProvider = ((SpliceOperation)source).getMapRowProvider(this, getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()),spliceRuntimeContext);
        nextTime+= System.currentTimeMillis()-start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(),this,spliceRuntimeContext);
        return rowProvider.shuffleRows(soi);
    }

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return RowEncoder.create(sourceExecIndexRow.nColumns(), convertIntegers(allKeyColumns),convertBooleans(groupByDescAscInfo), null, new KeyMarshall() {
            @Override
            public void encodeKey(DataValueDescriptor[] columns,
                                  int[] keyColumns,
                                  boolean[] sortOrder,
                                  byte[] keyPostfix,
                                  MultiFieldEncoder keyEncoder) throws StandardException {
//                byte[] hash = new byte [] { HashUtils.hash(new byte[][] { currentKey }), (byte) 0 };
                //TODO -sf- this might break DistinctGroupedAggregations
                byte[] key = BytesUtil.concatenate(currentKey, SpliceUtils.getUniqueKey());
                keyEncoder.setRawBytes(key);
                keyEncoder.setRawBytes(keyPostfix);
            }

            @Override
            public void decode(DataValueDescriptor[] columns,
                               int[] reversedKeyColumns,
                               boolean[] sortOrder,
                               MultiFieldDecoder rowDecoder) throws StandardException {
                rowDecoder.seek(2); // seek past the hash
                hasher.decode(columns, reversedKeyColumns, sortOrder, rowDecoder);
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
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        ExecRow row = doSinkAggregation(spliceRuntimeContext);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "getNextSinkRow %s",row);
        return row;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        ExecRow row = doScanAggregation(spliceRuntimeContext);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "nextRow %s",row);
        return row;
    }

    private final HashMerger merger = new HashMerger<ByteBuffer,ExecRow>() {
        @Override
        public ExecRow shouldMerge(HashBuffer<ByteBuffer, ExecRow> hashBuffer, ByteBuffer key){
            return hashBuffer.get(key);
        }

        @Override
        public void merge(HashBuffer<ByteBuffer, ExecRow> hashBuffer, ExecRow curr,ExecRow next){
            try {
                mergeVectorAggregates(next,curr);
            } catch (StandardException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }};

    private ExecRow doSinkAggregation(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {

        if(resultRows==null){
            resultRows = isRollup?new ExecRow[groupByColumns.size()+1]:new ExecRow[1]; // Need to fix Group By Columns
        }
        if (hbs == null)
        	createHashBufferSource(spliceRuntimeContext);

        Pair<ByteBuffer,ExecRow> nextRow = hbs.getNextAggregatedRow();
        ExecRow rowResult = null;
        if(nextRow != null){
            makeCurrent(nextRow.getFirst(),nextRow.getSecond());
            rowResult = nextRow.getSecond();
        }else{
            SpliceLogUtils.trace(LOG, "finalizeResults");
            completedExecution=true;
        }

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"next aggregated row = %s",nextRow);
        return rowResult;
    }

    private ExecRow doScanAggregation(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        if (completedExecution) {
            if (currentAggregations.size()>0) {
                ByteBuffer key = currentAggregations.keySet().iterator().next();
                return makeCurrent(key,currentAggregations.remove(key));
            } else
                return null; // Done
        }
        long start = System.nanoTime();
        if(resultRows==null)
            resultRows = new ExecRow[1];
        ExecRow nextRow = getNextRowFromScan();
        if(nextRow ==null)
            return finalizeResults();
        //TODO -sf- stash these away somewhere so we're not constantly autoboxing
        int[] groupByCols = convertIntegers(groupByColumns);

        long rowsScanned = 0l;

        do{
            resultRows[0] = nextRow;
            ExecRow[] rolledUpRows = resultRows;
            for(ExecRow rolledUpRow:rolledUpRows) {
                sinkEncoder.reset();
                ((KeyMarshall)hasher).encodeKey(rolledUpRow.getRowArray(), groupByCols, null, null, sinkEncoder);
                ByteBuffer keyBuffer = ByteBuffer.wrap(sinkEncoder.build());
                if(!currentAggregations.merge(keyBuffer, rolledUpRow, merger)){
                    ExecRow row = rolledUpRow.getClone();
                    refreshDistinctValues(row);
                    Map.Entry<ByteBuffer,ExecRow> finalized = currentAggregations.add(keyBuffer,row);
                    if(finalized!=null&&finalized !=row){
                        return makeCurrent(finalized.getKey(),finishAggregation(finalized.getValue()));
                    }
                }
            }
            nextRow = getNextRowFromScan();

            if(scanAccumulator.shouldCollectStats()){
                scanAccumulator.tick(System.nanoTime()-start);
                start = System.nanoTime();
            }else{
                rowsScanned++;
            }

        } while (nextRow!=null);

        if( !scanAccumulator.shouldCollectStats() ){
            scanAccumulator.tickRecords(rowsScanned);
        }

        ExecRow next = finalizeResults();
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"next aggregated row = %s",next);
        return next;
    }

    private void refreshDistinctValues(ExecRow row) throws StandardException {
        distinctValues.clear();
        for (int i = 0; i < aggregates.length; i++) {
            SpliceGenericAggregator agg = aggregates[i];
            if(agg.isDistinct()) {
                DataValueDescriptor value = agg.getInputColumnValue(row);
                List<DataValueDescriptor> values;
                values = new ArrayList<DataValueDescriptor>();
                values.add(value);
                distinctValues.put(i, values);
            }
        }

    }

    private ExecRow[] getRolledUpRows(ExecRow rowToRollUp) throws StandardException {
        if(!isRollup){
            resultRows[0] = rowToRollUp;
            return resultRows;
        }
        int rollUpPos = groupByColumns.size();
        int pos = 0;
        ExecRow nextRow = rowToRollUp.getClone();
        SpliceLogUtils.trace(LOG,"setting rollup cols to null");
        do{
            SpliceLogUtils.trace(LOG,"adding row %s",nextRow);
            resultRows[pos] = nextRow;

            //strip out the next key in the rollup
            if(rollUpPos>0){
                nextRow = nextRow.getClone();
                DataValueDescriptor rollUpCol = nextRow.getColumn(order[rollUpPos-1].getColumnId()+1);
                rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        }while(rollUpPos>=0);

        return resultRows;
    }

    protected void initializeVectorAggregation(ExecRow row) throws StandardException{
        for(SpliceGenericAggregator aggregator: aggregates){
            aggregator.initialize(row);
            aggregator.accumulate(row, row);
        }
    }

    private void mergeVectorAggregates(ExecRow newRow, ExecRow currRow) throws StandardException {
        for (int i=0; i< aggregates.length; i++) {
            SpliceGenericAggregator agg = aggregates[i];
            DataValueDescriptor value = agg.getInputColumnValue(newRow).cloneValue(false);
            if(agg.isDistinct()) {
                if (!isTemp)
                    continue;
                else {
                    List<DataValueDescriptor> values;
                    if (distinctValues.containsKey(i)) {
                        values = distinctValues.get(i);
                        if (values.contains(value)) {
                            continue; // Already there, skip...
                        }
                        values.add(value);
                        distinctValues.put(i, values);
                    } else {
                        values = new ArrayList<DataValueDescriptor>();
                        values.add(value);
                        distinctValues.put(i, values);
                    }
                }
            }
            agg.merge(newRow,currRow);
        }
    }

    protected ExecRow getNextRowFromScan() throws StandardException, IOException {
        SpliceLogUtils.trace(LOG,"getting next row from scan");
        if(rowProvider.hasNext())
            return rowProvider.next();
        else return null;
    }

    private ExecRow getNextRowFromSource(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        ExecRow sourceRow;
        ExecRow inputRow = null;

        if ((sourceRow = source.nextRow(spliceRuntimeContext))!=null){
            sourceExecIndexRow.execRowToExecIndexRow(sourceRow);
            inputRow = sourceExecIndexRow;
        }

        return inputRow;
    }

    private ExecRow finalizeResults() throws StandardException {
        SpliceLogUtils.trace(LOG, "finalizeResults");
        completedExecution=true;
        currentAggregations = currentAggregations.finishAggregates(aggregateFinisher);
        if(currentAggregations.size()>0) {
            ByteBuffer key = currentAggregations.keySet().iterator().next();
            return makeCurrent(key,currentAggregations.remove(key));
        }
        else
            return null;
    }

    private ExecRow makeCurrent(ByteBuffer key, ExecRow row) throws StandardException{
        setCurrentRow(row);
        currentKey = key.array();
        return row;
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
        return this.numDistinctAggs>0;
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
        if(hbs!=null)
            hbs.close();
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
    public static int[] convertIntegers(List<Integer> integers) {
        int[] ret = new int[integers.size()];
        for (int i=0; i < ret.length; i++) {
            ret[i] = integers.get(i).intValue();
        }
        return ret;
    }
    public static boolean[] convertBooleans(List<Boolean> booleans) {
        boolean[] ret = new boolean[booleans.size()];
        for (int i=0; i < ret.length; i++) {
            ret[i] = booleans.get(i).booleanValue();
        }
        return ret;
    }

    public class SourceIterator implements RowProviderIterator<ExecRow> {
    	protected SpliceRuntimeContext spliceRuntimeContext;
    	public SourceIterator(SpliceRuntimeContext spliceRuntimeContext) {
    		this.spliceRuntimeContext = spliceRuntimeContext;
    	}
    	   private Iterator<ExecRow> rolledUpRows = Collections.<ExecRow>emptyList().iterator();
           private boolean populated;

           @Override
           public boolean hasNext() throws StandardException, IOException {

               if(!populated && rolledUpRows != null && !rolledUpRows.hasNext()){
                   ExecRow nextRow = getNextRowFromSource(spliceRuntimeContext);

                   if(nextRow != null){
                       rolledUpRows = Arrays.asList(getRolledUpRows(nextRow)).iterator();
                       populated = true;
                   }else{
                       rolledUpRows = null;
                       populated = true;
                   }
               }

               return rolledUpRows != null && rolledUpRows.hasNext();
           }

           @Override
           public ExecRow next() throws StandardException, IOException {

               if(!populated){
                   hasNext();
               }

               ExecRow nextRow = null;

               if( rolledUpRows != null){
                   nextRow = rolledUpRows.next();
                   populated = false;
                   initializeVectorAggregation(nextRow);
               }

               return nextRow;
           }    	
    }

}
