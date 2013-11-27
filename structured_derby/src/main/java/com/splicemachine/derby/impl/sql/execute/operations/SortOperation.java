
package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.base.Strings;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.ColumnOrdering;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class SortOperation extends SpliceBaseOperation implements SinkingOperation {
    private static final long serialVersionUID = 2l;
    private static Logger LOG = Logger.getLogger(SortOperation.class);
    private static final List<NodeType> nodeTypes;

    private static final HashMerger merger = new DistinctMerger();

    protected SpliceOperation source;
    protected boolean distinct;
    protected int orderingItem;
    protected int[] keyColumns;
    protected boolean[] descColumns; //descColumns[i] = false => column[i] sorted descending, else sorted ascending
    private ExecRow sortResult;
    private int numColumns;
    private Scan reduceScan;
    private ExecRow execRowDefinition = null;
    private Properties sortProperties = new Properties();
    private HashBufferSource hbs;

    static {
        nodeTypes = Arrays.asList(NodeType.REDUCE, NodeType.SCAN);
    }

    private RowDecoder rowDecoder;

    /*
     * Used for serialization. DO NOT USE
     */
    @Deprecated
    public SortOperation() {
//		SpliceLogUtils.trace(LOG, "instantiated without parameters");
    }

    public SortOperation(SpliceOperation s,
                         boolean distinct,
                         int orderingItem,
                         int numColumns,
                         Activation a,
                         GeneratedMethod ra,
                         int resultSetNumber,
                         double optimizerEstimatedRowCount,
                         double optimizerEstimatedCost) throws StandardException {
        super(a, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
        this.source = s;
        this.distinct = distinct;
        this.orderingItem = orderingItem;
        this.numColumns = numColumns;
        init(SpliceOperationContext.newContext(a));
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException,
            ClassNotFoundException {
        super.readExternal(in);
        source = (SpliceOperation) in.readObject();
        distinct = in.readBoolean();
        orderingItem = in.readInt();
        numColumns = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        SpliceLogUtils.trace(LOG, "writeExternal");
        super.writeExternal(out);
        out.writeObject(source);
        out.writeBoolean(distinct);
        out.writeInt(orderingItem);
        out.writeInt(numColumns);
    }

    @Override
    public List<NodeType> getNodeTypes() {
        return nodeTypes;
    }

    @Override
    public List<SpliceOperation> getSubOperations() {
        SpliceLogUtils.trace(LOG, "getSubOperations");
        List<SpliceOperation> ops = new ArrayList<SpliceOperation>();
        ops.add((SpliceOperation) source);
        return ops;
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException {
    	hbs = null;
        SpliceLogUtils.trace(LOG, "init");
        super.init(context);
        ((SpliceOperation) source).init(context);

        FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(orderingItem);
        if (fah == null) {
            LOG.error("Unable to find column ordering for sorting!");
            throw new RuntimeException("Unable to find Column ordering for sorting!");
        }
        ColumnOrdering[] order = (ColumnOrdering[]) fah.getArray(ColumnOrdering.class);

        keyColumns = new int[order.length];
        descColumns = new boolean[order.length];

        for (int i = 0; i < order.length; i++) {
            keyColumns[i] = order[i].getColumnId();
            descColumns[i] = order[i].getIsAscending();
        }
//        hbs = new HashBufferSource(uniqueSequenceID, columns, wrapOperationWithProviderIterator(source), merger, KeyType.FIXED_PREFIX, MultiFieldEncoder.create(SpliceDriver.getKryoPool(),columns.length + 1));
        SpliceLogUtils.trace(LOG, "keyColumns %s, distinct %s", Arrays.toString(keyColumns), distinct);
    }
    
    private void createHashBufferSource(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        int columns[] = new int[getExecRowDefinition().nColumns()];
        for (int i = 0; i < columns.length; ++i) {
            columns[i] = i;
        }
    	hbs = new HashBufferSource(uniqueSequenceID, columns, new WrapOperationWithProviderIterator(source,spliceRuntimeContext), merger, KeyType.FIXED_PREFIX, MultiFieldEncoder.create(SpliceDriver.getKryoPool(),keyColumns.length + 1));
    }

    @Override
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {    	
        ExecRow nextRow = null;

        if(!distinct){
            nextRow = source.nextRow(spliceRuntimeContext);
        }else{ 
        	if (hbs == null) {
        		createHashBufferSource(spliceRuntimeContext);
        	}
            Pair<ByteBuffer,ExecRow> result = hbs.getNextAggregatedRow();
            if(result != null){
                nextRow = result.getSecond();
            }
        }
        setCurrentRow(nextRow);

        return nextRow;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "nextRow");
        sortResult = getNextRowFromScan(spliceRuntimeContext);
        if (sortResult != null)
            setCurrentRow(sortResult);
        return sortResult;
    }

    private ExecRow getNextRowFromScan(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        regionScanner.next(keyValues);
        if(keyValues.isEmpty()) return null;

        if(rowDecoder==null)
            rowDecoder = getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition(),true);
        return rowDecoder.decode(keyValues);
    }

    @Override
    public SpliceOperation getLeftOperation() {
//		SpliceLogUtils.trace(LOG,"getLeftOperation");
        return (SpliceOperation) this.source;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException {
//		SpliceLogUtils.trace(LOG, "getExecRowDefinition");
        if (execRowDefinition == null){
            execRowDefinition = ((SpliceOperation) source).getExecRowDefinition();
        }
        return execRowDefinition;
    }

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return ((SpliceOperation) source).getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return ((SpliceOperation) source).isReferencingTable(tableNumber);
    }

    @Override
	public RowProvider getReduceRowProvider(SpliceOperation top,RowDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        try {
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID, SpliceUtils.NA_TRANSACTION_ID);
            if (failedTasks.size() > 0 && !distinct) {
                //we don't need the filter when distinct is true, because we'll overwrite duplicates anyway
                reduceScan.setFilter(new SuccessFilter(failedTasks));
            }
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
        if(top!=this)
            SpliceUtils.setInstructions(reduceScan,getActivation(),top,spliceRuntimeContext);
		return new ClientScanProvider("sort",SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
	}


	@Override
	public NoPutResultSet executeScan() throws StandardException {
		SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        RowProvider provider = getReduceRowProvider(this,getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()),spliceRuntimeContext);
		SpliceNoPutResultSet rs =  new SpliceNoPutResultSet(activation,this,provider);
		nextTime += getCurrentTimeMillis() - beginTime;
		return rs;
	}

    @Override
    public RowEncoder getRowEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        ExecRow def = getExecRowDefinition();
        KeyType keyType = distinct? KeyType.FIXED_PREFIX: KeyType.FIXED_PREFIX_UNIQUE_POSTFIX;
        return RowEncoder.create(def.nColumns(), keyColumns,
                descColumns,
                uniqueSequenceID,
                keyType, RowMarshaller.packed());
    }

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder rowDecoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getReduceRowProvider(top, rowDecoder, spliceRuntimeContext);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        long start = System.currentTimeMillis();
        SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
        final RowProvider rowProvider = ((SpliceOperation)source).getMapRowProvider(this, getRowEncoder(spliceRuntimeContext).getDual(getExecRowDefinition()),spliceRuntimeContext);
        nextTime += System.currentTimeMillis() - start;
        SpliceObserverInstructions soi = SpliceObserverInstructions.create(getActivation(), this, spliceRuntimeContext);
        return rowProvider.shuffleRows(soi);
    }

    @Override
    public String toString() {
        return "SortOperation {resultSetNumber=" + resultSetNumber + ",source=" + source + "}";
    }

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(source!=null)source.open();
    }

    public SpliceOperation getSource() {
        return this.source;
    }

    public boolean needsDistinct() {
        return this.distinct;
    }

    @Override
    public void close() throws StandardException, IOException {
        SpliceLogUtils.trace(LOG, "close in Sort");
        beginTime = getCurrentTimeMillis();
        if (isOpen) {
            if(reduceScan!=null)
                SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());
            clearCurrentRow();

            sortResult = null;
            source.close();

            super.close();
        }

        closeTime += getElapsedMillis(beginTime);

        isOpen = false;
    }

//    @Override
//    public long getTimeSpent(int type) {
//        long totTime = constructorTime + openTime + nextTime + closeTime;
//
//        if (type == NoPutResultSet.CURRENT_RESULTSET_ONLY)
//            return totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
//        else
//            return totTime;
//    }

    public Properties getSortProperties() {
        if (sortProperties == null)
            sortProperties = new Properties();

        sortProperties.setProperty("numRowsInput", "" + getRowsInput());
        sortProperties.setProperty("numRowsOutput", "" + getRowsOutput());
        return sortProperties;
    }

    public long getRowsInput() {
        return getRegionStats() == null ? 0l : getRegionStats().getTotalProcessedRecords();
    }

    public long getRowsOutput() {
        return getRegionStats() == null ? 0l : getRegionStats().getTotalSunkRecords();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n" + Strings.repeat("\t", indentLevel);

        return new StringBuilder("Sort:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("distinct:").append(distinct)
                .append(indent).append("orderingItem:").append(orderingItem)
                .append(indent).append("keyColumns:").append(Arrays.toString(keyColumns))
                .append(indent).append("source:").append(((SpliceOperation) source).prettyPrint(indentLevel + 1))
                .toString();
    }
    
    public class WrapOperationWithProviderIterator implements RowProviderIterator<ExecRow> {
        private ExecRow nextRow;
        private boolean populated = false;
        protected SpliceOperation operationSource;
        protected SpliceRuntimeContext spliceRuntimeContext;

    	public WrapOperationWithProviderIterator(SpliceOperation operationSource, SpliceRuntimeContext spliceRuntimeContext) {
    		this.operationSource = operationSource;
    		this.spliceRuntimeContext = spliceRuntimeContext;
    	}

            @Override
            public boolean hasNext() throws StandardException, IOException {
                if(!populated){
                    nextRow = operationSource.nextRow(spliceRuntimeContext); 
                    populated=true;
                }

                return nextRow != null;
            }

            @Override
            public ExecRow next() throws StandardException, IOException {

                if(!populated){
                    hasNext();
                }

                if(nextRow != null){
                    populated = false;
                }

                return nextRow;
            }

    	}    
}
