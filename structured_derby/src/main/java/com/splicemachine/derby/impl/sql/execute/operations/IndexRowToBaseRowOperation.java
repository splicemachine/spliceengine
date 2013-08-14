package com.splicemachine.derby.impl.sql.execute.operations;


import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.*;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.ConcurrentRingBuffer;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Maps between an Index Table and a data Table.
 */
public class IndexRowToBaseRowOperation extends SpliceBaseOperation implements CursorResultSet{
	private static Logger LOG = Logger.getLogger(IndexRowToBaseRowOperation.class);
	protected int lockMode;
	protected int isolationLevel;
	protected FormatableBitSet accessedCols;
	protected String resultRowAllocatorMethodName;
	protected StaticCompiledOpenConglomInfo scoci;
	protected DynamicCompiledOpenConglomInfo dcoci;
	protected SpliceOperation source;
	protected String indexName;
	protected boolean forUpdate;
	protected SpliceMethod<DataValueDescriptor> restriction;
	protected String restrictionMethodName;
	protected FormatableBitSet accessedHeapCols;
	protected FormatableBitSet heapOnlyCols;
	protected FormatableBitSet accessedAllCols;
	protected int[] indexCols;
	protected ExecRow resultRow;
	protected DataValueDescriptor[]	rowArray;
	protected int scociItem;
	protected long conglomId;
	protected int heapColRefItem;
	protected int allColRefItem;
	protected int heapOnlyColRefItem;
	protected int indexColMapItem;
	private ExecRow compactRow;
	RowLocation baseRowLocation = null;
	boolean copiedFromSource = false;

	/*
 	 * Variable here to stash pre-generated DataValue definitions for use in
 	 * getExecRowDefinition(). Save a little bit of performance by caching it
 	 * once created.
 	 */
	private HTableInterface table;
    private MultiFieldDecoder fieldDecoder;
    private int[] adjustedBaseColumnMap;
    private byte[] predicateFilterBytes;
    private ConcurrentRingBuffer<RowAndLocation> ringBuffer;


    public IndexRowToBaseRowOperation () {
		super();
	}

	public IndexRowToBaseRowOperation(long conglomId, int scociItem,
			Activation activation, NoPutResultSet source,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			String indexName, int heapColRefItem, int allColRefItem,
			int heapOnlyColRefItem, int indexColMapItem,
			GeneratedMethod restriction, boolean forUpdate,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		SpliceLogUtils.trace(LOG,"instantiate with parameters");
		this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
		this.source = (SpliceOperation) source;
		this.indexName = indexName;
		this.forUpdate = forUpdate;
		this.scociItem = scociItem;
		this.conglomId = conglomId;
		this.heapColRefItem = heapColRefItem;
		this.allColRefItem = allColRefItem;
		this.heapOnlyColRefItem = heapOnlyColRefItem;
		this.indexColMapItem = indexColMapItem;
		this.restrictionMethodName = restriction==null? null: restriction.getMethodName();
		init(SpliceOperationContext.newContext(activation));
		recordConstructorTime(); 
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		super.readExternal(in);
		scociItem = in.readInt();
		conglomId = in.readLong();
		heapColRefItem = in.readInt();
		allColRefItem = in.readInt();
		heapOnlyColRefItem = in.readInt();
		indexColMapItem = in.readInt();
		source = (SpliceOperation) in.readObject();
		accessedCols = (FormatableBitSet) in.readObject();
		resultRowAllocatorMethodName = in.readUTF();
		indexName = in.readUTF();
		restrictionMethodName = readNullableString(in);
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		super.writeExternal(out);
		out.writeInt(scociItem);
		out.writeLong(conglomId);
		out.writeInt(heapColRefItem);
		out.writeInt(allColRefItem);
		out.writeInt(heapOnlyColRefItem);
		out.writeInt(indexColMapItem);
		out.writeObject(source);
		out.writeObject(accessedCols);
		out.writeUTF(resultRowAllocatorMethodName);
		out.writeUTF(indexName);
		writeNullableString(restrictionMethodName, out);
	}

	@Override
	public void init(SpliceOperationContext context) throws StandardException{
		super.init(context);
		source.init(context);
		try {
			if(restrictionMethodName !=null){
				SpliceLogUtils.trace(LOG,"%s:restrictionMethodName=%s",indexName,restrictionMethodName);
				restriction = new SpliceMethod<DataValueDescriptor>(restrictionMethodName,activation);
			}
			SpliceMethod<ExecRow> generatedMethod = new SpliceMethod<ExecRow>(resultRowAllocatorMethodName,activation);
			final GenericPreparedStatement gp = (GenericPreparedStatement)activation.getPreparedStatement();
			final Object[] saved = gp.getSavedObjects();
			scoci = (StaticCompiledOpenConglomInfo)saved[scociItem];
			TransactionController tc = activation.getTransactionController();
			dcoci = tc.getDynamicCompiledConglomInfo(conglomId);
			// the saved objects, if it exists
			if (heapColRefItem != -1) {
				this.accessedHeapCols = (FormatableBitSet)saved[heapColRefItem];
			}
			if (allColRefItem != -1) {
				this.accessedAllCols = (FormatableBitSet)saved[allColRefItem];
			}

            // retrieve the array of columns coming from the index
            indexCols = ((ReferencedColumnsDescriptorImpl) saved[indexColMapItem]).getReferencedColumnPositions();
			/* Get the result row template */           
			resultRow = generatedMethod.invoke();

            compactRow = getCompactRow(activation.getLanguageConnectionContext(),resultRow, accessedAllCols, false);

			if(heapOnlyColRefItem!=-1){
				this.heapOnlyCols = (FormatableBitSet)saved[heapOnlyColRefItem];
                adjustedBaseColumnMap = new int[heapOnlyCols.getNumBitsSet()];
                int pos=0;
                for(int i=heapOnlyCols.anySetBit();i>=0;i=heapOnlyCols.anySetBit(i)){
                    adjustedBaseColumnMap[pos] = baseColumnMap[i];
                    pos++;
                }
			}

            if (accessedHeapCols == null) {
                rowArray = resultRow.getRowArray();

            }
            else {
                // Figure out how many columns are coming from the heap
                final DataValueDescriptor[] resultRowArray = resultRow.getRowArray();
                final int heapOnlyLen = heapOnlyCols.getLength();

                // Need a separate DataValueDescriptor array in this case
                rowArray = new DataValueDescriptor[heapOnlyLen];
                final int minLen = Math.min(resultRowArray.length, heapOnlyLen);

                // Make a copy of the relevant part of rowArray
                for (int i = 0; i < minLen; ++i) {
                    if (resultRowArray[i] != null && heapOnlyCols.isSet(i)) {
                        rowArray[i] = resultRowArray[i];
                    }
                }
                if (indexCols != null) {
                    for (int index = 0; index < indexCols.length; index++) {
                        if (indexCols[index] != -1) {
                            compactRow.setColumn(index + 1,source.getExecRowDefinition().getColumn(indexCols[index] + 1));
                        }
                    }
                }
            }
            SpliceLogUtils.trace(LOG,"accessedAllCols=%s,accessedHeapCols=%s,heapOnlyCols=%s,accessedCols=%s",accessedAllCols,accessedHeapCols,heapOnlyCols,accessedCols);
            SpliceLogUtils.trace(LOG,"rowArray=%s,compactRow=%s,resultRow=%s,resultSetNumber=%d",
                    Arrays.asList(rowArray),compactRow,resultRow,resultSetNumber);
        } catch (StandardException e) {
        	SpliceLogUtils.logAndThrowRuntime(LOG, "Operation Init Failed!",e);
        }

    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        SpliceLogUtils.trace(LOG,"executeScan");
        final List<SpliceOperation> operationStack = getOperationStack();
        SpliceLogUtils.trace(LOG,"operationStack=%s",operationStack);
        SpliceOperation regionOperation = operationStack.get(0);
        SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
        RowProvider provider;
        RowDecoder decoder = getRowEncoder().getDual(getExecRowDefinition());
        if(regionOperation.getNodeTypes().contains(NodeType.REDUCE)&&this!=regionOperation){
            SpliceLogUtils.trace(LOG,"Scanning temp tables");
			provider = regionOperation.getReduceRowProvider(this,decoder);
		}else {
			SpliceLogUtils.trace(LOG,"scanning Map table");
			provider = regionOperation.getMapRowProvider(this,decoder);
		}
		return new SpliceNoPutResultSet(activation,this, provider);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return source.getMapRowProvider(top, decoder);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return source.getReduceRowProvider(top, decoder);
    }

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        ExecRow template = getExecRowDefinition();
        return RowEncoder.create(template.nColumns(),
                null, null, null,
                KeyType.BARE, RowMarshaller.packed());
    }

    @Override
	public SpliceOperation getLeftOperation() {
		return this.source;
	}
	
	@Override
	public RowLocation getRowLocation() throws StandardException {
		return currentRowLocation;
	}

	@Override
	public ExecRow getCurrentRow() throws StandardException {
		return currentRow;
	}

	@Override
	public List<NodeType> getNodeTypes() {
		return Collections.singletonList(NodeType.SCAN);
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		SpliceLogUtils.trace(LOG,"getSubOperations");
		return Collections.singletonList(source);
	}

	@Override
	public ExecRow getNextRowCore() throws StandardException {
		SpliceLogUtils.trace(LOG,"<%s> getNextRowCore",indexName);
		ExecRow sourceRow;
		ExecRow retRow = null;
		boolean restrict;
		DataValueDescriptor restrictBoolean;

        if(ringBuffer==null)
            ringBuffer = new ConcurrentRingBuffer<RowAndLocation>(100,new RowAndLocation[0],new IndexFiller(25,source));
        do{
            try{
                RowAndLocation roLoc = ringBuffer.next();
                boolean rowExists = roLoc!=null;
                if(!rowExists){
                    //No Rows remaining
                    clearCurrentRow();
                    baseRowLocation= null;
                    retRow = null;
                    if(table!=null){
                        try {
                            table.close();
                        } catch (IOException e) {
                            SpliceLogUtils.warn(LOG,"Unable to close HTable");
                        }
                    }
                    break;
                }
                sourceRow = roLoc.getRow();
                if(baseRowLocation==null)
                    baseRowLocation = new HBaseRowLocation();

                baseRowLocation.setValue(roLoc.location);

                SpliceLogUtils.trace(LOG,"<%s> retrieved index row %s",indexName,sourceRow);
                SpliceLogUtils.trace(LOG, "<%s>compactRow=%s", indexName,compactRow);
                setCurrentRow(sourceRow);
                currentRowLocation = baseRowLocation;
                restrictBoolean = ((restriction == null) ? null: restriction.invoke());
                restrict = (restrictBoolean ==null) ||
                        ((!restrictBoolean.isNull()) && restrictBoolean.getBoolean());

                if(!restrict||!rowExists){
                    clearCurrentRow();
                    baseRowLocation=null;
                    currentRowLocation=null;
                }else{
                    retRow = sourceRow;
                    setCurrentRow(sourceRow);
                    currentRowLocation = baseRowLocation;
                }
            }catch(ExecutionException e){
                throw Exceptions.parseException(e.getCause());
            }
        }while(!restrict);
        return retRow;
	}

	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close in IndexRowToBaseRow");
		beginTime = getCurrentTimeMillis();
		source.close();
		super.close();
		closeTime += getElapsedMillis(beginTime);
	}

	@Override
	public ExecRow getExecRowDefinition() {
		return compactRow.getClone();
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        return source.getRootAccessedCols(tableNumber);
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        return source.isReferencingTable(tableNumber);
    }

    public String getIndexName() {
		return this.indexName;
	}
	
	public  FormatableBitSet getAccessedHeapCols() {
		return this.accessedHeapCols;
	}
	
	public SpliceOperation getSource() {
		return this.source;
	}
	
	@Override
	public long getTimeSpent(int type)
	{
		long totTime = constructorTime + openTime + nextTime + closeTime;

		if (type == CURRENT_RESULTSET_ONLY)
			return	totTime - source.getTimeSpent(ENTIRE_RESULTSET_TREE);
		else
			return totTime;
	}
	
	@Override
	public String toString() {
		return String.format("IndexRowToBaseRow {source=%s,indexName=%s,conglomId=%d,resultSetNumber=%d}",
                                                                        source,indexName,conglomId,resultSetNumber);
	}

    @Override
    public void openCore() throws StandardException {
        super.openCore();
        if(source!=null)source.openCore();
    }

    @Override
    public String prettyPrint(int indentLevel) {
        String indent = "\n"+ Strings.repeat("\t",indentLevel);

        return new StringBuilder("IndexRowToBaseRow:")
                .append(indent).append("resultSetNumber:").append(resultSetNumber)
                .append(indent).append("accessedCols:").append(accessedCols)
                .append(indent).append("resultRowAllocatorMethodName:").append(resultRowAllocatorMethodName)
                .append(indent).append("indexName:").append(indexName)
                .append(indent).append("accessedHeapCols:").append(accessedHeapCols)
                .append(indent).append("heapOnlyCols:").append(heapOnlyCols)
                .append(indent).append("accessedAllCols:").append(accessedAllCols)
                .append(indent).append("indexCols:").append(Arrays.toString(indexCols))
                .append(indent).append("source:").append(source.prettyPrint(indentLevel+1))
                .toString();
    }

    private class IndexFiller implements ConcurrentRingBuffer.Filler<RowAndLocation>{
        private final List<Get> batch;
        private final int batchSize;
        private final SpliceOperation source;
        private HTableInterface table;

        private final RowAndLocation[] rowBatch;
        private Result[] resultBatch;
        private int currentResultPos=-1;

        private byte[] predicateFilterBytes;
        private boolean populated = false;
        private boolean finished = false;

        private IndexFiller(int batchSize, SpliceOperation source) {
            this.batch = Lists.newArrayListWithCapacity(batchSize);
            this.batchSize = batchSize;
            this.source = source;
            this.rowBatch = new RowAndLocation[batchSize];
        }

        @Override
        public void prepareToFill() throws ExecutionException {
            //no-op
        }

        @Override
        public RowAndLocation getNext(RowAndLocation old) throws ExecutionException {
            if(currentResultPos<0 || currentResultPos>=resultBatch.length){
                fillBatch();
            }
            if(currentResultPos<0 || currentResultPos>=resultBatch.length)
               return null; //nothing to return now

            //we have data to return
            RowAndLocation next = rowBatch[currentResultPos];
            Result nextResult = resultBatch[currentResultPos];
            if(fieldDecoder==null)
                fieldDecoder = MultiFieldDecoder.create();
            fieldDecoder.reset();

            currentResultPos++;
            if(old!=null){
                try{
                    for(KeyValue kv:nextResult.raw()){
                        RowMarshaller.sparsePacked().decode(kv,old.row.getRowArray(),adjustedBaseColumnMap,fieldDecoder);
                    }
                } catch (StandardException e) {
                    throw new ExecutionException(e);
                }
                old.location = next.location;
                return old;
            }else{
                try{
                    for(KeyValue kv:nextResult.raw()){
                        RowMarshaller.sparsePacked().decode(kv,next.row.getRowArray(),adjustedBaseColumnMap,fieldDecoder);
                    }
                } catch (StandardException e) {
                    throw new ExecutionException(e);
                }
                return new RowAndLocation(next);
            }
        }

        private void fillBatch() throws ExecutionException {
            if(finished) return; //nothing more to do
            try{
                for(int batchCount=0;batchCount<batchSize;batchCount++){
                    ExecRow sourceRow = source.getNextRowCore();
                    if(sourceRow==null){
                        finished=true;
                        break;
                    }

                    if(!populated){
                        for(int index=0;index<indexCols.length;index++){
                            if(indexCols[index]!=-1){
                                compactRow.setColumn(index+1,sourceRow.getColumn(indexCols[index]+1));
                            }
                        }

                        populated=true;
                    }

                    RowAndLocation rowLoc = rowBatch[batchCount];
                    if(rowLoc==null){
                        rowLoc = new RowAndLocation(compactRow.getClone(),null);
                        rowBatch[batchCount] = rowLoc;
                    }

                    rowLoc.setLocation(sourceRow.getColumn(sourceRow.nColumns()).getBytes());

                    if(predicateFilterBytes==null){
                        BitSet fieldsToReturn = new BitSet(heapOnlyCols.getNumBitsSet());
                        for(int i=heapOnlyCols.anySetBit();i>=0;i=heapOnlyCols.anySetBit(i)){
                            fieldsToReturn.set(i);
                        }
                        EntryPredicateFilter getPredicateFilter = new EntryPredicateFilter(fieldsToReturn, Collections.<Predicate>emptyList());
                        predicateFilterBytes = getPredicateFilter.toBytes();
                    }

                    Get get = SpliceUtils.createGet(getTransactionID(), rowLoc.location);
                    get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilterBytes);
                    batch.add(get);
                }

                //get the results
                if(table==null)
                    table = SpliceAccessManager.getHTable(conglomId);

                if(batch.size()>0){
                    resultBatch= table.get(batch);
                    batch.clear();
                    currentResultPos=0;
                }
            }catch(StandardException se){
                throw new ExecutionException(se);
            }catch(IOException ioe){
                throw new ExecutionException(ioe);
            }
        }

        @Override
        public void finishFill() throws ExecutionException {
            //no-op
        }
    }

    private static class RowAndLocation{
        private ExecRow row;
        private byte[] location;

        private RowAndLocation(ExecRow row, byte[] location) {
            this.row = row;
            this.location = location;
        }

        private RowAndLocation(RowAndLocation rowAndLocation){
           this.row = rowAndLocation.row.getClone();
            this.location = rowAndLocation.location;
        }

        public ExecRow getRow() {
            return row;
        }

        public void setRow(ExecRow row) {
            this.row = row;
        }

        public byte[] getLocation() {
            return location;
        }

        public void setLocation(byte[] location) {
            this.location = location;
        }
    }
}
