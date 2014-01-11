package com.splicemachine.derby.impl.sql.execute.operations;


import com.google.common.base.Strings;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.SpliceMethod;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.StandardSupplier;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.Timer;
import org.apache.derby.catalog.types.ReferencedColumnsDescriptorImpl;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.DynamicCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericPreparedStatement;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Maps between an Index Table and a data Table.
 */
public class IndexRowToBaseRowOperation extends SpliceBaseOperation{
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
    private int[] adjustedBaseColumnMap;

    private static final MetricName scanName = new MetricName("com.splicemachine.operations","indexLookup","totalTime");
    private final Timer totalTimer = SpliceDriver.driver().getRegistry().newTimer(scanName,TimeUnit.MILLISECONDS,TimeUnit.SECONDS);
    private IndexRowReader reader;


    public IndexRowToBaseRowOperation () {
		super();
	}

	public IndexRowToBaseRowOperation(long conglomId, int scociItem,
			Activation activation, SpliceOperation source,
			GeneratedMethod resultRowAllocator, int resultSetNumber,
			String indexName, int heapColRefItem, int allColRefItem,
			int heapOnlyColRefItem, int indexColMapItem,
			GeneratedMethod restriction, boolean forUpdate,
			double optimizerEstimatedRowCount, double optimizerEstimatedCost) throws StandardException {
		super(activation, resultSetNumber, optimizerEstimatedRowCount, optimizerEstimatedCost);
		SpliceLogUtils.trace(LOG,"instantiate with parameters");
		this.resultRowAllocatorMethodName = resultRowAllocator.getMethodName();
		this.source = source;
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

            compactRow = operationInformation.compactRow(resultRow, accessedAllCols, false);

            int[] baseColumnMap = operationInformation.getBaseColumnMap();
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
    public NoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
        SpliceLogUtils.trace(LOG,"executeScan");
        final List<SpliceOperation> operationStack = getOperationStack();
        SpliceLogUtils.trace(LOG,"operationStack=%s",operationStack);
        SpliceOperation regionOperation = operationStack.get(0);
        SpliceLogUtils.trace(LOG,"regionOperation=%s",regionOperation);
        RowProvider provider;
				PairDecoder decoder = OperationUtils.getPairDecoder(this,runtimeContext);
        if(regionOperation.getNodeTypes().contains(NodeType.REDUCE)&&this!=regionOperation){
            SpliceLogUtils.trace(LOG,"Scanning temp tables");
			provider = regionOperation.getReduceRowProvider(this,decoder,runtimeContext);
		}else {
			SpliceLogUtils.trace(LOG,"scanning Map table");
			provider = regionOperation.getMapRowProvider(this,decoder,runtimeContext);
		}
		return new SpliceNoPutResultSet(activation,this, provider);
	}

    @Override
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder,SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return source.getMapRowProvider(top, decoder, spliceRuntimeContext);
    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return source.getReduceRowProvider(top, decoder, spliceRuntimeContext);
    }

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
			/*
			 * We only ask for this KeyEncoder if we are the top of a RegionScan.
			 * In this case, we encode with either the current row location or a
			 * random UUID (if the current row location is null).
			 */
				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								if(currentRowLocation!=null)
										return currentRowLocation.getBytes();
								return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
						}
				});

				return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow defnRow = getExecRowDefinition();
				return BareKeyHash.encoder(IntArrays.count(defnRow.nColumns()),null);
		}

		@Override
	public SpliceOperation getLeftOperation() {
		return this.source;
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
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
        long start = System.nanoTime();
        try{
            SpliceLogUtils.trace(LOG,"<%s> nextRow",indexName);
            ExecRow sourceRow;
            ExecRow retRow = null;
            boolean restrict;
            DataValueDescriptor restrictBoolean;

            if(reader==null){
                reader = IndexRowReader.create(source,conglomId,compactRow,getTransactionID(),indexCols,operationInformation.getBaseColumnMap(),heapOnlyCols);
            }

            do{
                IndexRowReader.RowAndLocation roLoc = reader.next();
                boolean rowExists = roLoc!=null;
                if(!rowExists){
                    //No Rows remaining
                    clearCurrentRow();
                    baseRowLocation= null;
                    retRow = null;
                    if(reader!=null){
                        try {
                            reader.close();
                        } catch (IOException e) {
                            SpliceLogUtils.warn(LOG,"Unable to close IndexRowReader");
                        }
                    }
                    break;
                }
                sourceRow = roLoc.row;
                if(baseRowLocation==null)
                    baseRowLocation = new HBaseRowLocation();

                baseRowLocation.setValue(roLoc.rowLocation);

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
                    source.setCurrentRowLocation(baseRowLocation);
                }
            }while(!restrict);
            return retRow;
        }finally{
            if(SpliceConstants.collectStats)
                totalTimer.update(System.nanoTime()-start, TimeUnit.NANOSECONDS);
        }
    }

	@Override
	public void close() throws StandardException, IOException {
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
    public int[] getRootAccessedCols(long tableNumber) throws StandardException {
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
	public String toString() {
		return String.format("IndexRowToBaseRow {source=%s,indexName=%s,conglomId=%d,resultSetNumber=%d}",
                                                                        source,indexName,conglomId,resultSetNumber);
	}

    @Override
    public void open() throws StandardException, IOException {
        super.open();
        if(source!=null)source.open();;
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
}
