package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationCoprocessor;
import com.splicemachine.derby.iapi.sql.execute.*;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.sort.DistinctSortAggregateBuffer;
import com.splicemachine.derby.impl.sql.execute.operations.sort.SinkSortIterator;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.*;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.job.JobResults;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 5/23/13
 */
public class DistinctScanOperation extends ScanOperation implements SinkingOperation{
    private static final long serialVersionUID = 3l;
    private static final List<NodeType> nodeTypes = Arrays.asList(NodeType.REDUCE,NodeType.SCAN);
    private static Logger LOG = Logger.getLogger(DistinctScanOperation.class);

    private Scan reduceScan;
		private byte[] groupingKey;
		private List<KeyValue> keyValues;
		private Scan scan;

		@SuppressWarnings("UnusedDeclaration")
		public DistinctScanOperation() { }

    private int hashKeyItem;
    private String tableName;
    private String indexName;
    private int[] keyColumns;
    private PairDecoder rowDecoder;

		private SinkSortIterator sinkIterator;
		private DistinctSortAggregateBuffer buffer;


    @SuppressWarnings("UnusedParameters")
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
                                 double optimizerEstimatedCost) throws StandardException {
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
				try {
						init(SpliceOperationContext.newContext(activation));
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
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
    public void init(SpliceOperationContext context) throws StandardException, IOException {
        super.init(context);
        FormatableArrayHolder fah = (FormatableArrayHolder)activation.getPreparedStatement().getSavedObject(hashKeyItem);
        FormatableIntHolder[] fihArray = (FormatableIntHolder[])fah.getArray(FormatableIntHolder.class);

        keyColumns = new int[fihArray.length];
        
        for(int index=0;index<fihArray.length;index++){
            keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(scanInformation.getAccessedColumns(),fihArray[index].getInt());
        }
				this.scan = context.getScan();
				startExecutionTime = System.currentTimeMillis();
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
    public ExecRow getNextSinkRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				if(sinkIterator==null){
						final ExecRow template = getExecRowDefinition();
						StandardSupplier<ExecRow> supplier = new StandardSupplier<ExecRow>() {
								@Override
								public ExecRow get() throws StandardException {
										return template;
								}
						};

						buffer =  new DistinctSortAggregateBuffer(SpliceConstants.ringBufferSize,null,supplier,spliceRuntimeContext);
						FormatableBitSet cols = scanInformation.getAccessedColumns();
						int[] colMap;
						if(cols!=null){
								colMap = new int[cols.getLength()];
								Arrays.fill(colMap,-1);
								for(int i=cols.anySetBit(),pos=0;i>=0;i=cols.anySetBit(i),pos++){
										colMap[i] = pos;
								}
						}else
							colMap = keyColumns;
						StandardIterator<ExecRow> source = new TableScannerBuilder()
										.scanner(regionScanner)
										.template(template)
										.metricFactory(spliceRuntimeContext)
										.scan(scan)
										.rowDecodingMap(colMap)
										.transactionID(transactionID)
										.keyColumnEncodingOrder(scanInformation.getColumnOrdering())
										.keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
										.keyColumnTypes(getKeyFormatIds())
										.accessedKeyColumns(scanInformation.getAccessedPkColumns())
										.keyDecodingMap(getKeyDecodingMap())
										.tableVersion(scanInformation.getTableVersion())
										.indexName(indexName)
										.build();
						DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(template);
						KeyEncoder encoder = KeyEncoder.bare(keyColumns,null,serializers);
						sinkIterator = new SinkSortIterator(buffer, source,encoder);
						timer = spliceRuntimeContext.newTimer();
				}

				timer.startTiming();
				GroupedRow groupedRow = sinkIterator.next(spliceRuntimeContext);
				if(groupedRow ==null){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
						setCurrentRow(null);
						return null;
				}else{
						groupingKey = groupedRow.getGroupingKey();
						setCurrentRow(groupedRow.getRow());
						timer.tick(1);
						return groupedRow.getRow();
				}
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				if(keyValues==null){
						keyValues = Lists.newArrayListWithCapacity(1); //one column from TEMP
						timer = spliceRuntimeContext.newTimer();
				} else
					keyValues.clear();

				timer.startTiming();
        try {
            regionScanner.next(keyValues);
        } catch (IOException ioe) {
            SpliceLogUtils.logAndThrow(LOG,
                    StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
        }
        if(keyValues.isEmpty()){
						timer.stopTiming();
						stopExecutionTime = System.currentTimeMillis();
						return null;
				}

        if(rowDecoder==null){
						rowDecoder = getTempDecoder(spliceRuntimeContext);
				}
				ExecRow decodedRow = rowDecoder.decode(KeyValueUtils.matchDataColumn(keyValues));
				setCurrentRow(decodedRow);
				timer.tick(1);
				return decodedRow;
    }

		@Override
		protected int getNumMetrics() {
				int baseSize = super.getNumMetrics();
				if(buffer!=null)
						baseSize+=3;
				if(regionScanner!=null)
						baseSize+=5;
				return baseSize;
		}

		@Override
		protected void updateStats(OperationRuntimeStats stats) {
				if(buffer!=null){
						//we are in the sink phase
						stats.addMetric(OperationMetric.FILTERED_ROWS,buffer.getRowsMerged());
						stats.addMetric(OperationMetric.INPUT_ROWS,sinkIterator.getRowsRead());
						stats.setBufferFillRatio(buffer.getMaxFillRatio());
				}
				//in both sink and scan we read from local scanner
				if(regionScanner!=null){
						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,regionScanner.getBytesOutput());
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,regionScanner.getRowsOutput());
						TimeView localReadTime = regionScanner.getReadTime();
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME,localReadTime.getWallClockTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,localReadTime.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,localReadTime.getUserTime());
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
		public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException {
				try{
						reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
						/*
						 * We do a bit of an optimization here. If top == this, then we would be doing the following:
						 * 1. scan data out of Region as KeyValue (locally)
						 * 2. decode it into row
						 * 3. encode it into KeyValue
						 * 4. send it over the network.
						 *
						 * So there's no point in actually sending all this information over to the server, since it won't be doing anything
						 * anyway.
						 *
						 * However, this means that the serialization format is different, so we have to deal with that
						 */
						if(top != this)
								SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
						else{
								decoder = getTempDecoder(spliceRuntimeContext);
						}
						return new DistributedClientScanProvider("distinctScanReduce", SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
        }
    }

		/**
		 * @return the decoder to use when reading data out of TEMP
		 * @throws StandardException
		 */
		protected PairDecoder getTempDecoder(SpliceRuntimeContext ctx) throws StandardException {
				ExecRow templateRow = getExecRowDefinition();
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(templateRow);
				PairDecoder decoder;
				KeyDecoder actualKeyDecoder = new KeyDecoder(BareKeyHash.decoder(keyColumns, null, serializers),9);
				KeyHashDecoder actualRowDecoder =  BareKeyHash.decoder(IntArrays.complement(keyColumns, templateRow.nColumns()),null,serializers);
				decoder = new PairDecoder(actualKeyDecoder,actualRowDecoder,templateRow);
				return decoder;
		}

		@Override
    public void close() throws StandardException, IOException {
        super.close();
    }

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}

		@Override
		public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException {
				return getMapRowProvider(top, decoder, spliceRuntimeContext);
		}

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        Scan scan = getNonSIScan(runtimeContext);
        
        RowProvider provider = new ClientScanProvider("shuffle",Bytes.toBytes(Long.toString(scanInformation.getConglomerateId())),scan,null,runtimeContext);

        SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this,runtimeContext);
        return provider.shuffleRows(soi,OperationUtils.cleanupSubTasks(this));
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						RowProvider provider = getReduceRowProvider(this, OperationUtils.getPairDecoder(this, runtimeContext), runtimeContext, true);
						return new SpliceNoPutResultSet(activation,this,provider);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
    }

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				HashPrefix keyPrefix = new BucketingPrefix(new FixedPrefix(uniqueSequenceID), HashFunctions.murmur3(0),SpliceDriver.driver().getTempTable().getCurrentSpread());
				DataHash hash = new SuppliedDataHash(new StandardSupplier<byte[]>() {
						@Override
						public byte[] get() throws StandardException {
								return groupingKey;
						}
				});
				KeyPostfix postfix = NoOpPostfix.INSTANCE;

				return new KeyEncoder(keyPrefix,hash,postfix);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				ExecRow execRowDefinition = getExecRowDefinition();
				DescriptorSerializer[] serializers = VersionedSerializers.latestVersion(false).getSerializers(execRowDefinition);
				return BareKeyHash.encoder(IntArrays.complement(keyColumns, execRowDefinition.nColumns()),null,serializers);
		}

		@Override
    public String prettyPrint(int indentLevel) {
        return "Distinct"+super.prettyPrint(indentLevel);
    }

}
