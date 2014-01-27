package com.splicemachine.derby.impl.sql.execute.operations;

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
import com.splicemachine.derby.impl.sql.execute.deprecate.DistinctMerger;
import com.splicemachine.derby.impl.sql.execute.deprecate.HashBufferSource;
import com.splicemachine.derby.impl.sql.execute.deprecate.HashMerger;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.impl.storage.DistributedClientScanProvider;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.job.JobResults;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.IntArrays;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableArrayHolder;
import org.apache.derby.iapi.services.io.FormatableIntHolder;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.store.access.StaticCompiledOpenConglomInfo;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    private static final HashMerger merger = new DistinctMerger();

    private Scan reduceScan;

    public DistinctScanOperation() {
    }

    private int hashKeyItem;
    private String tableName;
    private String indexName;
    private int[] keyColumns;
    private PairDecoder rowDecoder;

    private HashBufferSource hbs;


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
        init(SpliceOperationContext.newContext(activation));
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
            keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(scanInformation.getAccessedColumns(),fihArray[index].getInt());
        }
 
        RowProviderIterator<ExecRow> sourceProvider = wrapScannerWithProvider(regionScanner,
                getExecRowDefinition(),operationInformation.getBaseColumnMap());
        MultiFieldEncoder mfe = MultiFieldEncoder.create(SpliceDriver.getKryoPool(),keyColumns.length + 1);

        hbs = new HashBufferSource(uniqueSequenceID, keyColumns, sourceProvider, merger, KeyType.FIXED_PREFIX, mfe);
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
        Pair<ByteBuffer,ExecRow> result = hbs.getNextAggregatedRow();
        ExecRow row = null;

        if(result != null){
            row = result.getSecond();
            setCurrentRow(row);
        }

        return row;
    }

    @Override
    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        try {
            regionScanner.next(keyValues);
        } catch (IOException ioe) {
            SpliceLogUtils.logAndThrow(LOG,
                    StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
        }
        if(keyValues.isEmpty()) return null;

        if(rowDecoder==null){
						rowDecoder = OperationUtils.getPairDecoder(this,spliceRuntimeContext);
				}
        return rowDecoder.decode(KeyValueUtils.matchDataColumn(keyValues));
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
    public RowProvider getMapRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        try{
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
            if(top != this)
                SpliceUtils.setInstructions(reduceScan,activation,top,spliceRuntimeContext);
			return new DistributedClientScanProvider("distinctScanReduce", SpliceOperationCoprocessor.TEMP_TABLE,reduceScan,decoder, spliceRuntimeContext);
//            return new ClientScanProvider("distinctScanMap",SpliceConstants.TEMP_TABLE_BYTES,reduceScan,
//										decoder,spliceRuntimeContext);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException, IOException {
        super.close();
        if(hbs!=null)
            hbs.close();
    }

		@Override
		public byte[] getUniqueSequenceId() {
				return uniqueSequenceID;
		}

		@Override
    public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
        return getMapRowProvider(top, decoder, spliceRuntimeContext);
			
    }

    @Override
    protected JobResults doShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
        Scan scan = buildScan(runtimeContext);
        
        RowProvider provider = new ClientScanProvider("shuffle",Bytes.toBytes(Long.toString(scanInformation.getConglomerateId())),scan,null,runtimeContext);

        SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this,runtimeContext);
        return provider.shuffleRows(soi);
    }

    @Override
    public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException {
    	
        RowProvider provider = getReduceRowProvider(this,OperationUtils.getPairDecoder(this,runtimeContext), runtimeContext);
        return new SpliceNoPutResultSet(activation,this,provider);
    }

		@Override
		public CallBuffer<KVPair> transformWriteBuffer(CallBuffer<KVPair> bufferToTransform) throws StandardException {
				return bufferToTransform;
		}

		@Override
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				HashPrefix keyPrefix = new BucketingPrefix(new FixedPrefix(uniqueSequenceID), HashFunctions.murmur3(0),SpliceDriver.driver().getTempTable().getCurrentSpread());
				DataHash hash = BareKeyHash.encoder(keyColumns,null);
				KeyPostfix postfix = NoOpPostfix.INSTANCE;

				return new KeyEncoder(keyPrefix,hash,postfix);
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				return BareKeyHash.encoder(IntArrays.complement(keyColumns,getExecRowDefinition().nColumns()),null);
		}

		@Override
    public String prettyPrint(int indentLevel) {
        return "Distinct"+super.prettyPrint(indentLevel);
    }


    private static RowProviderIterator<ExecRow> wrapScannerWithProvider(final RegionScanner regionScanner, final ExecRow rowTemplate, final int[] baseColumnMap){
        return new RowProviderIterator<ExecRow>() {

            private List<KeyValue> values = new ArrayList(rowTemplate.nColumns());

            private ExecRow nextRow;
            private boolean populated = false;
            private EntryDecoder decoder;

            @Override
            public boolean hasNext() throws StandardException {
                try{
                    if(!populated){

                        nextRow = null;
                        values.clear();
                        regionScanner.next(values);
                        populated = true;

                        if(!values.isEmpty()){
                            nextRow = rowTemplate.getClone();
                            DataValueDescriptor[] rowArray = nextRow.getRowArray();

                            if(decoder==null)
                                decoder = new EntryDecoder(SpliceDriver.getKryoPool());

                            for(KeyValue kv:values){
                                RowMarshaller.sparsePacked().decode(kv, rowArray, baseColumnMap, decoder);
                            }
                        }

                    }
                } catch(IOException e){
                    throw Exceptions.parseException(e);
                }
                if(nextRow==null && decoder!=null)
                    decoder.close();
                return nextRow != null;
            }

            @Override
            public ExecRow next() throws StandardException {

                if(!populated){
                    hasNext();
                }

                if(nextRow != null){
                    populated = false;
                }

                return nextRow;
            }
        };
    }
}
