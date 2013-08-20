package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.iapi.storage.RowProviderIterator;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.KeyType;
import com.splicemachine.derby.utils.marshall.RowDecoder;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
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
    private RowDecoder rowDecoder;

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
            keyColumns[index] = FormatableBitSetUtils.currentRowPositionFromBaseRow(accessedCols,fihArray[index].getInt());
        }

        RowProviderIterator<ExecRow> sourceProvider = wrapScannerWithProvider(regionScanner, getExecRowDefinition(),baseColumnMap);
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
    public ExecRow getNextSinkRow() throws StandardException {

        Pair<ByteBuffer,ExecRow> result = hbs.getNextAggregatedRow();
        ExecRow row = null;

        if(result != null){
            row = result.getSecond();
            setCurrentRow(row);
        }

        return row;
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        List<KeyValue> keyValues = new ArrayList<KeyValue>();
        try {
            regionScanner.next(keyValues);
        } catch (IOException ioe) {
            SpliceLogUtils.logAndThrow(LOG,
                    StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION, ioe));
        }
        if(keyValues.isEmpty()) return null;

        if(rowDecoder==null)
            rowDecoder = getRowEncoder().getDual(getExecRowDefinition(),true);
        return rowDecoder.decode(keyValues);
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
    public RowProvider getMapRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        try{
            reduceScan = Scans.buildPrefixRangeScan(uniqueSequenceID,SpliceUtils.NA_TRANSACTION_ID);
            return new ClientScanProvider("distinctScanMap",SpliceConstants.TEMP_TABLE_BYTES,reduceScan,decoder);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException {
        super.close();
        if(hbs!=null)
            hbs.close();
        if(reduceScan!=null)
            SpliceDriver.driver().getTempCleaner().deleteRange(uniqueSequenceID,reduceScan.getStartRow(),reduceScan.getStopRow());

    }

    @Override
    public RowProvider getReduceRowProvider(SpliceOperation top, RowDecoder decoder) throws StandardException {
        return getMapRowProvider(top, decoder);
    }

    @Override
    protected JobStats doShuffle() throws StandardException {
        Scan scan = buildScan();
        RowProvider provider = new ClientScanProvider("shuffle",Bytes.toBytes(Long.toString(conglomId)),scan,null);

        SpliceObserverInstructions soi = SpliceObserverInstructions.create(activation, this);
        return provider.shuffleRows(soi);
    }

    @Override
    public NoPutResultSet executeScan() throws StandardException {
        RowProvider provider = getReduceRowProvider(this,getRowEncoder().getDual(getExecRowDefinition()));
        return new SpliceNoPutResultSet(activation,this,provider);
    }

    @Override
    public RowEncoder getRowEncoder() throws StandardException {

        ExecRow def = getExecRowDefinition();
        KeyType keyType = KeyType.FIXED_PREFIX;
        return RowEncoder.create(def.nColumns(), keyColumns,
                null,
                uniqueSequenceID,
                keyType, RowMarshaller.packed());
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
            private MultiFieldDecoder decoder;

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
                                decoder = MultiFieldDecoder.create(SpliceDriver.getKryoPool());

                            for(KeyValue kv:values){
                                RowMarshaller.sparsePacked().decode(kv, rowArray, null, decoder);
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
