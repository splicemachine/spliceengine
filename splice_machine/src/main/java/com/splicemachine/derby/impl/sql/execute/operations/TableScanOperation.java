package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.output.WriteReadUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.List;

public class TableScanOperation extends ScanOperation{
    private static final long serialVersionUID=3l;
    private static Logger LOG=Logger.getLogger(TableScanOperation.class);
    protected int indexColItem;
    public String userSuppliedOptimizerOverrides;
    public int rowsPerRead;
    public ByteSlice slice;
    protected int[] baseColumnMap;
    protected static final String NAME=TableScanOperation.class.getSimpleName().replaceAll("Operation","");
    protected byte[] tableNameBytes;

    @Override
    public String getName(){
        return NAME;
    }


    public TableScanOperation(){
        super();
    }

    @SuppressWarnings("UnusedParameters")
    public TableScanOperation(long conglomId,
                              StaticCompiledOpenConglomInfo scoci,
                              Activation activation,
                              GeneratedMethod resultRowAllocator,
                              int resultSetNumber,
                              GeneratedMethod startKeyGetter,int startSearchOperator,
                              GeneratedMethod stopKeyGetter,int stopSearchOperator,
                              boolean sameStartStopPosition,
                              boolean rowIdKey,
                              String qualifiersField,
                              String tableName,
                              String userSuppliedOptimizerOverrides,
                              String indexName,
                              boolean isConstraint,
                              boolean forUpdate,
                              int colRefItem,
                              int indexColItem,
                              int lockMode,
                              boolean tableLocked,
                              int isolationLevel,
                              int rowsPerRead,
                              boolean oneRowScan,
                              double optimizerEstimatedRowCount,
                              double optimizerEstimatedCost,String tableVersion) throws StandardException{
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,rowIdKey,qualifiersField,resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,indexColItem,oneRowScan,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        SpliceLogUtils.trace(LOG,"instantiated for tablename %s or indexName %s with conglomerateID %d",
                tableName,indexName,conglomId);
        this.forUpdate=forUpdate;
        // JL TODO Will need to get the isolation on the transaction
        //System.out.println("Current Isolation Level" + activation.getLanguageConnectionContext().getCurrentIsolationLevel());
        this.isConstraint=isConstraint;
        this.rowsPerRead=rowsPerRead;
        this.tableName=Long.toString(scanInformation.getConglomerateId());
        this.tableNameBytes=Bytes.toBytes(this.tableName);
        this.indexColItem=indexColItem;
        this.indexName=indexName;
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"statisticsTimingOn=%s,isTopResultSet=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f",statisticsTimingOn,isTopResultSet,optimizerEstimatedCost,optimizerEstimatedRowCount);
        try{
            init(SpliceOperationContext.newContext(activation));
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        recordConstructorTime();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        tableName=in.readUTF();
        tableNameBytes=Bytes.toBytes(tableName);
        indexColItem=in.readInt();
        if(in.readBoolean())
            indexName=in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeInt(indexColItem);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
    }

    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        this.baseColumnMap=operationInformation.getBaseColumnMap();
        this.slice=ByteSlice.empty();
    }

    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.emptyList();
    }


    @Override
    public ExecRow getExecRowDefinition(){
        return currentTemplate;
    }

    @Override
    public String prettyPrint(int indentLevel){
        return "Table"+super.prettyPrint(indentLevel);
    }

    @Override
    public void close() throws StandardException{
        SpliceLogUtils.trace(LOG,"close in TableScan");
        beginTime=getCurrentTimeMillis();
        if(forUpdate && scanInformation.isKeyed()){
            activation.clearIndexScanInfo();
        }
        super.close();
    }

    @Override
    public int[] getAccessedNonPkColumns() throws StandardException{
        FormatableBitSet accessedNonPkColumns=scanInformation.getAccessedNonPkColumns();
        int num=accessedNonPkColumns.getNumBitsSet();
        int[] cols=null;
        if(num>0){
            cols=new int[num];
            int pos=0;
            for(int i=accessedNonPkColumns.anySetBit();i!=-1;i=accessedNonPkColumns.anySetBit(i)){
                cols[pos++]=baseColumnMap[i];
            }
        }
        return cols;
    }

    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        assert currentTemplate!=null:"Current Template Cannot Be Null";
        dsp.createOperationContext(this);
        return getTableScannerBuilder(dsp);
    }

    @Override
    public String toString(){
        try{
            return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,scanInformation.isKeyed(),resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }catch(Exception e){
            return String.format("TableScanOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,"UNKNOWN",resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }
    }

    public DataSet<LocatedRow> getTableScannerBuilder(DataSetProcessor dsp) throws StandardException{
        TxnView txn=getCurrentTransaction();
        return dsp.<TableScanOperation,LocatedRow>newScanSet(this,tableName)
                .transaction(txn)
                .scan(getNonSIScan())
                .template(currentTemplate)
                .tableVersion(tableVersion)
                .indexName(indexName)
                .reuseRowLocation(false)
                .keyColumnEncodingOrder(scanInformation.getColumnOrdering())
                .keyColumnSortOrder(scanInformation.getConglomerate().getAscDescInfo())
                .keyColumnTypes(getKeyFormatIds())
                .execRowTypeFormatIds(WriteReadUtils.getExecRowTypeFormatIds(currentTemplate))
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(baseColumnMap).buildDataSet();
    }

}
