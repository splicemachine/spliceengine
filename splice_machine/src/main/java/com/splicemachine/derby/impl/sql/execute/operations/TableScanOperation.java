/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.impl.sql.compile.ActivationClassBuilder;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.output.WriteReadUtils;
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
import com.splicemachine.db.impl.sql.compile.FromTable;

/**
 *
 * Base Operation for scanning either and index or a base table.
 *
 */
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

    /**
     *
     * Return the nice formatted name for the Table Scan operation.
     *
     * @return
     */
    @Override
    public String getName(){
        return NAME;
    }

    /**
     * Empty Constructor
     *
     */
    public TableScanOperation(){
        super();
    }

    /**
     *
     * Massive Constructor that is generated from the SQL Parser.
     *
     * Here is where these elements are created.
     *
     * @see FromTable#generate(ActivationClassBuilder, MethodBuilder)
     *
     * @param conglomId
     * @param scoci
     * @param activation
     * @param resultRowAllocator
     * @param resultSetNumber
     * @param startKeyGetter
     * @param startSearchOperator
     * @param stopKeyGetter
     * @param stopSearchOperator
     * @param sameStartStopPosition
     * @param rowIdKey
     * @param qualifiersField
     * @param tableName
     * @param userSuppliedOptimizerOverrides
     * @param indexName
     * @param isConstraint
     * @param forUpdate
     * @param colRefItem
     * @param indexColItem
     * @param lockMode
     * @param tableLocked
     * @param isolationLevel
     * @param rowsPerRead
     * @param oneRowScan
     * @param optimizerEstimatedRowCount
     * @param optimizerEstimatedCost
     * @param tableVersion
     * @throws StandardException
     */
    @SuppressWarnings("UnusedParameters")
    public TableScanOperation(long conglomId,
                              StaticCompiledOpenConglomInfo scoci,
                              Activation activation,
                              GeneratedMethod resultRowAllocator,
                              int resultSetNumber,
                              GeneratedMethod startKeyGetter,
                              int startSearchOperator,
                              GeneratedMethod stopKeyGetter,
                              int stopSearchOperator,
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
                              double optimizerEstimatedCost,
                              String tableVersion) throws StandardException{
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,rowIdKey,qualifiersField,resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,indexColItem,oneRowScan,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion);
        SpliceLogUtils.trace(LOG,"instantiated for tablename %s or indexName %s with conglomerateID %d",
                tableName,indexName,conglomId);
        this.forUpdate=forUpdate;
        this.isConstraint=isConstraint;
        this.rowsPerRead=rowsPerRead;
        this.tableName=Long.toString(scanInformation.getConglomerateId());
        this.tableDisplayName = tableName;
        this.tableNameBytes=Bytes.toBytes(this.tableName);
        this.indexColItem=indexColItem;
        this.indexName=indexName;
        init();
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"isTopResultSet=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f",isTopResultSet,optimizerEstimatedCost,optimizerEstimatedRowCount);
    }

    /**
     *
     * Serialization/Deserialization
     *
     * TODO SPLICE-716
     *
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        tableName=in.readUTF();
        tableDisplayName=in.readUTF();
        tableNameBytes=Bytes.toBytes(tableName);
        indexColItem=in.readInt();
        if(in.readBoolean())
            indexName=in.readUTF();
    }
    /**
     *
     * Serialization/Deserialization
     *
     * TODO SPLICE-716
     *
     * @param out
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeUTF(tableName);
        out.writeUTF(tableDisplayName);
        out.writeInt(indexColItem);
        out.writeBoolean(indexName!=null);
        if(indexName!=null)
            out.writeUTF(indexName);
    }

    /**
     *
     * Initialize variables after creation or serialization.
     *
     * @param context
     * @throws StandardException
     * @throws IOException
     */
    @Override
    public void init(SpliceOperationContext context) throws StandardException, IOException{
        super.init(context);
        this.baseColumnMap=operationInformation.getBaseColumnMap();
        this.slice=ByteSlice.empty();
    }

    /**
     *
     * Recursive fetch of operations below this operation.  Empty in the
     * case of a table scan operation.
     *
     * @return
     */
    @Override
    public List<SpliceOperation> getSubOperations(){
        return Collections.emptyList();
    }

    /**
     *
     * Definiton of the current row as an ExecRow
     *
     * @return
     */
    @Override
    public ExecRow getExecRowDefinition(){
        return currentTemplate;
    }

    /**
     *
     * Prints the name for explain plan.
     *
     * @param indentLevel
     * @return
     */
    @Override
    public String prettyPrint(int indentLevel){
        return "Table"+super.prettyPrint(indentLevel);
    }

    /**
     *
     * Close the current operation.  Usually called via the activation process.
     *
     * @throws StandardException
     */
    @Override
    public void close() throws StandardException{
        SpliceLogUtils.trace(LOG,"close in TableScan");
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

    /**
     *
     * Retrieve the DataSet abstraction for this table scan.
     *
     * @param dsp
     * @return
     * @throws StandardException
     */
    @Override
    public DataSet<LocatedRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        assert currentTemplate!=null:"Current Template Cannot Be Null";
        return getTableScannerBuilder(dsp);
    }

    /**
     *
     * Return the string representation for TableScan.
     *
     * @return
     */
    @Override
    public String toString(){
        try{
            return String.format("TableScanOperation {tableName=%s,isKeyed=%b,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,scanInformation.isKeyed(),resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }catch(Exception e){
            return String.format("TableScanOperation {tableName=%s,isKeyed=%s,resultSetNumber=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f}",tableName,"UNKNOWN",resultSetNumber,optimizerEstimatedCost,optimizerEstimatedRowCount);
        }
    }

    /**
     *
     * Retrieve the Table Scan Builder for creating the actual data set from a scan.
     *
     * @param dsp
     * @return
     * @throws StandardException
     */
    public DataSet<LocatedRow> getTableScannerBuilder(DataSetProcessor dsp) throws StandardException{
        TxnView txn=getCurrentTransaction();
        return dsp.<TableScanOperation,LocatedRow>newScanSet(this,tableName)
                .tableDisplayName(tableDisplayName)
                .activation(activation)
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
                .rowDecodingMap(baseColumnMap)
                .buildDataSet(this);
    }
}