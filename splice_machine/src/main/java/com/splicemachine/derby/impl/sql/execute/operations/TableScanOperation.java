/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
 * Base Operation for scanning either and index, base table, or an external table.
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
     * @param delimited
     * @param escaped
     * @param lines
     * @param location
     * @param pin
     * @param storedAs
     *
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
                              String tableVersion,
                              boolean pin,
                              String delimited,
                              String escaped,
                              String lines,
                              String storedAs,
                              String location) throws StandardException{
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,rowIdKey,qualifiersField,resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,indexColItem,oneRowScan,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion,
                pin,delimited,escaped,lines,storedAs,location);
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
     * @param in
     * @throws IOException
     * @throws ClassNotFoundException
     */
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        pin = in.readBoolean();
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
     * @param out
     * @throws IOException
     */
    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeBoolean(pin);
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
                .accessedKeyColumns(scanInformation.getAccessedPkColumns())
                .keyDecodingMap(getKeyDecodingMap())
                .rowDecodingMap(getRowDecodingMap())
                .baseColumnMap(baseColumnMap)
                .pin(pin)
                .delimited(delimited)
                .escaped(escaped)
                .lines(lines)
                .storedAs(storedAs)
                .location(location)
                .buildDataSet(this);
    }
}