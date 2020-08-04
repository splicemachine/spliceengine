/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.context.ContextManager;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.loader.GeneratedMethod;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.StaticCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.compile.ActivationClassBuilder;
import com.splicemachine.db.impl.sql.compile.FromTable;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.function.SetCurrentLocatedRowAndRowKeyFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;

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
    protected long pastTx;

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
     * @param defaultRowFunc
     * @param defaultValueMapItem
     * @param pastTxFunctor a functor that returns the id of a committed transaction for time-travel queries, -1 for not set.
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
                              int splits,
                              String delimited,
                              String escaped,
                              String lines,
                              String storedAs,
                              String location,
                              int partitionByRefItem,
                              GeneratedMethod defaultRowFunc,
                              int defaultValueMapItem,
                              GeneratedMethod pastTxFunctor) throws StandardException{
        super(conglomId,activation,resultSetNumber,startKeyGetter,startSearchOperator,stopKeyGetter,stopSearchOperator,
                sameStartStopPosition,rowIdKey,qualifiersField,resultRowAllocator,lockMode,tableLocked,isolationLevel,
                colRefItem,indexColItem,oneRowScan,optimizerEstimatedRowCount,optimizerEstimatedCost,tableVersion,
                pin,splits,delimited,escaped,lines,storedAs,location,partitionByRefItem,defaultRowFunc,defaultValueMapItem);
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
        this.pastTx=-1;
        init();
        if(pastTxFunctor != null) {
            this.pastTx = mapToTxId((DataValueDescriptor)pastTxFunctor.invoke(activation));
            if(pastTx == -1){
                pastTx = SIConstants.OLDEST_TIME_TRAVEL_TX; // force going back to the oldest transaction instead of ignoring it.
            }
        } else {
            this.pastTx = -1; // nothing is set, go ahead and use the latest transaction.
        }
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"isTopResultSet=%s,optimizerEstimatedCost=%f,optimizerEstimatedRowCount=%f",isTopResultSet,optimizerEstimatedCost,optimizerEstimatedRowCount);
    }

    private long mapToTxId(DataValueDescriptor dataValue) throws StandardException {
        if(dataValue instanceof SQLTimestamp) {
            Timestamp ts = ((SQLTimestamp)dataValue).getTimestamp(null);
            SpliceLogUtils.trace(LOG,"time travel ts=%s", ts.toString());
            try {
                return SIDriver.driver().getTxnStore().getTxnAt(ts.getTime());
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }else if(dataValue instanceof SQLTinyint || dataValue instanceof SQLSmallint || dataValue instanceof SQLInteger || dataValue instanceof SQLLongint) {
            return dataValue.getLong();
        }else {
            throw StandardException.newException(SQLState.NOT_IMPLEMENTED); // fix me, we should read SqlTime as well.
        }
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
        pastTx=in.readLong();
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
        out.writeLong(pastTx);
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
    public DataSet<ExecRow> getDataSet(DataSetProcessor dsp) throws StandardException{
        if (!isOpen)
            throw new IllegalStateException("Operation is not open");

        assert currentTemplate!=null:"Current Template Cannot Be Null";

        DataSet<ExecRow> ds = getTableScannerBuilder(dsp);
        if (ds.isNativeSpark())
            dsp.incrementOpDepth();
        dsp.prependSpliceExplainString(this.explainPlan);
        if (ds.isNativeSpark())
            dsp.decrementOpDepth();
        return ds;
    }

    /**
     * @return the string representation for TableScan.
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
     * @param pastTx The ID of the past transaction.
     * @return a view of a past transaction.
     */
    private TxnView getPastTransaction(long pastTx) throws StandardException {
        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        ContextManager cm = ContextService.getFactory().newContextManager();
        TransactionController pastTC = transactionExecute.getAccessManager().getReadOnlyTransaction(cm, pastTx);
        Transaction rawStoreXact=((TransactionManager)pastTC).getRawStoreXact();
        return ((BaseSpliceTransaction)rawStoreXact).getActiveStateTxn();
    }

    /**
     * @return either current transaction or a committed transaction in the past.
     */
    protected TxnView getTransaction() throws StandardException {
        return (pastTx >= 0) ? getPastTransaction(pastTx) : super.getCurrentTransaction();
    }

    @Override
    public TxnView getCurrentTransaction() throws StandardException{
        return getTransaction();
    }

    /**
     * @return the Table Scan Builder for creating the actual data set from a scan.
     * @throws StandardException
     */
    public DataSet<ExecRow> getTableScannerBuilder(DataSetProcessor dsp) throws StandardException{
        TxnView txn = getTransaction();
        operationContext = dsp.createOperationContext(this);

        // we currently don't support external tables in Control, so this shouldn't happen
        assert storedAs == null || !( dsp.getType() == DataSetProcessor.Type.CONTROL && !storedAs.isEmpty() )
                : "tried to access external table " + tableDisplayName + ":" + tableName + " over control/OLTP";
        return dsp.<TableScanOperation,ExecRow>newScanSet(this,tableName)
                .tableDisplayName(tableDisplayName)
                .activation(activation)
                .transaction(txn)
                .scan(getNonSIScan())
                .template(currentTemplate)
                .tableVersion(tableVersion)
                .indexName(indexName)
                .reuseRowLocation(true)
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
                .partitionByColumns(getPartitionColumnMap())
                .defaultRow(defaultRow,scanInformation.getDefaultValueMap())
                .ignoreRecentTransactions(isReadOnly(txn))
                .buildDataSet(this)
                .map(new SetCurrentLocatedRowAndRowKeyFunction<>(operationContext));
    }

    private boolean isReadOnly(TxnView txn) {
        while(txn != Txn.ROOT_TRANSACTION) {
            if (txn.allowsWrites())
                return false;
            txn = txn.getParentTxnView();
        }
        return true;
    }
}
