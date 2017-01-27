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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.splicemachine.derby.stream.iapi.*;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.ExecutionFactory;
import com.splicemachine.db.iapi.sql.execute.NoPutResultSet;
import com.splicemachine.db.iapi.sql.execute.RowChanger;
import com.splicemachine.db.iapi.sql.execute.TargetResultSet;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceBaseOperation implements SpliceOperation, ScopeNamed, Externalizable{
    private static final long serialVersionUID=4l;
    private static Logger LOG=Logger.getLogger(SpliceBaseOperation.class);
    private static Logger LOG_CLOSE=Logger.getLogger(SpliceBaseOperation.class.getName()+".close");
    protected Iterator<LocatedRow> locatedRowIterator;
    protected Activation activation;
    protected String explainPlan="";
    protected double optimizerEstimatedRowCount;
    protected double optimizerEstimatedCost;
    protected boolean isTopResultSet=false;
    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;
    protected boolean executed=false;
    protected OperationContext operationContext;
    protected boolean isOpen=true;
    protected int resultSetNumber;
    protected OperationInformation operationInformation;
    protected LocatedRow locatedRow;
    protected StatementContext statementContext;
    protected List<AutoCloseable> closeables;
    protected NoPutResultSet[] subqueryTrackingArray;
    protected List<SpliceOperation> leftOperationStack;
    protected String jobName;
    protected RemoteQueryClient remoteQueryClient;

    public SpliceBaseOperation(){
        super();
    }

    @SuppressFBWarnings(value = "UR_UNINIT_READ",justification = "Intentionally creates a Null BitDataField")
    public SpliceBaseOperation(OperationInformation information) throws StandardException{
        this.operationInformation=information;
        this.resultSetNumber=operationInformation.getResultSetNumber();
    }

    @SuppressFBWarnings(value = "UR_UNINIT_READ",justification = "Intentionally creates a Null BitDataField")
    public SpliceBaseOperation(Activation activation,
                               int resultSetNumber,
                               double optimizerEstimatedRowCount,
                               double optimizerEstimatedCost) throws StandardException{
        this.operationInformation=new DerbyOperationInformation(activation,optimizerEstimatedRowCount,optimizerEstimatedCost,resultSetNumber);
        this.activation=activation;
        this.resultSetNumber=resultSetNumber;
        this.optimizerEstimatedRowCount=optimizerEstimatedRowCount;
        this.optimizerEstimatedCost=optimizerEstimatedCost;
        if(activation.getLanguageConnectionContext().getStatementContext()==null){
            SpliceLogUtils.trace(LOG,"Cannot get StatementContext from Activation's lcc");
        }
    }

    public ExecutionFactory getExecutionFactory(){
        return activation.getExecutionFactory();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        this.optimizerEstimatedCost=in.readDouble();
        this.optimizerEstimatedRowCount=in.readDouble();
        this.operationInformation=(OperationInformation)in.readObject();
        isTopResultSet=in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SpliceLogUtils.trace(LOG,"writeExternal");
        out.writeDouble(optimizerEstimatedCost);
        out.writeDouble(optimizerEstimatedRowCount);
        out.writeObject(operationInformation);
        out.writeBoolean(isTopResultSet);
    }

    @Override
    public OperationInformation getOperationInformation(){
        return operationInformation;
    }

    @Override
    public SpliceOperation getLeftOperation(){
        return null;
    }

    @Override
    public int modifiedRowCount() {
        long modifiedRowCount = 0;
        long badRecords = 0;
        try {
            while (locatedRowIterator.hasNext()) {
                LocatedRow next = locatedRowIterator.next();
                ExecRow row = next.getRow();
                modifiedRowCount += row.getColumn(1).getLong();
                if (row.nColumns() > 1) {
                    badRecords += row.getColumn(2).getLong();
                    getActivation().getLanguageConnectionContext().setBadFile(row.getColumn(3).getString());
                }
            }
            if (modifiedRowCount > Integer.MAX_VALUE || modifiedRowCount < Integer.MIN_VALUE) {
                // DB-5369: int overflow when modified rowcount is larger than max int
                // Add modified row count as a long value in warning
                activation.addWarning(StandardException.newWarning(SQLState.LANG_MODIFIED_ROW_COUNT_TOO_LARGE, modifiedRowCount));
                return -1;
            }
            getActivation().getLanguageConnectionContext().setRecordsImported(modifiedRowCount);
            getActivation().getLanguageConnectionContext().setFailedRecords(badRecords);
            return (int) modifiedRowCount;
        } catch (StandardException se) {
            Exceptions.throwAsRuntime(PublicAPI.wrapStandardException(se));
            return 0; //never reached
        }
    }

    @Override
    public Activation getActivation(){
        return activation;
    }

    public String getPrettyExplainPlan(){
        return explainPlan;
    }

    public void setExplainPlan(String plan){
        // This is returned by getExplainPlan and getPrettyExplainPlan.
        // No difference. We can change that later if needed.
        // Right now this is only used by Spark UI, so don't change it
        // unless you want to change that UI.

        CompilerContext.DataSetProcessorType type = this.activation.getLanguageConnectionContext().getDataSetProcessorType();
        // JL-TODO Cannot do this in the hot path
        explainPlan=(plan==null?"":plan.replace("n=","RS=").replace("->","").trim());
    }

    @Override
    public void clearCurrentRow(){
        if(activation!=null){
            int resultSetNumber=operationInformation.getResultSetNumber();
            if(resultSetNumber!=-1)
                activation.clearCurrentRow(resultSetNumber);
        }
        currentRow=null;
    }

    @Override
    public void close() throws StandardException{
        try{
            if(LOG_CLOSE.isTraceEnabled())
                LOG_CLOSE.trace(String.format("closing operation %s",this));
            if (remoteQueryClient != null) {
                remoteQueryClient.close();
            }
            if(closeables!=null){
                for(AutoCloseable closeable : closeables){
                    closeable.close();
                }
                closeables=null;
            }
            clearCurrentRow();
            for(SpliceOperation op : getSubOperations())
                op.close();


		/* If this is the top ResultSet then we must
         * close all of the open subqueries for the
		 * entire query.
		 */
            if(isTopResultSet){

                LanguageConnectionContext lcc=getActivation().getLanguageConnectionContext();

                int staLength=(subqueryTrackingArray==null)?0:
                        subqueryTrackingArray.length;

                for(int index=0;index<staLength;index++){
                    if(subqueryTrackingArray[index]==null){
                        continue;
                    }
                    if(subqueryTrackingArray[index].isClosed()){
                        continue;
                    }
                    subqueryTrackingArray[index].close();
                }
            }
            isOpen=false;
            operationContext = null;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    //	@Override
    public void addWarning(SQLWarning w){
        activation.addWarning(w);
    }

    //	@Override
    public SQLWarning getWarnings(){
        return activation.getWarnings();
    }

    @Override
    public void markAsTopResultSet(){
        this.isTopResultSet=true;
    }

    @Override
    public void open() throws StandardException{
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("open operation %s",this));
        openCore();
    }

    //	@Override
    public double getEstimatedRowCount(){
        return operationInformation.getEstimatedRowCount();
    }

    @Override
    public int resultSetNumber(){
        return operationInformation.getResultSetNumber();
    }

    @Override
    public void setCurrentRow(ExecRow row){
        if(resultSetNumber!=-1){
            operationInformation.setCurrentRow(row);
        }
        currentRow=row;
    }

    // Debugging utility
    public ExecRow returning(ExecRow r){
        return returning(r,null);
    }

    public ExecRow returning(ExecRow r,String msg){
        LOG.error(String.format("%s %s returning %s%s",
                this.getClass().getSimpleName(),
                resultSetNumber,
                msg==null?"":msg+" ",
                r));
        return r;
    }

    public static void writeNullableString(String value,DataOutput out) throws IOException{
        if(value!=null){
            out.writeBoolean(true);
            out.writeUTF(value);
        }else{
            out.writeBoolean(false);
        }
    }

    public static String readNullableString(DataInput in) throws IOException{
        if(in.readBoolean())
            return in.readUTF();
        return null;
    }

    @Override
    public void init(SpliceOperationContext context) throws IOException, StandardException{
        this.activation=context.getActivation();
        this.operationInformation.initialize(context);
        this.resultSetNumber=operationInformation.getResultSetNumber();
    }

    protected ExecRow getFromResultDescription(ResultDescription resultDescription) throws StandardException{
        ExecRow row=new ValueRow(resultDescription.getColumnCount());
        for(int i=1;i<=resultDescription.getColumnCount();i++){
            ResultColumnDescriptor rcd=resultDescription.getColumnDescriptor(i);
            row.setColumn(i,rcd.getType().getNull());
        }
        return row;
    }

    @Override
    public ExecRow getExecRowDefinition() throws StandardException{
        throw new RuntimeException("No ExecRow Definition for this node "+this.getClass());
    }

    @Override
    public SpliceOperation getRightOperation(){
        return null;
    }

    public long getExecuteTime(){
        return getTimeSpent(ResultSet.ENTIRE_RESULTSET_TREE);
    }
    // Do we need to calculate this, ugh. -- TODO-JL
    public long getTimeSpent(int type){
        return 0l;
    }

    @Override
    public RowLocation getCurrentRowLocation(){
        return currentRowLocation;
    }

    @Override
    public void setCurrentRowLocation(RowLocation rowLocation){
        currentRowLocation=rowLocation;
    }

    public int getResultSetNumber(){
        return resultSetNumber;
    }

    public double getEstimatedCost(){
        return operationInformation.getEstimatedCost();
    }


    public void setActivation(Activation activation) throws StandardException{
        this.activation=activation;
    }

    public int[] getAccessedNonPkColumns() throws StandardException{
        // by default return null
        return null;
    }

    public String getScopeName() {
        return StringUtils.join(this.getClass().getSimpleName().replace("Operation","").split("(?=[A-Z])"), " ");
    }

    public void openCore(DataSetProcessor dsp) throws StandardException{
        try{
            if(LOG.isTraceEnabled())
                LOG.trace(String.format("openCore %s",this));
            isOpen=true;
            String sql=activation.getPreparedStatement().getSource();
            if (!(this instanceof ExplainOperation || activation.isMaterialized()))
                activation.materialize();
            long txnId=getCurrentTransaction().getTxnId();
            sql=sql==null?this.toString():sql;
            String userId=activation.getLanguageConnectionContext().getCurrentUserId(activation);
            if (dsp.getType() == DataSetProcessor.Type.SPARK) { // Only do this for spark jobs
                this.jobName = userId + " <" + txnId + ">";
                dsp.setJobGroup(jobName, sql);
            }
            dsp.clearBroadcastedOperation();
            this.locatedRowIterator=getDataSet(dsp).toLocalIterator();
        }catch(Exception e){ // This catches all the iterator errors for things that are not lazy.
            throw Exceptions.parseException(e);
        }
    }

    protected boolean isOlapServer() {
        return Thread.currentThread().currentThread().getName().startsWith("olap-worker");
    }

    @Override
    public void openCore() throws StandardException{
        DataSetProcessor dsp = EngineDriver.driver().processorFactory().chooseProcessor(activation,this);
        if (dsp.getType() == DataSetProcessor.Type.SPARK && !isOlapServer()) {
            remoteQueryClient = EngineDriver.driver().processorFactory().getRemoteQueryClient(this);
            remoteQueryClient.submit();
            locatedRowIterator = remoteQueryClient.getIterator();
        } else {
            openCore(dsp);
        }
    }

    @Override
    public void reopenCore() throws StandardException{
        if(LOG.isTraceEnabled())
            LOG.trace(String.format("reopenCore %s",this));
        openCore();
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException{
        try{
            if(locatedRowIterator.hasNext()){
                locatedRow=locatedRowIterator.next();
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG,"getNextRowCore %s locatedRow=%s",this,locatedRow);
                return locatedRow.getRow();
            }
            locatedRow=null;
            if(LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"getNextRowCore %s locatedRow=%s",this,locatedRow);
            return null;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int getPointOfAttachment(){
        return 0;
    }

    @Override
    public int getScanIsolationLevel(){
        return 0;
    }

    @Override
    public void setTargetResultSet(TargetResultSet targetResultSet){

    }

    @Override
    public void setNeedsRowLocation(boolean b){

    }

    @Override
    public boolean requiresRelocking(){
        return false;
    }

    @Override
    public boolean isForUpdate(){
        return false;
    }

    @Override
    public void updateRow(ExecRow execRow,RowChanger rowChanger) throws StandardException{
        // I suspect this is for cursors, might get interesting...
        throw new RuntimeException("Not Implemented");
    }

    @Override
    public void markRowAsDeleted() throws StandardException{

    }

    @Override
    public void positionScanAtRowLocation(RowLocation rowLocation) throws StandardException{

    }

    @Override
    public boolean returnsRows(){
        return !(this instanceof DMLWriteOperation || this instanceof CallStatementOperation
                || this instanceof MiscOperation);
    }

    @Override
    public ResultDescription getResultDescription(){
        return activation.getPreparedStatement().getResultDescription();
    }

    @Override
    public ExecRow getAbsoluteRow(int i) throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow getRelativeRow(int i) throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow setBeforeFirstRow() throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow getFirstRow() throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow getNextRow() throws StandardException{
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getNextRow");
        if(!isOpen)
            throw StandardException.newException(SQLState.LANG_RESULT_SET_NOT_OPEN,NEXT);
        attachStatementContext();
        return getNextRowCore();
    }

    @Override
    public ExecRow getPreviousRow() throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow getLastRow() throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public ExecRow setAfterLastRow() throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public boolean checkRowPosition(int i) throws StandardException{
        throw new RuntimeException("ScrollInsensitiveResultSet Should Handle this");
    }

    @Override
    public int getRowNumber(){
        return 0;
    }

    @Override
    public void cleanUp() throws StandardException{

    }

    @Override
    public boolean isClosed(){
        return !isOpen;
    }

    @Override
    public void finish() throws StandardException{

    }

    @Override
    public Timestamp getBeginExecutionTimestamp(){
        return null;
    }

    @Override
    public Timestamp getEndExecutionTimestamp(){
        return null;
    }

    @Override
    @SuppressFBWarnings(value = "EI_EXPOSE_REP",justification = "Intentional")
    public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries){
        SpliceLogUtils.trace(LOG,"getSubqueryTrackingArray with numSubqueries %d",numSubqueries);
        if(subqueryTrackingArray==null)
            subqueryTrackingArray=new NoPutResultSet[numSubqueries];
        return subqueryTrackingArray;
    }

    @Override
    public ResultSet getAutoGeneratedKeysResultset(){
        return null;
    }

    @Override
    public String getCursorName(){
        return activation.getCursorName();
    }

    @Override
    public boolean needsRowLocation(){
        return false;
    }

    @Override
    public void rowLocation(RowLocation rowLocation) throws StandardException{

    }

    @Override
    public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException{

        return new DataValueDescriptor[0];
    }

    @Override
    public boolean needsToClone(){
        return false;
    }

    @Override
    public FormatableBitSet getValidColumns(){
        return null;
    }

    @Override
    public void closeRowSource(){

    }

    @Override
    public TxnView getCurrentTransaction() throws StandardException{
        return getTransaction();
    }

    protected TxnView elevateTransaction() throws StandardException{
		/*
		 * Elevate the current transaction to make sure that we are writable
		 */
        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
        BaseSpliceTransaction rawTxn=(BaseSpliceTransaction)rawStoreXact;
        TxnView currentTxn = rawTxn.getActiveStateTxn();
        if(this instanceof DMLWriteOperation) {
            if (currentTxn instanceof ActiveWriteTxn)
                return rawTxn.getActiveStateTxn();
            else if (rawTxn instanceof  SpliceTransaction)
                return ((SpliceTransaction) rawTxn).elevate(((DMLWriteOperation) this).getDestinationTable());
            else
                throw new IllegalStateException("Programmer error: " + "cannot elevate transaction");
        }
        else
            throw new IllegalStateException("Programmer error: " +
                "attempting to elevate an operation txn without specifying a destination table");
    }

    private TxnView getTransaction() throws StandardException{
        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction)rawStoreXact).getActiveStateTxn();
    }

    @Override
    public void generateLeftOperationStack(List<SpliceOperation> operations){
        OperationUtils.generateLeftOperationStack(this,operations);
    }

    public List<SpliceOperation> getOperationStack(){
        if(leftOperationStack==null){
            leftOperationStack=new LinkedList<>();
            generateLeftOperationStack(leftOperationStack);
        }
        return leftOperationStack;
    }

    public void generateRightOperationStack(boolean initial,List<SpliceOperation> operations){
        SpliceLogUtils.trace(LOG,"generateRightOperationStack");
        SpliceOperation op;
        if(initial)
            op=getRightOperation();
        else
            op=getLeftOperation();

        if(op!=null){
            op.generateRightOperationStack(initial,operations);
        }
        operations.add(this);
    }

    /**
     * Attach this result set to the top statement context on the stack.
     * Result sets can be directly read from the JDBC layer. The JDBC layer
     * will push and pop a statement context around each ResultSet.getNext().
     * There's no guarantee that the statement context used for the last
     * getNext() will be the context used for the current getNext(). The
     * last statement context may have been popped off the stack and so
     * will not be available for cleanup if an error occurs. To make sure
     * that we will be cleaned up, we always attach ourselves to the top
     * context.
     * <p/>
     * The fun and games occur in nested contexts: using JDBC result sets inside
     * user code that is itself invoked from queries or CALL statements.
     *
     * @throws StandardException thrown if cursor finished.
     */
    protected void attachStatementContext() throws StandardException{
        if(isTopResultSet){
            if(statementContext==null || !statementContext.onStack()){
                statementContext=activation.getLanguageConnectionContext().getStatementContext();
            }
            statementContext.setTopResultSet(this,subqueryTrackingArray);
            // Pick up any materialized subqueries
            if(subqueryTrackingArray==null){
                subqueryTrackingArray=statementContext.getSubqueryTrackingArray();
            }
            statementContext.setActivation(activation);
        }
    }

    @Override
    public SpliceOperation getOperation(){
        return this;
    }

    @Override
    public RowLocation getRowLocation() throws StandardException{
        return this.currentRowLocation;
    }

    @Override
    public ExecRow getCurrentRow() throws StandardException{
        return this.currentRow;
    }

    @Override
    public void setCurrentLocatedRow(LocatedRow locatedRow){
        if(locatedRow!=null){
            setCurrentRow(locatedRow.getRow());
            setCurrentRowLocation(locatedRow.getRowLocation());
        }
    }

    @Override
    public Iterator<LocatedRow> getLocatedRowIterator(){
        return locatedRowIterator;
    }

    public void registerCloseable(AutoCloseable closeable) throws StandardException{
        if(closeables==null)
            closeables=new ArrayList<>(1);
        closeables.add(closeable);
    }

    @Override
    public void fireBeforeStatementTriggers() throws StandardException{
        // No Op
    }

    @Override
    public void fireAfterStatementTriggers() throws StandardException{
        // No Op
    }

    @Override
    public TriggerHandler getTriggerHandler() throws StandardException{
        return null;
    }

    @Override
    public ExecIndexRow getStartPosition() throws StandardException{
        throw new RuntimeException("getStartPosition not implemented");
    }

    @Override
    public OperationContext getOperationContext(){
        return operationContext;
    }

    @Override
    public void setOperationContext(OperationContext operationContext){
        this.operationContext=operationContext;
    }

    @Override
    public String getVTIFileName(){
        throw new RuntimeException("Not Supported");
    }

    protected void init() throws StandardException {
        try {
            init(SpliceOperationContext.newContext(activation));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public DataSet<LocatedRow> getResultDataSet(DataSetProcessor dsp) throws StandardException {
        return getDataSet(dsp);
    }
}
