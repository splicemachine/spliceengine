package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.Orderable;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.OperationInformation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

public abstract class SpliceBaseOperation implements SpliceOperation, Externalizable{
    private static final long serialVersionUID=4l;
    private static Logger LOG=Logger.getLogger(SpliceBaseOperation.class);
    private static Logger LOG_CLOSE=Logger.getLogger(SpliceBaseOperation.class.getName()+".close");
    protected static final SDataLib dataLib=SIDriver.driver().getDataLib();
    protected static final DerbyFactory derbyFactory=DerbyFactoryDriver.derbyFactory;
    /* Run time statistics variables */
    public int numOpens;
    public long beginTime;
    public long constructorTime;
    public long openTime;
    public long nextTime;
    public long closeTime;
    protected boolean statisticsTimingOn;
    protected Iterator<LocatedRow> locatedRowIterator;
    protected Activation activation;
    protected String explainPlan="";
    protected Timer timer;
    protected long stopExecutionTime;
    protected double optimizerEstimatedRowCount;
    protected double optimizerEstimatedCost;
    protected String info;
    protected boolean isTopResultSet=false;
    protected volatile byte[] uniqueSequenceID;
    protected ExecRow currentRow;
    protected RowLocation currentRowLocation;
    protected boolean executed=false;
    protected OperationContext operationContext;
    protected DataValueDescriptor[] sequence;
    protected boolean isOpen=true;
    protected int resultSetNumber;
    protected OperationInformation operationInformation;
    protected long statementId=-1l; //default value if the statementId isn't set
    protected LocatedRow locatedRow;
    protected StatementContext statementContext;
    protected List<AutoCloseable> closeables;
    protected NoPutResultSet[] subqueryTrackingArray;
    protected List<SpliceOperation> leftOperationStack;

    public SpliceBaseOperation(){
        super();
    }

    public SpliceBaseOperation(OperationInformation information) throws StandardException{
        this.operationInformation=information;
        this.resultSetNumber=operationInformation.getResultSetNumber();
        sequence=new DataValueDescriptor[1];
        sequence[0]=information.getSequenceField(uniqueSequenceID);
    }

    public SpliceBaseOperation(Activation activation,
                               int resultSetNumber,
                               double optimizerEstimatedRowCount,
                               double optimizerEstimatedCost) throws StandardException{
        this.operationInformation=new DerbyOperationInformation(activation,optimizerEstimatedRowCount,optimizerEstimatedCost,resultSetNumber);
        this.activation=activation;
        this.resultSetNumber=resultSetNumber;
        this.optimizerEstimatedRowCount=optimizerEstimatedRowCount;
        this.optimizerEstimatedCost=optimizerEstimatedCost;
        sequence=new DataValueDescriptor[1];
        sequence[0]=operationInformation.getSequenceField(uniqueSequenceID);
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
        if(in.readBoolean()){
            uniqueSequenceID=new byte[in.readInt()];
            in.readFully(uniqueSequenceID);
        }
        statisticsTimingOn=in.readBoolean();
        statementId=in.readLong();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        SpliceLogUtils.trace(LOG,"writeExternal");
        out.writeDouble(optimizerEstimatedCost);
        out.writeDouble(optimizerEstimatedRowCount);
        out.writeObject(operationInformation);
        out.writeBoolean(isTopResultSet);
        out.writeBoolean(uniqueSequenceID!=null);
        if(uniqueSequenceID!=null){
            out.writeInt(uniqueSequenceID.length);
            out.write(uniqueSequenceID);
        }
        out.writeBoolean(statisticsTimingOn);
        out.writeLong(statementId);

    }

    @Override
    public long getStatementId(){
        return statementId;
    }

    @Override
    public void setStatementId(long statementId){
        this.statementId=statementId;
        SpliceOperation sub=getLeftOperation();
        if(sub!=null) sub.setStatementId(statementId);
        sub=getRightOperation();
        if(sub!=null) sub.setStatementId(statementId);
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
    public int modifiedRowCount(){
        try{
            int modifiedRowCount=0;
            while(locatedRowIterator.hasNext())
                modifiedRowCount+=locatedRowIterator.next().getRow().getColumn(1).getInt();
            return modifiedRowCount;
        }catch(StandardException se){
            throw new RuntimeException(se);
        }
    }

    @Override
    public Activation getActivation(){
        return activation;
    }

    public String getExplainPlan(){
        return explainPlan;
    }

    public String getPrettyExplainPlan(){
        return explainPlan;
    }

    public void setExplainPlan(String plan){
        // This is returned by getExplainPlan and getPrettyExplainPlan.
        // No difference. We can change that later if needed.
        // Right now this is only used by Spark UI, so don't change it
        // unless you want to change that UI.
        explainPlan=(plan==null?"":plan.replace("n=","RS=").replace("->","").trim());
        ;
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
        this.uniqueSequenceID=operationInformation.getUUIDGenerator().nextBytes();
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
        this.uniqueSequenceID=operationInformation.getUUIDGenerator().nextBytes();
        sequence=new DataValueDescriptor[1];
        sequence[0]=operationInformation.getSequenceField(uniqueSequenceID);
    }

    @Override
    public byte[] getUniqueSequenceID(){
        return uniqueSequenceID;
    }


    public RecordingCallBuffer<KVPair> transformWriteBuffer(RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException{
        return bufferToTransform;
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

    protected final long getCurrentTimeMillis(){
        if(statisticsTimingOn)
            return System.currentTimeMillis();
        else
            return 0;
    }

    protected final long getElapsedMillis(long beginTime){
        if(statisticsTimingOn)
            return (System.currentTimeMillis()-beginTime);
        else
            return 0;
    }

    protected final void recordConstructorTime(){
        if(statisticsTimingOn)
            constructorTime=getElapsedMillis(beginTime);
    }

    public long getTimeSpent(int type){
        return constructorTime+openTime+nextTime+closeTime;
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

    public String getInfo(){
        return info;
    }

    private String sparkStageName=null;

    public String getSparkStageName(){
        if(sparkStageName==null){
            String[] words=this.getClass().getSimpleName().replace("Operation","").split("(?=[A-Z])");
            sparkStageName=StringUtils.join(words," ");
        }
        return sparkStageName;
    }

    public DataSet<LocatedRow> getDataSet() throws StandardException{
        DataSetProcessor dsp=StreamUtils.getDataSetProcessorFromActivation(activation,this);
        return getDataSet(dsp);
    }

    public void openCore(DataSetProcessor dsp) throws StandardException{
        try{
            if(LOG.isTraceEnabled())
                LOG.trace(String.format("openCore %s",this));
            isOpen=true;
            String sql=activation.getPreparedStatement().getSource();
            long txnId=getCurrentTransaction().getTxnId();
            sql=sql==null?this.toString():sql;
            String userId=activation.getLanguageConnectionContext().getCurrentUserId(activation);
            String jobName=userId+" <"+txnId+">";
            dsp.setJobGroup(jobName,sql);
            this.locatedRowIterator=getDataSet(dsp).toLocalIterator();
        }catch(Exception e){ // This catches all the iterator errors for things that are not lazy.
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void openCore() throws StandardException{
        openCore(StreamUtils.getDataSetProcessorFromActivation(activation,this));
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

    public TxnView getCurrentTransaction() throws StandardException{
        if(this instanceof DMLWriteOperation){
            return elevateTransaction();
        }else
            return getTransaction();
    }

    private TxnView elevateTransaction() throws StandardException{
				/*
				 * Elevate the current transaction to make sure that we are writable
				 */
        TransactionController transactionExecute=activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact=((TransactionManager)transactionExecute).getRawStoreXact();
        BaseSpliceTransaction rawTxn=(BaseSpliceTransaction)rawStoreXact;
        if(this instanceof DMLWriteOperation)
            return ((SpliceTransaction)rawTxn).elevate(((DMLWriteOperation)this).getDestinationTable());
        else
            throw new IllegalStateException("Programmer error: "+
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

    protected List<SpliceOperation> getOperationStack(){
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
//            operations.add(op);
        }
        operations.add(this);
    }

    public void generateAllOperationStack(List<SpliceOperation> operations){
        OperationUtils.generateAllOperationStack(this,operations);
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
}
