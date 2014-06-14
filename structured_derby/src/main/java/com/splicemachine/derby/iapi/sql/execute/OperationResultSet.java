package com.splicemachine.derby.iapi.sql.execute;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.sql.execute.operations.ExplainOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationTree;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.*;
import org.apache.derby.iapi.tools.run;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Delegating ResultSet to ensure that operation stacks are re-run when Derby
 * re-uses an Operation.
 *
 * This is necessary to ensure that the OperationTree is re-traversed when the Activation changes.
 * When using PreparedStatements, Derby will construct a ResultSet entity only once, and then
 * repeatedly call open() on it each time the activation changes and it needs to be re-executed. This
 * means that each time open() is called, we must assume that the Activation has changed, and re-execute
 * the operation stack. This class wraps out that behavior.
 *
 * This class is <em>not</em> thread-safe, and should <em>never</em> be used by more than one thread
 * simultaneously.
 *
 * @author Scott Fines
 * Created on: 3/28/13
 */
public class OperationResultSet implements NoPutResultSet,HasIncrement,CursorResultSet,ConvertedResultSet {
    private static final Logger LOG = Logger.getLogger(OperationResultSet.class);
    private static Logger PLAN_LOG = Logger.getLogger("com.splicemachine.queryPlan");
    private Activation activation;
    private SpliceOperation topOperation;
    private SpliceNoPutResultSet delegate;
    private boolean closed = false;
    private long parentOperationID = -1l;
    private long statementId;
    private SpliceBaseOperation.XplainOperationChainInfo operationChainInfo;

		private StatementInfo statementInfo;
		private long scrollUuid;

		public OperationResultSet() {
			// no-op
		}

		
		public OperationResultSet(Activation activation,
                              SpliceOperation topOperation){
        this.activation = activation;
        this.topOperation = topOperation;
    }

    public void setParentOperationID(byte[] parentOperationID) {
        this.parentOperationID = Bytes.toLong(parentOperationID);
    }

    public void setStatementId(long statementId) {
        this.statementId = statementId;
    }

    public SpliceOperation getTopOperation() {
        return topOperation;
    }

    @Override
    public void markAsTopResultSet() {
        SpliceLogUtils.trace(LOG, "markAsTopResultSet");
        topOperation.markAsTopResultSet();
    }

    private StatementInfo initStatmentInfo(StatementInfo stmtInfo, SpliceOperationContext opCtx) {
        if (stmtInfo != null){
            // if statementInfo already created for this ResultSet, don't create again nor add
            // to StatementManager
            return stmtInfo;
        }
        String sql = opCtx.getPreparedStatement().getSource();
        String user = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String txnId = activation.getTransactionController().getActiveStateTxIdString();
        if (stmtInfo == null) {
            if (parentOperationID == -1) {
                stmtInfo = new StatementInfo(sql, user, txnId,
                        OperationTree.getNumSinks(topOperation),
                        SpliceDriver.driver().getUUIDGenerator());
            }
            else {
                stmtInfo = new StatementInfo(sql, user, txnId,
                        OperationTree.getNumSinks(topOperation),
                        statementId);
            }
        }
        List<OperationInfo> operationInfo = getOperationInfo(stmtInfo.getStatementUuid());
        stmtInfo.setOperationInfo(operationInfo);
        topOperation.setStatementId(stmtInfo.getStatementUuid());
        SpliceDriver.driver().getStatementManager().addStatementInfo(stmtInfo);

        return stmtInfo;
    }

		public SpliceNoPutResultSet getDelegate(){
				return delegate;
		}

		public void open(boolean useProbe) throws StandardException, IOException{
            if (topOperation instanceof ExplainOperation) {
                open(useProbe, false);
            }
            else {
                open(useProbe, true);
            }
		}

    public void executeScan(boolean useProbe, SpliceRuntimeContext context) throws StandardException {
        try{
            delegate = useProbe? topOperation.executeProbeScan(): topOperation.executeScan(context);
            delegate.setScrollId(scrollUuid);
            delegate.openCore();
        }catch(RuntimeException re){
            throw Exceptions.parseException(re);
        }
        if(PLAN_LOG.isDebugEnabled() && Boolean.valueOf(System.getProperty("derby.language.logQueryPlan"))){
            PLAN_LOG.debug(topOperation.prettyPrint(1));
        }
    }

    public SpliceRuntimeContext sinkOpen(boolean useProbe, boolean showStatementInfo) throws StandardException, IOException {
        SpliceLogUtils.trace(LOG,"openCore");
        closed=false;
        if(delegate!=null) delegate.close();
        SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext();

        try {
            SpliceOperationContext operationContext = SpliceOperationContext.newContext(activation);
            topOperation.init(operationContext);
            topOperation.open();
            if(showStatementInfo)
                statementInfo = initStatmentInfo(statementInfo, operationContext);

        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        try{

            if(showStatementInfo)
                runtimeContext.setStatementInfo(statementInfo);
            if(activation.getLanguageConnectionContext().getStatisticsTiming()){
                runtimeContext.recordTraceMetrics();
                String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
                runtimeContext.setXplainSchema(xplainSchema);
            }

            List<SpliceBaseOperation.XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
            if (operationChain != null && operationChain.size() > 0) {
                operationChainInfo = operationChain.get(operationChain.size()-1);
                runtimeContext.recordTraceMetrics();
                runtimeContext.setXplainSchema(operationChainInfo.getXplainSchema());
                parentOperationID = operationChainInfo.getOperationId();
                statementId = operationChainInfo.getStatementId();
            }

            List<byte[]> taskChain = OperationSink.taskChain.get();
            if (taskChain != null && taskChain.size() > 0){
                runtimeContext.setParentTaskId(taskChain.get(taskChain.size() - 1));
            }

            OperationTree.sink(topOperation, runtimeContext);
            return runtimeContext;
        }catch(RuntimeException e){
            throw Exceptions.parseException(e);
        }
    }

		public void open(boolean useProbe,boolean showStatementInfo) throws StandardException, IOException {
        SpliceRuntimeContext ctx = sinkOpen(useProbe,showStatementInfo);
        executeScan(useProbe,ctx);
		}

    @Override
    public void openCore() throws StandardException {
        try {
            open(false);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

		private List<OperationInfo> getOperationInfo(long statementId) {
				List<OperationInfo> info = Lists.newArrayList();
				if(!(topOperation instanceof DMLWriteOperation)){
					/*
					 * We add an extra OperationInfo on the top here to indicate that there is
					 * a "Scroll Insensitive" which returns the final output.
					 */
                        String m = operationChainInfo==null ? null : operationChainInfo.getMethodName();
						scrollUuid = SpliceDriver.driver().getUUIDGenerator().nextUUID();
						OperationInfo opInfo = new OperationInfo(scrollUuid,statementId,"ScrollInsensitive",
                                m, false,parentOperationID);
						info.add(opInfo);
				}else{
						scrollUuid = -1l;
				}

				populateOpInfo(statementId,scrollUuid,false,topOperation,info);
				return info;
		}

		private void populateOpInfo(long statementId,long parentOperationId,boolean isRight,SpliceOperation operation, List<OperationInfo> infos) {
				if(operation==null) return;
				long operationUuid = Bytes.toLong(operation.getUniqueSequenceID());
				OperationInfo opInfo = new OperationInfo(operationUuid,statementId, operation.getName(), operation.getInfo(), isRight,parentOperationId);
				infos.add(opInfo);
				populateOpInfo(statementId,operationUuid, false, operation.getLeftOperation(), infos);
				populateOpInfo(statementId,operationUuid,true,operation.getRightOperation(),infos);
		}

		@Override
    public void reopenCore() throws StandardException {
        openCore();
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        checkDelegate();
        return delegate.getNextRowCore();
    }

    @Override
    public int getPointOfAttachment() {
        checkDelegate();
        return delegate.getPointOfAttachment();
    }

    @Override
    public int getScanIsolationLevel() {
        checkDelegate();
        return delegate.getScanIsolationLevel();
    }

    @Override
    public void setTargetResultSet(TargetResultSet trs) {
        checkDelegate();
        delegate.setTargetResultSet(trs);
    }

    @Override
    public void setNeedsRowLocation(boolean needsRowLocation) {
        checkDelegate();
        delegate.setNeedsRowLocation(needsRowLocation);
    }

    @Override
    public double getEstimatedRowCount() {
        return topOperation.getOperationInformation().getEstimatedRowCount();
    }

    @Override
    public int resultSetNumber() {
        checkDelegate();
        return delegate.resultSetNumber();
    }

    @Override
    public void setCurrentRow(ExecRow row) {
        checkDelegate();
        delegate.setCurrentRow(row);
    }

    @Override
    public boolean requiresRelocking() {
        checkDelegate();
        return delegate.requiresRelocking();
    }

    @Override
    public boolean isForUpdate() {
        checkDelegate();
        return delegate.isForUpdate();
    }

    @Override
    public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException {
        checkDelegate();
        delegate.updateRow(row, rowChanger);
    }

    @Override
    public void markRowAsDeleted() throws StandardException {
        checkDelegate();
        delegate.markRowAsDeleted();
    }

    @Override
    public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException {
        checkDelegate();
        delegate.positionScanAtRowLocation(rLoc);
    }

    @Override
    public boolean returnsRows() {
        return delegate != null && delegate.returnsRows();
    }

    @Override
    public int modifiedRowCount() {
        checkDelegate();
        return delegate.modifiedRowCount();
    }

    @Override
    public ResultDescription getResultDescription() {
        checkDelegate();
        return delegate.getResultDescription();
    }

    @Override
    public Activation getActivation() {
        return activation;
    }

    @Override
    public void open() throws StandardException {
        openCore();
    }

    @Override
    public ExecRow getAbsoluteRow(int row) throws StandardException {
        checkDelegate();
        return delegate.getAbsoluteRow(row);
    }

    @Override
    public ExecRow getRelativeRow(int row) throws StandardException {
        checkDelegate();
        return delegate.getRelativeRow(row);
    }

    @Override
    public ExecRow setBeforeFirstRow() throws StandardException {
        checkDelegate();
        return delegate.setBeforeFirstRow();
    }

    @Override
    public ExecRow getFirstRow() throws StandardException {
        checkDelegate();
        return delegate.getFirstRow();
    }

    @Override
    public ExecRow getNextRow() throws StandardException {
        checkDelegate();
        return delegate.getNextRow();
    }

    @Override
    public ExecRow getPreviousRow() throws StandardException {
        checkDelegate();
        return delegate.getPreviousRow();
    }

    @Override
    public ExecRow getLastRow() throws StandardException {
        checkDelegate();
        return delegate.getLastRow();
    }

    @Override
    public ExecRow setAfterLastRow() throws StandardException {
        checkDelegate();
        return delegate.setAfterLastRow();
    }

    @Override
    public void clearCurrentRow() {
        checkDelegate();
        delegate.clearCurrentRow();
    }

    @Override
    public boolean checkRowPosition(int isType) throws StandardException {
        checkDelegate();
        return delegate.checkRowPosition(isType);
    }

    @Override
    public int getRowNumber() {
        checkDelegate();
        return delegate.getRowNumber();
    }

    @Override
    public void close() throws StandardException {
				if(statementInfo!=null){
						statementInfo.markCompleted();
                        String xplainSchema = null;
                        if (operationChainInfo != null) {
                            xplainSchema = operationChainInfo.getXplainSchema();
                        }
                        else {
                            xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
                        }

                        boolean explain = xplainSchema !=null &&
                                (activation.getLanguageConnectionContext().getRunTimeStatisticsMode() ||
                                        topOperation.shouldRecordStats());
                        SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,
                                explain? xplainSchema: null);
						statementInfo = null; //remove the field in case we call close twice
				}
        if(delegate!=null)delegate.close();
        closed=true;
    }

    @Override
    public void cleanUp() throws StandardException {
        checkDelegate();
        delegate.cleanUp();
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public void finish() throws StandardException {
        if (delegate != null) {
            delegate.finish();
        }
    }

    @Override
    public long getExecuteTime() {
        checkDelegate();
        return delegate.getExecuteTime();
    }

    @Override
    public Timestamp getBeginExecutionTimestamp() {
        checkDelegate();
        return delegate.getBeginExecutionTimestamp();
    }

    @Override
    public Timestamp getEndExecutionTimestamp() {
        checkDelegate();
        return delegate.getEndExecutionTimestamp();
    }

    @Override
    public long getTimeSpent(int type) {
        checkDelegate();
        return delegate.getTimeSpent(type);
    }

    @Override
    public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
        checkDelegate();
        return delegate.getSubqueryTrackingArray(numSubqueries);
    }

    @Override
    public ResultSet getAutoGeneratedKeysResultset() {
        checkDelegate();
        return delegate.getAutoGeneratedKeysResultset();
    }

    @Override
    public String getCursorName() {
        checkDelegate();
        return delegate.getCursorName();
    }

    @Override
    public void addWarning(SQLWarning w) {
        activation.addWarning(w);
    }

    @Override
    public SQLWarning getWarnings() {
        checkDelegate();
        return delegate.getWarnings();
    }

    @Override
    public boolean needsRowLocation() {
        checkDelegate();
        return delegate.needsRowLocation();
    }

    @Override
    public void rowLocation(RowLocation rl) throws StandardException {
        checkDelegate();
        delegate.rowLocation(rl);
    }

    @Override
    public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException {
        checkDelegate();
        return delegate.getNextRowFromRowSource();
    }

    @Override
    public boolean needsToClone() {
        checkDelegate();
        return delegate.needsToClone();
    }

    @Override
    public FormatableBitSet getValidColumns() {
        checkDelegate();
        return delegate.getValidColumns();
    }

    @Override
    public void closeRowSource() {
        checkDelegate();
        delegate.closeRowSource();
    }

    @Override
    public DataValueDescriptor increment(int columnPosition, long increment) throws StandardException {
        if(!(topOperation instanceof HasIncrement))
            throw StandardException.newException(SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
        return ((HasIncrement)topOperation).increment(columnPosition,increment);
    }

/*********************************************************************************************************************/
    /*private helper methods*/

    private void checkDelegate() {
        Preconditions.checkNotNull(delegate,
                "No Delegate Result Set provided, please ensure open() or openCore() was called");
    }


    @Override
    public RowLocation getRowLocation() throws StandardException {
        if(delegate instanceof CursorResultSet)
            return ((CursorResultSet)delegate).getRowLocation();
        return null;
    }

    @Override
    public ExecRow getCurrentRow() throws StandardException {
        if(delegate instanceof CursorResultSet)
            return ((CursorResultSet)delegate).getCurrentRow();
        return null;
    }

    @Override
    public SpliceOperation getOperation() {
        return topOperation;
    }

		public StatementInfo getStatementInfo(){
				return statementInfo;
		}
	public void setActivation(Activation activation) throws StandardException {
		this.activation = activation;
		topOperation.setActivation(activation);
	}

}
