package com.splicemachine.derby.iapi.sql.execute;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationTree;
import com.splicemachine.derby.impl.sql.execute.operations.OperationUtils;
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
        stmtInfo = new StatementInfo(sql, user, txnId,
                                                 OperationTree.getNumSinks(topOperation),
                                                 SpliceDriver.driver().getUUIDGenerator());
        List<OperationInfo> operationInfo = getOperationInfo(stmtInfo.getStatementUuid());
        stmtInfo.setOperationInfo(operationInfo);
        topOperation.setStatementId(stmtInfo.getStatementUuid());
        SpliceDriver.driver().getStatementManager().addStatementInfo(stmtInfo);
        return stmtInfo;
    }

		public SpliceNoPutResultSet getDelegate(){
				return delegate;
		}

		public void open(boolean useProbe) throws StandardException{
				SpliceLogUtils.trace(LOG,"openCore");
				closed=false;
				if(delegate!=null) delegate.close();
				try {
						SpliceOperationContext operationContext = SpliceOperationContext.newContext(activation);
						topOperation.init(operationContext);
						topOperation.open();
						statementInfo = initStatmentInfo(statementInfo, operationContext);

				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}

				try{
						SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext();
						runtimeContext.setStatementInfo(statementInfo);
						if(activation.getLanguageConnectionContext().getStatisticsTiming()){
								runtimeContext.recordTraceMetrics();
								String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
								runtimeContext.setXplainSchema(xplainSchema);
						}

						delegate = OperationTree.executeTree(topOperation,runtimeContext,useProbe);
						delegate.setScrollId(scrollUuid);
						//open the delegate
						delegate.openCore();
				}catch(RuntimeException e){
						throw Exceptions.parseException(e);
				}

				if(PLAN_LOG.isDebugEnabled() && Boolean.valueOf(System.getProperty("derby.language.logQueryPlan"))){
						PLAN_LOG.debug(topOperation.prettyPrint(1));
				}

		}
    @Override
    public void openCore() throws StandardException {
				open(false);
    }

		private List<OperationInfo> getOperationInfo(long statementId) {
				List<OperationInfo> info = Lists.newArrayList();
				if(!(topOperation instanceof DMLWriteOperation)){
					/*
					 * We add an extra OperationInfo on the top here to indicate that there is
					 * a "Scroll Insensitive" which returns the final output.
					 */
						scrollUuid = SpliceDriver.driver().getUUIDGenerator().nextUUID();
						OperationInfo opInfo = new OperationInfo(scrollUuid,statementId,"ScrollInsensitive",false,-1l);
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
				OperationInfo opInfo = new OperationInfo(operationUuid,statementId, operation.getName(),isRight,parentOperationId);
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
        checkDelegate();
        return delegate.getEstimatedRowCount();
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
						String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
						boolean explain = xplainSchema !=null &&
										activation.getLanguageConnectionContext().getRunTimeStatisticsMode();
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
