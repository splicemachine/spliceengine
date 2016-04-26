package com.splicemachine.derby.iapi.sql.execute;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.*;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;

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
public class OperationResultSet implements NoPutResultSet, HasIncrement, CursorResultSet, ConvertedResultSet {
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
    private long scrollUuid = -1l;
    private TxnView txn;

    public OperationResultSet() {
        // no-op
    }


    public OperationResultSet(Activation activation,
                              SpliceOperation topOperation) {
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

    private StatementInfo initStatmentInfo(TxnView txn, StatementInfo stmtInfo, SpliceOperationContext opCtx) throws StandardException {
        if (stmtInfo != null) {
            // if statementInfo already created for this ResultSet, don't create again nor add
            // to StatementManager
            return stmtInfo;
        }
        String sql = opCtx.getPreparedStatement().getSource();
        String user = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        if (parentOperationID == -1) {
            Snowflake snowflake = SpliceDriver.driver().getUUIDGenerator();
            long sId = snowflake.nextUUID();
            if (activation.isTraced()) {
                activation.getLanguageConnectionContext().setXplainStatementId(sId);
            }
            stmtInfo = new StatementInfo(sql, user, txn,
                    OperationTree.getNumSinks(topOperation),
                    sId);
        } else {
            stmtInfo = new StatementInfo(sql, user, txn,
                    OperationTree.getNumSinks(topOperation),
                    statementId);
        }
        List<OperationInfo> operationInfo = getOperationInfo(stmtInfo.getStatementUuid());
        stmtInfo.setOperationInfo(operationInfo);
        topOperation.setStatementId(stmtInfo.getStatementUuid());

        if(sql!=null && !sql.equals("null")) //don't display statement info if we aren't actually a statement
            SpliceDriver.driver().getStatementManager().addStatementInfo(stmtInfo);

        return stmtInfo;
    }

    public SpliceNoPutResultSet getDelegate() {
        return delegate;
    }

    public void open(boolean useProbe) throws StandardException, IOException {
        boolean showStatementInfo = !(topOperation instanceof ExplainOperation || topOperation instanceof ExportOperation);
        open(useProbe, showStatementInfo);
    }

    public void executeScan(boolean useProbe, SpliceRuntimeContext context) throws StandardException {
        try {
            if (context.useSpark()) {
                OperationInformation info = topOperation.getOperationInformation();
                Activation activation = topOperation.getActivation();
                int resultSetNumber = info.getResultSetNumber();
                StatementInfo statementInfo = context.getStatementInfo();
                long txnId = ((SpliceTransactionManager) activation.getTransactionController()).getRawTransaction().getTxnInformation().getTxnId();
                String jobName = topOperation.getName() + " rs " + resultSetNumber + " <" + txnId + ">";
                String jobDescription = statementInfo != null ? statementInfo.getSql() : null;
                SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
                delegate = topOperation.executeRDD(context);
            } else {
                delegate = useProbe ? topOperation.executeProbeScan() : topOperation.executeScan(context);
            }
            delegate.setScrollId(scrollUuid);
            delegate.openCore();
        } catch (RuntimeException re) {
            throw Exceptions.parseException(re);
        }
        if (PLAN_LOG.isDebugEnabled() && Boolean.valueOf(System.getProperty("derby.language.logQueryPlan"))) {
            PLAN_LOG.debug(topOperation.prettyPrint(1));
        }
    }

    public SpliceRuntimeContext sinkOpen(TxnView txn, boolean showStatementInfo) throws StandardException, IOException {
        this.txn = txn;
        SpliceLogUtils.trace(LOG, "openCore");
        closed = false;
        if (delegate != null) delegate.close();
        SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext(txn);

        try {
            List<SpliceBaseOperation.XplainOperationChainInfo> operationChain = SpliceBaseOperation.operationChain.get();
            if (operationChain != null && operationChain.size() > 0) {
                operationChainInfo = operationChain.get(operationChain.size() - 1);
                runtimeContext.recordTraceMetrics();
                if (parentOperationID == -1l)
                    parentOperationID = operationChainInfo.getOperationId();
                statementId = operationChainInfo.getStatementId();
            }

            SpliceOperationContext operationContext = SpliceOperationContext.newContext(activation, txn);
            topOperation.init(operationContext);
            topOperation.open();
            if (showStatementInfo) {
                statementInfo = initStatmentInfo(txn, statementInfo, operationContext);
//                if(activation.isTraced()){
//                    SQLWarning w = StandardException.newWarning(WarningState.XPLAIN_STATEMENT_ID.getSqlState(), statementInfo.getStatementUuid());
//                    ((GenericStorablePreparedStatement)activation.getPreparedStatement()).addWarning(w);
//                }
            }
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        try {

            if (showStatementInfo)
                runtimeContext.setStatementInfo(statementInfo);
            if (activation.isTraced()) {
                runtimeContext.recordTraceMetrics();
            }

            LinkedList<byte[]> taskChain = TaskIdStack.getCurrentThreadTaskIdList();
            if (taskChain != null && !taskChain.isEmpty()) {
                runtimeContext.setParentTaskId(taskChain.getLast());
            }

            // enable Spark if the whole stack supports it
            if (topOperation.expectsRDD() && !topOperation.pushedToServer()) {
                runtimeContext.setUseSpark(true);
            } else {
                OperationTree.sink(topOperation, runtimeContext);
            }

            return runtimeContext;
        } catch (RuntimeException e) {
            throw Exceptions.parseException(e);
        }
    }

    public SpliceRuntimeContext sinkOpen(boolean useProbe, boolean showStatementInfo) throws StandardException, IOException {
        TxnView t;
        if (topOperation instanceof DMLWriteOperation || activation.isTraced()) {
            //elevate the transaction
            t = elevateTransaction();
        } else
            t = getTransaction();

        return sinkOpen(t, showStatementInfo);
    }

    public void open(boolean useProbe, boolean showStatementInfo) throws StandardException, IOException {
        SpliceRuntimeContext ctx = sinkOpen(useProbe, showStatementInfo);
        executeScan(useProbe, ctx);
    }

    @Override
    public void openCore() throws StandardException {
        try {
            open(false);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    public void setParentOperationID(long operationId) {
        this.parentOperationID = operationId;
    }

    private List<OperationInfo> getOperationInfo(long statementId) {
        List<OperationInfo> info = Lists.newArrayList();
        SpliceOperation top = topOperation;
        if (!(top instanceof DMLWriteOperation || top instanceof MiscOperation)) {
            /*
             * We add an extra OperationInfo on the top here to indicate that there is
             * a "Scroll Insensitive" which returns the final output.
             */
            String m = operationChainInfo == null ? null : operationChainInfo.getMethodName();

            scrollUuid = SpliceDriver.driver().getUUIDGenerator().nextUUID();
            OperationInfo opInfo = new OperationInfo(scrollUuid, statementId, "ScrollInsensitive",
                    m, false, parentOperationID);
            info.add(opInfo);
        } else if (top instanceof MiscOperation) {
            top = topOperation.getLeftOperation();
            scrollUuid = -1l;
        } else {
            scrollUuid = -1l;
        }
        populateOpInfo(statementId, scrollUuid, false, top, info);
        return info;
    }


    private void populateOpInfo(long statementId, long parentOperationId, boolean isRight, SpliceOperation operation, List<OperationInfo> infos) {
        if (operation == null) return;
        long operationUuid = Bytes.toLong(operation.getUniqueSequenceID());
        OperationInfo opInfo = new OperationInfo(operationUuid, statementId, operation.getName(), operation.getInfo(), isRight, parentOperationId);
        infos.add(opInfo);
        populateOpInfo(statementId, operationUuid, false, operation.getLeftOperation(), infos);
        /*
         * Most joins will record their information during the scan phase, and are not themselves
         * sinks. However, MergeSortJoin will need to record its right hand side data directly here
         */
        //if(operation.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE))
        populateOpInfo(statementId, operationUuid, true, operation.getRightOperation(), infos);
    }

    @Override
    public void reopenCore() throws StandardException {
        try {
            open(false);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
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
        try {
            openCore();
        } catch (RuntimeException re) {
            throw Exceptions.parseException(re);
        }
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
        if (statementInfo != null) {
            statementInfo.markCompleted();
            try {
                TxnView recordTxn = txn != null ? txn : getTransaction();
                SpliceDriver.driver().getStatementManager().completedStatement(statementInfo, activation.isTraced(), recordTxn);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
            statementInfo = null; //remove the field in case we call close twice
            ((GenericStorablePreparedStatement) activation.getPreparedStatement()).clearWarnings();
        }
        if (delegate != null) {
            try {
                delegate.close();
            } finally {
                closed = true;
            }
        }
        closed = true;
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
        if (!(topOperation instanceof HasIncrement))
            throw StandardException.newException(SQLState.STORE_FEATURE_NOT_IMPLEMENTED);
        return ((HasIncrement) topOperation).increment(columnPosition, increment);
    }

    @Override
    public RowLocation getRowLocation() throws StandardException {
        if (delegate != null) return delegate.getRowLocation();
        return null;
    }

    @Override
    public ExecRow getCurrentRow() throws StandardException {
        if (delegate != null) return delegate.getCurrentRow();
        return null;
    }

    @Override
    public SpliceOperation getOperation() {
        return topOperation;
    }

    public StatementInfo getStatementInfo() {
        return statementInfo;
    }

    public void setActivation(Activation activation) throws StandardException {
        this.activation = activation;
        topOperation.setActivation(activation);
    }

    /*********************************************************************************************************************/
    /*private helper methods*/
    private void checkDelegate() {
        Preconditions.checkNotNull(delegate,
                "No Delegate Result Set provided, please ensure open() or openCore() was called");
    }

    private TxnView elevateTransaction() throws StandardException {
        /*
         * Elevate the current transaction to make sure that we are writable
         */
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        BaseSpliceTransaction rawTxn = (BaseSpliceTransaction) rawStoreXact;
        TxnView currentTxn = rawTxn.getActiveStateTxn();
        if (!currentTxn.allowsWrites()) {
            SpliceTransaction spliceRawTxn = (SpliceTransaction) rawTxn;
            if (topOperation instanceof DMLWriteOperation) {
                DMLWriteOperation dmlTopOperation = (DMLWriteOperation) this.topOperation;
                return spliceRawTxn.elevate(dmlTopOperation.getDestinationTable());
            } else if (activation.isTraced()) {
                return spliceRawTxn.elevate("xplain".getBytes());
            } else {
                throw new IllegalStateException("Programmer error: attempting to elevate a non-write/non-trace operation");
            }
        }
        return currentTxn;
    }

    private TxnView getTransaction() throws StandardException {
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
    }

    public IOStats getStats() {
        return delegate.getStats();
    }
}
