package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.derby.impl.sql.execute.operations.NoRowsOperation;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.Properties;

/**
 * Converts between a SpliceOperation and a NoPutResultSet.
 *
 * @author Scott Fines
 * Created on: 9/23/13
 */
public class ConversionResultSet implements NoPutResultSet,CursorResultSet,Externalizable,ConvertedResultSet {
    private SpliceOperation topOperation;
    private Activation activation;
    private SpliceRuntimeContext spliceRuntimeContext;
    private Properties optimizerOverrides;

    public ConversionResultSet() {
    }

    public ConversionResultSet(SpliceOperation topOperation) {
        this(topOperation,null);
    }

    public ConversionResultSet(SpliceOperation topOperation, Activation activation) {
        this.topOperation = topOperation;
        this.activation = activation;
        this.spliceRuntimeContext = new SpliceRuntimeContext();
    }

    @Override
    public RowLocation getRowLocation() throws StandardException {
        return topOperation.getCurrentRowLocation();
    }

    @Override
    public ExecRow getCurrentRow() throws StandardException {
        return (ExecRow)activation.getCurrentRow(topOperation.resultSetNumber());
    }

    @Override
    public void markAsTopResultSet() {
        topOperation.markAsTopResultSet();
    }

    @Override
    public void openCore() throws StandardException {
        TxnView txn = spliceRuntimeContext.getTxn();
        if(txn ==null)
            spliceRuntimeContext.setTxn(((SpliceTransactionManager)activation.getTransactionController()).getActiveStateTxn());
        try {
            topOperation.open();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void reopenCore() throws StandardException {
        openCore();
    }

    @Override
    public ExecRow getNextRowCore() throws StandardException {
        try {
            return topOperation.nextRow(spliceRuntimeContext);
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public int getPointOfAttachment() {
        return 0;
    }

    @Override
    public int getScanIsolationLevel() {
        return 0;
    }

    @Override
    public void setTargetResultSet(TargetResultSet trs) {
        //no-op
    }

    @Override
    public void setNeedsRowLocation(boolean needsRowLocation) {
        //no-op
    }

    @Override
    public double getEstimatedRowCount() {
        return 0d;
    }

    @Override
    public int resultSetNumber() {
        return topOperation.resultSetNumber();
    }

    @Override
    public void setCurrentRow(ExecRow row) {
        activation.setCurrentRow(row,topOperation.resultSetNumber());
    }

    @Override
    public boolean requiresRelocking() {
        return false;
    }

    @Override
    public boolean isForUpdate() {
        return topOperation instanceof UpdateOperation;
    }

    @Override
    public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException {
        //no-op
    }

    @Override
    public void markRowAsDeleted() throws StandardException {
        //no-op
    }

    @Override
    public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException {
        //no-op
    }

    @Override
    public boolean returnsRows() {
        return !(topOperation instanceof NoRowsOperation);
    }

    @Override
    public int modifiedRowCount() {
        return topOperation.modifiedRowCount();
    }

    @Override
    public ResultDescription getResultDescription() {
        return activation.getResultDescription();
    }

    @Override
    public Activation getActivation() {
        return activation;
    }

    @Override
    public void open() throws StandardException {
        try {
            topOperation.open();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public ExecRow getAbsoluteRow(int row) throws StandardException {
        return null;
    }

    @Override
    public ExecRow getRelativeRow(int row) throws StandardException {
        return null;
    }

    @Override
    public ExecRow setBeforeFirstRow() throws StandardException {
        return null;
    }

    @Override
    public ExecRow getFirstRow() throws StandardException {
        return null;
    }

    @Override
    public ExecRow getNextRow() throws StandardException {
        return getNextRowCore();
    }

    @Override
    public ExecRow getPreviousRow() throws StandardException {
        return null;
    }

    @Override
    public ExecRow getLastRow() throws StandardException {
        return null;
    }

    @Override
    public ExecRow setAfterLastRow() throws StandardException {
        return null;
    }

    @Override
    public void clearCurrentRow() {
        activation.clearCurrentRow(topOperation.resultSetNumber());
    }

    @Override
    public boolean checkRowPosition(int isType) throws StandardException {
        return false;
    }

    @Override
    public int getRowNumber() {
        return 0;
    }

    @Override
    public void close() throws StandardException {
        try {
            topOperation.close();
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void cleanUp() throws StandardException {
        close();
    }

    @Override
    public boolean isClosed() {
        return false;
    }

    @Override
    public void finish() throws StandardException {
        close();
    }

    @Override
    public long getExecuteTime() {
        return 0;
    }

    @Override
    public Timestamp getBeginExecutionTimestamp() {
        return null;
    }

    @Override
    public Timestamp getEndExecutionTimestamp() {
        return null;
    }

    @Override
    public long getTimeSpent(int type) {
        return 0;
    }

    @Override
    public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
        return new NoPutResultSet[0];
    }

    @Override
    public ResultSet getAutoGeneratedKeysResultset() {
        return null;
    }

    @Override
    public String getCursorName() {
        return null;
    }

    @Override
    public void addWarning(SQLWarning w) {
        activation.addWarning(w);
    }

    @Override
    public SQLWarning getWarnings() {
        return activation.getWarnings();
    }

    @Override
    public boolean needsRowLocation() {
        return false;
    }

    @Override
    public void rowLocation(RowLocation rl) throws StandardException {
    }

    @Override
    public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException {
        return new DataValueDescriptor[0];
    }

    @Override
    public boolean needsToClone() {
        return false;
    }

    @Override
    public FormatableBitSet getValidColumns() {
        return null;
    }

    @Override
    public void closeRowSource() {
        //no-op
    }


    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(topOperation);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        topOperation = (SpliceOperation)in.readObject();
    }

    public void setActivation(Activation activation){
        this.activation = activation;
    }

    @Override
    public SpliceOperation getOperation() {
        return topOperation;
    }

    @Override
    public Properties getUserOptimizerOverrides(){
        if(optimizerOverrides==null){
            String overrideString = topOperation.getOptimizerOverrides();
            if(overrideString==null||overrideString.length()<=0) return null;

            String[] properties=overrideString.split(",");
            optimizerOverrides = new Properties();
            for(String prop:properties){
                String[] keyValue=prop.split("=");
                optimizerOverrides.put(keyValue[0],keyValue[1]);
            }
        }
        return optimizerOverrides;
    }
}
