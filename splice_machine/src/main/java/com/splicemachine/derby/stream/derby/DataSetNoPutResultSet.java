package com.splicemachine.derby.stream.derby;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.ResultSet;
import com.splicemachine.db.iapi.sql.conn.StatementContext;
import com.splicemachine.db.iapi.sql.execute.*;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.DataSet;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.sql.SQLWarning;
import java.sql.Timestamp;
import java.util.Iterator;

/**
 * Created by jleach on 4/28/15.
 */
public class DataSetNoPutResultSet implements NoPutResultSet, CursorResultSet {
        private static Logger LOG = Logger.getLogger(DataSetNoPutResultSet.class);
        protected Activation activation;
        protected ResultDescription resultDescription;
        protected SpliceOperation topOperation;
        protected DataSet dataSet;
        protected LocatedRow locatedRow;
        protected volatile boolean closed;
        protected boolean returnsRows;
        private StatementContext statementContext;
        private NoPutResultSet[] subqueryTrackingArray;
        protected Iterator<LocatedRow> iterator;
        private long scrollId;
        private long taskId = -1;
        private String regionName;


        public DataSetNoPutResultSet(Activation activation,SpliceOperation topOperation,DataSet dataSet){
            this(activation,topOperation,dataSet,true);
        }

        public DataSetNoPutResultSet(Activation activation,
                                    SpliceOperation topOperation,
                                    DataSet dataSet, boolean returnsRows){
            SpliceLogUtils.trace(LOG, "instantiate with topOperation=%s, dataSet %s, returnRows=%s", topOperation, dataSet, returnsRows);
            this.activation = activation;
            if(activation!=null)
                this.resultDescription = activation.getPreparedStatement().getResultDescription();
            this.topOperation = topOperation;
            this.dataSet = dataSet;
            this.returnsRows = returnsRows;
        }

        @Override
        public boolean returnsRows() {
            return returnsRows;
        }

        @Override
        public int modifiedRowCount() {
            try {
                int modifiedRowCount = 0;
                while (iterator.hasNext()) {
                    modifiedRowCount += iterator.next().getRow().getColumn(1).getInt();
                }
                return modifiedRowCount;
            } catch (StandardException se) {
                throw new RuntimeException(se);
            }
        }

        @Override
        public ResultDescription getResultDescription() {
            return resultDescription;
        }

        @Override
        public Activation getActivation() {
            return activation;
        }

        @Override
        public void open() throws StandardException {

        }

        @Override
        public ExecRow getAbsoluteRow(int row) throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow getRelativeRow(int row) throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow setBeforeFirstRow() throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow getFirstRow() throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow getNextRow() throws StandardException {
            SpliceLogUtils.trace(LOG, "getNextRow");
            attachStatementContext();
            return getNextRowCore();
        }

        private void attachStatementContext() throws StandardException {
            if(statementContext == null || !statementContext.onStack()){
                statementContext = activation.getLanguageConnectionContext().getStatementContext();
            }
            statementContext.setTopResultSet(this,subqueryTrackingArray);
            if(subqueryTrackingArray == null)
                subqueryTrackingArray = statementContext.getSubqueryTrackingArray();
            statementContext.setActivation(activation);
        }

        @Override
        public ExecRow getPreviousRow() throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow getLastRow() throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public ExecRow setAfterLastRow() throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void clearCurrentRow() {
            SpliceLogUtils.trace(LOG,"clearCurrentRow");
        }

        @Override
        public boolean checkRowPosition(int isType) throws StandardException {
            throw new RuntimeException("not implemented");
        }

        @Override
        public int getRowNumber() {
            throw new RuntimeException("not implemented");
        }

        @Override
        public void close() throws StandardException {
            SpliceLogUtils.trace(LOG, "close=%s",closed);
            if(closed) return;
            dataSet.close();
            closed =true;
        }

        @Override
        public void cleanUp() throws StandardException {
            SpliceLogUtils.trace(LOG, "cleanup");
        }

        @Override
        public boolean isClosed() {
            return closed;
        }

        @Override
        public void finish() throws StandardException {
            SpliceLogUtils.trace(LOG, "finish");
            if(!isClosed())close();
        }

        @Override
        public long getExecuteTime() {
            SpliceLogUtils.trace(LOG,"getExecuteTime");
            return 0;
        }

        @Override
        public Timestamp getBeginExecutionTimestamp() {
            SpliceLogUtils.trace(LOG,"getBeginExecutionTimestamp");
            return null;
        }

        @Override
        public Timestamp getEndExecutionTimestamp() {
            SpliceLogUtils.trace(LOG,"getEndExecutionTimestamp");
            return null;
        }

        @Override
        public long getTimeSpent(int type) {
            SpliceLogUtils.trace(LOG,"getTimeSpent type %d",type);
            return 0;
        }

        @Override
        public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
            SpliceLogUtils.trace(LOG,"getSubqueryTrackingArray with numSubqueries %d",numSubqueries);
            if (subqueryTrackingArray == null)
                subqueryTrackingArray = new NoPutResultSet[numSubqueries];

            return subqueryTrackingArray;
        }

        @Override
        public ResultSet getAutoGeneratedKeysResultset() {
            SpliceLogUtils.trace(LOG,"getAutoGeneratedKeysResultSet");
            return null;
        }

        @Override
        public String getCursorName() {
            SpliceLogUtils.trace(LOG, "getCursorName");
            if ((activation.getCursorName() == null) && isForUpdate())
                activation.setCursorName(activation.getLanguageConnectionContext().getUniqueCursorName());
            return activation.getCursorName();
        }

        @Override
        public void addWarning(SQLWarning w) {
            SpliceLogUtils.trace(LOG, "addWarning");
        }

        @Override
        public SQLWarning getWarnings() {
            SQLWarning warnings = activation.getWarnings();
            activation.clearWarnings();
            return warnings;
        }

        @Override
        public boolean needsRowLocation() {
            SpliceLogUtils.trace(LOG, "needsRowLocation");
            return false;
        }

        @Override
        public void rowLocation(RowLocation rl) throws StandardException {
            SpliceLogUtils.trace(LOG,"needsRowLocation");
        }

        @Override
        public DataValueDescriptor[] getNextRowFromRowSource()
                throws StandardException {
            SpliceLogUtils.trace(LOG,"getNextRowFromRowSource");
            return null;
        }

        @Override
        public boolean needsToClone() {
            SpliceLogUtils.trace(LOG,"needsToClone");
            return false;
        }

        @Override
        public FormatableBitSet getValidColumns() {
            SpliceLogUtils.trace(LOG,"getValidColumns");
            return null;
        }

        @Override
        public void closeRowSource() {
            SpliceLogUtils.trace(LOG, "closeRowSource");
        }

        @Override
        public void markAsTopResultSet() {
            SpliceLogUtils.trace(LOG,"markAsTopResultSet");
        }

        @Override
        public void openCore() throws StandardException {
            SpliceLogUtils.trace(LOG,"opening rowProvider %s",dataSet);
            iterator = dataSet.toLocalIterator();
            closed=false;
        }

        @Override
        public void reopenCore() throws StandardException {
            SpliceLogUtils.trace(LOG, "reopening rowProvider %s",dataSet);
            openCore();
        }

        @Override
        public ExecRow getNextRowCore() throws StandardException {
            SpliceLogUtils.trace(LOG,"nextRow");
            try {
                if(iterator.hasNext()){
                    locatedRow = iterator.next();
                    activation.setCurrentRow(locatedRow.getRow(),resultSetNumber());
                    SpliceLogUtils.trace(LOG, "nextRow=%s", locatedRow);
                    return locatedRow.getRow();
                }else {
                    return null;
                }
            } catch (Throwable t) {
                throw Exceptions.parseException(t);
            }
        }

        @Override
        public int getPointOfAttachment() {
            SpliceLogUtils.trace(LOG, "getPointOfAttachment");
            return 0;
        }

        @Override
        public int getScanIsolationLevel() {
            SpliceLogUtils.trace(LOG,"getScanIsolationLevel");
            return 0;
        }

        @Override
        public void setTargetResultSet(TargetResultSet trs) {
            SpliceLogUtils.trace(LOG,"setTargetResultSet %s",trs);
        }

        @Override
        public void setNeedsRowLocation(boolean needsRowLocation) {
            SpliceLogUtils.trace(LOG,"setNeedsRowLocation %b",needsRowLocation);
        }

        @Override
        public double getEstimatedRowCount() {
            SpliceLogUtils.trace(LOG,"getEstimatedRowCount");
            return topOperation.getEstimatedRowCount();
        }

        @Override
        public int resultSetNumber() {
            SpliceLogUtils.trace(LOG,"resultSetNumber");
            return topOperation.resultSetNumber();
        }

        @Override
        public void setCurrentRow(ExecRow row) {
            SpliceLogUtils.trace(LOG, "setCurrentRow %s",row);
            activation.setCurrentRow(row, topOperation.resultSetNumber());
        }

        @Override
        public boolean requiresRelocking() {
            SpliceLogUtils.trace(LOG,"requiresRelocking");
            return false;
        }

        @Override
        public boolean isForUpdate() {
//		SpliceLogUtils.trace(LOG, "isForUpdate");
            return false;
        }

        @Override
        public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException {
            SpliceLogUtils.trace(LOG, "updateRow with row %s, and rowChanger %s",row,rowChanger);
        }

        @Override
        public void markRowAsDeleted() throws StandardException {
            SpliceLogUtils.trace(LOG,"markRowAsDeleted");
        }

        @Override
        public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException {
            SpliceLogUtils.trace(LOG,"positionScanAtRowLocation with RowLocation %s",rLoc);
        }
        @Override
        public RowLocation getRowLocation() throws StandardException {
            SpliceLogUtils.trace(LOG, "getRowLocation");
            return locatedRow!=null?locatedRow.getRowLocation():null;
        }
        @Override
        public ExecRow getCurrentRow() throws StandardException {
            SpliceLogUtils.trace(LOG, "getCurrentRow");
            return locatedRow!=null?locatedRow.getRow():null;
        }

        public void setScrollId(long scrollId) {
            this.scrollId = scrollId;
        }

        public void setTaskId(long taskId){
            this.taskId = taskId;
        }

        public void setRegionName(String regionName){
            this.regionName = regionName;
        }

        public IOStats getStats() {
            return null;
        }
    }
