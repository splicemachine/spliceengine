package com.splicemachine.derby.iapi.sql.execute;

import java.io.IOException;
import java.sql.SQLWarning;
import java.sql.Timestamp;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.stats.IOStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

/**
 *
 * Basic interface for performing NoPutResultSets.  Will extend for bulk methods..
 *
 * @author johnleach
 *
 */
public class SpliceNoPutResultSet implements NoPutResultSet, CursorResultSet {
		private static Logger LOG = Logger.getLogger(SpliceNoPutResultSet.class);
		protected Activation activation;
		protected ResultDescription resultDescription;
		protected SpliceOperation topOperation;
		protected RowProvider rowProvider;
		protected ExecRow execRow;
		protected volatile boolean closed;
		protected boolean returnsRows;
		private StatementContext statementContext;
		private NoPutResultSet[] subqueryTrackingArray;
		/*Information for reporting statistics correctly*/
		private long scrollId;
		private long taskId = -1;
		private String regionName;

		public SpliceNoPutResultSet(Activation activation,SpliceOperation topOperation,RowProvider rowProvider){
				this(activation,topOperation,rowProvider,true);
		}

		public SpliceNoPutResultSet(Activation activation,
																SpliceOperation topOperation,
																RowProvider rowProvider, boolean returnsRows){
				SpliceLogUtils.trace(LOG, "instantiate with rowProvider %s",rowProvider);
				this.activation = activation;
				if(activation!=null)
						this.resultDescription = activation.getPreparedStatement().getResultDescription();
				this.topOperation = topOperation;
				this.rowProvider = rowProvider;
				this.returnsRows = returnsRows;
		}

		@Override
		public boolean returnsRows() {
				SpliceLogUtils.trace(LOG, "returnsRows");
				return returnsRows;
		}

		@Override
		public int modifiedRowCount() {
				SpliceLogUtils.trace(LOG,"modifiedRowCount");
				return rowProvider.getModifiedRowCount();
		}

		@Override
		public ResultDescription getResultDescription() {
				SpliceLogUtils.trace(LOG,"getResultDescription");
				return resultDescription;
		}

		@Override
		public Activation getActivation() {
				SpliceLogUtils.trace(LOG,"getActivation");
				return activation;
		}

		@Override
		public void open() throws StandardException {
				SpliceLogUtils.trace(LOG, "open");
				openCore();
		}

		@Override
		public ExecRow getAbsoluteRow(int row) throws StandardException {
				SpliceLogUtils.trace(LOG,"getAbsoluteRow row: %s",row);
				return null;
		}

		@Override
		public ExecRow getRelativeRow(int row) throws StandardException {
				SpliceLogUtils.trace(LOG,"getRelativeRow row: %s",row);
				return null;
		}

		@Override
		public ExecRow setBeforeFirstRow() throws StandardException {
				SpliceLogUtils.trace(LOG, "setBeforeFirstRow");
				return null;
		}

		@Override
		public ExecRow getFirstRow() throws StandardException {
				SpliceLogUtils.trace(LOG, "getFirstRow");
				return null;
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
				SpliceLogUtils.trace(LOG,"getPreviousRow");
				return null;
		}

		@Override
		public ExecRow getLastRow() throws StandardException {
				SpliceLogUtils.trace(LOG,"getLastRow");
				return null;
		}

		@Override
		public ExecRow setAfterLastRow() throws StandardException {
				SpliceLogUtils.trace(LOG,"setAfterLastRow");
				return null;
		}

		@Override
		public void clearCurrentRow() {
				SpliceLogUtils.trace(LOG,"clearCurrentRow");
		}

		@Override
		public boolean checkRowPosition(int isType) throws StandardException {
				SpliceLogUtils.trace(LOG, "checkRowPosition isType: %d",isType);
				return false;
		}

		@Override
		public int getRowNumber() {
				SpliceLogUtils.trace(LOG,"getRowNumber");
				return 0;
		}

		@Override
		public void close() throws StandardException {
				SpliceLogUtils.trace(LOG, "close=%s",closed);
				if(closed) return; //nothing to do;

				try{
						rowProvider.close();
				}catch(RuntimeException r){
						throw Exceptions.parseException(r);
				}
				boolean xplain = activation.getLanguageConnectionContext().getStatisticsTiming();
				if(xplain){
						String xplainSchema = activation.getLanguageConnectionContext().getXplainSchema();
						long statementId = topOperation.getStatementId();
						if(scrollId==-1l) scrollId = Bytes.toLong(topOperation.getUniqueSequenceID());
						if(taskId==-1l) taskId = SpliceDriver.driver().getUUIDGenerator().nextUUID();
						rowProvider.reportStats(statementId,scrollId,taskId,xplainSchema,regionName);
				}
				closed =true;
		}

		@Override
		public void cleanUp() throws StandardException {
				SpliceLogUtils.trace(LOG, "cleanup");
		}

		@Override
		public boolean isClosed() {
				//		SpliceLogUtils.trace(LOG, "isClosed?%b",closed);
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
				return null;
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
				SpliceLogUtils.trace(LOG,"opening rowProvider %s",rowProvider);
				try{
						rowProvider.open();
				}catch(Exception e){
						e.printStackTrace();
						throw Exceptions.parseException(e);
				}
				closed=false;
		}

		@Override
		public void reopenCore() throws StandardException {
				SpliceLogUtils.trace(LOG, "reopening rowProvider %s",rowProvider);
				openCore();
		}

		@Override
		public ExecRow getNextRowCore() throws StandardException {
				SpliceLogUtils.trace(LOG,"nextRow");
				try {
						if(rowProvider.hasNext()){
								execRow = rowProvider.next();
								activation.setCurrentRow(execRow,resultSetNumber());
								SpliceLogUtils.trace(LOG, "nextRow=%s", execRow);
								return execRow;
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
				return 0;
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
				return rowProvider.getCurrentRowLocation();
		}
		@Override
		public ExecRow getCurrentRow() throws StandardException {
				SpliceLogUtils.trace(LOG, "getCurrentRow");
				return execRow;
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
				return rowProvider.getIOStats();
		}
}
