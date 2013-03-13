package com.splicemachine.derby.iapi.sql.execute;

import java.sql.SQLWarning;
import java.sql.Timestamp;

import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.reference.SQLState;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.conn.StatementContext;
import org.apache.derby.iapi.sql.execute.CursorResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.ExecutionFactory;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.ResultSetStatisticsFactory;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.RunTimeStatistics;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.sql.execute.xplain.XPLAINVisitor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.storage.ClientScanProvider;
import com.splicemachine.derby.utils.SpliceUtils;
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
    protected long startTime;
	protected long endTime;
	protected boolean statisticsTimingOn;
	
    public SpliceNoPutResultSet(Activation activation,SpliceOperation topOperation,RowProvider rowProvider){
		this(activation,topOperation,rowProvider,true);
	}
	
	public SpliceNoPutResultSet(Activation activation,
															SpliceOperation topOperation,
															RowProvider rowProvider, boolean returnsRows){
		SpliceLogUtils.trace(LOG, "instantiate with rowProvider %s",rowProvider);
		this.activation = activation;
		this.resultDescription = activation.getPreparedStatement().getResultDescription();
		this.topOperation = topOperation;
		this.rowProvider = rowProvider;
		this.returnsRows = returnsRows;
//		if (statisticsTimingOn = activation.getLanguageConnectionContext().getStatisticsTiming())
//			startTime = System.currentTimeMillis();
		statisticsTimingOn = true;
		startTime = System.currentTimeMillis();
	}



	public SpliceNoPutResultSet(Scan scan, String table,
			Activation activation,
			SpliceOperation topOperation,
			ExecRow row) {
		this(activation,topOperation,buildRowProvider(table,scan,activation,topOperation,row));
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
		SpliceLogUtils.trace(LOG,"getAbsoluteRow row: "+row);
		return null;
	}

	@Override
	public ExecRow getRelativeRow(int row) throws StandardException {
		SpliceLogUtils.trace(LOG,"getRelativeRow row: "+row);
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
        statementContext.setTopResultSet(topOperation,subqueryTrackingArray);
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
		SpliceLogUtils.trace(LOG, "checkRowPosition isType: "+ isType);
		return false;
	}

	@Override
	public int getRowNumber() {
		SpliceLogUtils.trace(LOG,"getRowNumber");
		return 0;
	}

	@Override
	public void close() throws StandardException {
		SpliceLogUtils.trace(LOG, "close="+closed);
		if(closed) return; //nothing to do;

		/*
		 ** If run time statistics tracing is turned on, then now is the
		 ** time to dump out the information.
		 */
		LanguageConnectionContext lcc = activation.getLanguageConnectionContext();

		// only if statistics is switched on, collect & derive them
		if (lcc.getRunTimeStatisticsMode() &&
				!lcc.getStatementContext().getStatementWasInvalidated())
		{   
			endTime = System.currentTimeMillis() - startTime;
			
			// get the ResultSetStatisticsFactory, which gathers RuntimeStatistics
			ExecutionFactory ef = lcc.getLanguageConnectionFactory().getExecutionFactory();
			ResultSetStatisticsFactory rssf = ef.getResultSetStatisticsFactory();

			// get the RuntimeStatisticsImpl object which is the wrapper for all 
			// gathered statistics about all the different resultsets
			RunTimeStatistics rsImpl = rssf.getRunTimeStatistics(activation, this, subqueryTrackingArray); 

			// save the RTW (wrapper)object in the lcc
			lcc.setRunTimeStatisticsObject(rsImpl);

			// now explain gathered statistics, using an appropriate visitor
			XPLAINVisitor visitor = ef.getXPLAINFactory().getXPLAINVisitor();
			visitor.doXPLAIN(rsImpl,activation);
		}

		int staLength = (subqueryTrackingArray == null) ? 0 : subqueryTrackingArray.length;

		for (int index = 0; index < staLength; index++)
		{
			if (subqueryTrackingArray[index] == null || subqueryTrackingArray[index].isClosed())
				continue;
			subqueryTrackingArray[index].close();
		}

		rowProvider.close();
		topOperation.close();
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
		SpliceLogUtils.trace(LOG,"getTimeSpent type "+type);
		return 0;
	}

	@Override
	public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) {
		SpliceLogUtils.trace(LOG,"getSubqueryTrackingArray with numSubqueries "+ numSubqueries);
		return null;
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
		SpliceLogUtils.trace(LOG,"getNextRowCore");
        try{
            if(rowProvider.hasNext()){
                execRow = rowProvider.next();
                SpliceLogUtils.trace(LOG, "nextRow=%s", execRow);
                return execRow;
            }else {
                return null;
            }
        }catch(Throwable t){
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
	
	private static RowProvider buildRowProvider(String table,Scan scan, Activation activation,
			SpliceOperation topOperation,ExecRow execRow) {
		byte[] instructions = SpliceUtils.generateInstructions(activation,topOperation);
		scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,instructions);
		return new ClientScanProvider(Bytes.toBytes(table),scan,execRow,null);
	}
}
