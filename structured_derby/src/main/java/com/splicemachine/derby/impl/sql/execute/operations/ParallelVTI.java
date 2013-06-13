package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.RowEncoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultDescription;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.sql.execute.RowChanger;
import org.apache.derby.iapi.sql.execute.TargetResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.vti.VTITemplate;
import org.apache.log4j.Logger;

import java.io.*;
import java.sql.*;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * 
 * Base class for VTIs which are computed in parallel.
 * 
 * <p>Implementations of this base class will initiate a call to the SpliceEngine
 * to perform work in parallel. When using this, the task will be broken up into N computation blocks
 * (where N is the number of regions specified by getSplits()); Each computation block will execute sequentially,
 * in a map-only MR job style. Thus, this class should <em>only</em> be implemented when the desired task
 * can be accomplished <em>entirely</em> in parallel. For example, Data imports will work will, but multi-stage
 * aggregation will not.
 * 
 * @author Scott Fines
 */
public abstract class ParallelVTI extends VTITemplate implements SpliceOperation,Externalizable {
	private static final Logger LOG = Logger.getLogger(ParallelVTI.class);
	
	private static final List<NodeType> nodeTypes = Collections.singletonList(NodeType.REDUCE);
	
	private Activation activation;
	private boolean isTopResultSet;

	private double optimizerEstimatedRowCount;
	private int resultSetNumber;
	private ExecRow currentRow;

	private String uniqueSequenceId;
	
	@Override
	public void init(SpliceOperationContext context){
		this.activation = context.getActivation();
	}
	
	
	
	@Override
	public SQLWarning getWarnings() {
		try {
			return super.getWarnings();
		} catch (SQLException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
			return null;
		}
	}


	@Override
	public String getCursorName()  {
		try {
			return super.getCursorName();
		} catch (SQLException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
			return null;
		}
	}



	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		throw new UnsupportedOperationException();
		
	}

    public OperationSink.Translator getTranslator() throws IOException {
        throw new UnsupportedOperationException("getTranslator is not supported on node "+this.getClass());
    }

    @Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		throw new UnsupportedOperationException();
	}

    @Override
    public RowEncoder getRowEncoder() throws StandardException {
        throw new UnsupportedOperationException();
    }

    @Override public void markAsTopResultSet() { this.isTopResultSet=true; }

	@Override
	public void openCore() throws StandardException {
		throw new UnsupportedOperationException();
	}

	@Override
	public void reopenCore() throws StandardException {
		throw new UnsupportedOperationException();
	}

	public ExecRow getNextSinkRow() throws StandardException {
		throw new UnsupportedOperationException();
	}

    @Override
	public ExecRow getNextRowCore() throws StandardException {
		throw new UnsupportedOperationException();
	}

	/*default implementations and no-ops*/
	@Override public int getPointOfAttachment() { return 0;}
	@Override public int getScanIsolationLevel() { return 0; } 
	@Override public void setTargetResultSet(TargetResultSet trs) { } 
	@Override public void setNeedsRowLocation(boolean needsRowLocation) { } 
	@Override public boolean needsRowLocation() { return false; }
	@Override public boolean requiresRelocking() { return false; } 
	@Override public boolean isForUpdate() { return false; } 
	@Override public void updateRow(ExecRow row, RowChanger rowChanger) throws StandardException { }
	@Override public void markRowAsDeleted() throws StandardException { }
	@Override public void positionScanAtRowLocation(RowLocation rLoc) throws StandardException { }
	@Override public boolean returnsRows() { return false; }
	@Override public int modifiedRowCount() { return 0; }
	@Override public ResultDescription getResultDescription() { return null; }
	@Override public void open() throws StandardException { }
	@Override public ExecRow getAbsoluteRow(int row) throws StandardException { return null; }
	@Override public ExecRow getRelativeRow(int row) throws StandardException { return null; }
	@Override public ExecRow setBeforeFirstRow() throws StandardException { return null; }
	@Override public ExecRow getFirstRow() throws StandardException { return null; }
	@Override public ExecRow getNextRow() throws StandardException { return null; }
	@Override public ExecRow getPreviousRow() throws StandardException { return null; }
	@Override public ExecRow getLastRow() throws StandardException { return null; }
	@Override public ExecRow setAfterLastRow() throws StandardException { return null; }
	@Override public void clearCurrentRow() { }
	@Override public boolean checkRowPosition(int isType) throws StandardException { return false; }
	@Override public int getRowNumber() { return 0; }
	@Override public void cleanUp() throws StandardException { } 
	@Override public boolean isClosed() { return false; }
	@Override public void finish() throws StandardException { }
	@Override public long getExecuteTime() { return 0; }
	@Override public Timestamp getBeginExecutionTimestamp() { return null; }
	@Override public Timestamp getEndExecutionTimestamp() { return null; }
	@Override public long getTimeSpent(int type) { return 0; }
	@Override public NoPutResultSet[] getSubqueryTrackingArray(int numSubqueries) { return null; }
	@Override public ResultSet getAutoGeneratedKeysResultset() { return null; }
	@Override public void addWarning(SQLWarning w) { }
	@Override public void rowLocation(RowLocation rl) throws StandardException { }
	@Override public DataValueDescriptor[] getNextRowFromRowSource() throws StandardException { return null; }
	@Override public boolean needsToClone() { return false; }
	@Override public FormatableBitSet getValidColumns() { return null; }
	@Override public void closeRowSource() { }

	/*getters and setters and other one-line impls*/
	@Override public double getEstimatedRowCount() { return this.optimizerEstimatedRowCount; }
	@Override public int resultSetNumber() { return this.resultSetNumber; } 
	@Override public Activation getActivation() { return activation; }
	
	@Override
	public void setCurrentRow(ExecRow row) {
		activation.setCurrentRow(row, resultSetNumber);
		currentRow = row;
	}
	

	/*Default UnsupportedOperations*/
	@Override public RowId getRowId(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public RowId getRowId(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateRowId(int columnIndex, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateRowId(String columnLabel, RowId x) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public int getHoldability() throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNString(int columnIndex, String nString)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNString(String columnLabel, String nString)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(String columnLabel, NClob nClob)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public NClob getNClob(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public NClob getNClob(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public SQLXML getSQLXML(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public SQLXML getSQLXML(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateSQLXML(int columnIndex, SQLXML xmlObject)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateSQLXML(String columnLabel, SQLXML xmlObject)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public String getNString(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public String getNString(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public Reader getNCharacterStream(int columnIndex) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public Reader getNCharacterStream(String columnLabel) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateAsciiStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBinaryStream(int columnIndex, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateCharacterStream(int columnIndex, Reader x, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateAsciiStream(String columnLabel, InputStream x, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBinaryStream(String columnLabel, InputStream x,
			long length) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateCharacterStream(String columnLabel, Reader reader,
			long length) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBlob(int columnIndex, InputStream inputStream, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBlob(String columnLabel, InputStream inputStream,
			long length) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(int columnIndex, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(String columnLabel, Reader reader, long length)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateAsciiStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBinaryStream(int columnIndex, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateCharacterStream(int columnIndex, Reader x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateAsciiStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBinaryStream(String columnLabel, InputStream x)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateCharacterStream(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBlob(int columnIndex, InputStream inputStream)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateBlob(String columnLabel, InputStream inputStream)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(int columnIndex, Reader reader) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public void updateNClob(String columnLabel, Reader reader)
			throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public <T> T unwrap(Class<T> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override public boolean isWrapperFor(Class<?> iface) throws SQLException {
		throw new UnsupportedOperationException();
	}
	@Override
	public Object getObject(String colName, Map map) throws SQLException {
		return super.getObject(colName, map);
	}



	/*Actual SpliceBaseOperations to be implemented*/
	@Override
	public RowProvider getMapRowProvider(SpliceOperation top,
			ExecRow outputRowFormat) {
		throw new UnsupportedOperationException();
	}

	@Override
	public RowProvider getReduceRowProvider(SpliceOperation top,
			ExecRow outputRowFormat) {
		throw new UnsupportedOperationException();
	}

	@Override
	public void cleanup() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final List<NodeType> getNodeTypes() {
		return nodeTypes;
	}

	@Override
	public List<SpliceOperation> getSubOperations() {
		throw new UnsupportedOperationException();
	}

	@Override
	public String getUniqueSequenceID() {
		return this.uniqueSequenceId;
	}

	@Override
	public abstract void executeShuffle() throws StandardException;

	@Override
	public NoPutResultSet executeScan() throws StandardException {
		throw new UnsupportedOperationException("Scans are not supported in Fully parallel mode");
	}

	@Override
	public NoPutResultSet executeProbeScan() throws StandardException {
		throw new UnsupportedOperationException("Scans are not supported in Fully parallel mode");
	}

	@Override
	public SpliceOperation getLeftOperation() {
		throw new UnsupportedOperationException();
	}

	@Override
	public final void generateLeftOperationStack(List<SpliceOperation> operations) {
		SpliceLogUtils.trace(LOG, "generateLeftOperationStack");
		OperationUtils.generateLeftOperationStack(this, operations);
	}

	@Override
	public abstract ExecRow getExecRowDefinition();

	@Override
	public abstract boolean next();

	@Override
	public abstract void close();


    @Override
    public SpliceOperation getRightOperation() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {
        //To change body of implemented methods use File | Settings | File Templates.
    }

//    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }

//    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        throw new UnsupportedOperationException();
    }
}
