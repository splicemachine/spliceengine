package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultSet;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public abstract class NoRowsResultSetOperation implements ResultSet {
	private static Logger LOG = Logger.getLogger(NoRowsResultSetOperation.class);
	final Activation    activation;
    private NoPutResultSet[] subqueryTrackingArray;
	/** True if the result set has been opened, and not yet closed. */
	private boolean isOpen;
	/* Run time statistics variables */
	final LanguageConnectionContext lcc;
	protected long beginTime;
	protected long endTime;
	protected long beginExecutionTime;
	protected long endExecutionTime;

    private int                             firstColumn = -1;    // First column being stuffed. For UPDATES, this lies in the second half of the row.
    private int[]                           generatedColumnPositions; // 1-based positions of generated columns in the target row

    // One cell for  each slot in generatedColumnPositions. These are temporary
    // values which hold the result of running the generation clause before we
    // stuff the result into the target row.
    private DataValueDescriptor[]  normalizedGeneratedValues;
    
    
	public NoRowsResultSetOperation(Activation activation) {
		this.activation = activation;
		lcc = activation.getLanguageConnectionContext();
	}
	
	public final boolean returnsRows() { return false; }
	public int	modifiedRowCount() { return 0; }
	public final Activation getActivation() {
		SpliceLogUtils.trace(LOG, "getActivation");
		return activation;
	}
}
