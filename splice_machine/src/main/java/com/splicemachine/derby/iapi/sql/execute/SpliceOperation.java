package com.splicemachine.derby.iapi.sql.execute;

import com.splicemachine.db.iapi.sql.execute.ExecIndexRow;
import com.splicemachine.derby.iapi.storage.RowProvider;

import java.io.IOException;
import java.util.List;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.OperationInformation;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.job.JobResults;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.spark.api.java.JavaRDD;

/**
 * 
 * Interface for Parallel Operations in the Splice Machine.
 * 
 * @author John Leach
 *
 */

public interface SpliceOperation extends StandardCloseable {

    RowLocation getCurrentRowLocation();

    void setCurrentRowLocation(RowLocation rowLocation);

    /**
     * @return a descriptive name for this operation. Used for reporting information.
     */
    String getName();

    /**
     * @return true if statistics recording is enabled.
     */
    boolean shouldRecordStats();

	 JobResults getJobResults();

    OperationInformation getOperationInformation();

    /**
	 * 
	 * Enumeration with the following types:
	 * 
	 * 	SCAN 	: Accesses HBase storage.
	 *  MAP		: Can be pushed to either the scan or the reduce Operation below it on the left hand side.
	 *  REDUCE	: The node needs to run the reduce steps after the sink.
	 *  SINK	: 
	 *  SCROLL	: The node returns a scrollable set of data to the client.
	 *  
	 */
	public enum NodeType { SCAN, MAP, REDUCE, SINK, SCROLL}

    public int modifiedRowCount();

    public Activation getActivation();

    public void clearCurrentRow();

    public void close() throws StandardException,IOException;

    public void markAsTopResultSet();

    public void open() throws StandardException,IOException;

    public int resultSetNumber();

    public void setCurrentRow(ExecRow row);

    public ExecRow nextRow(SpliceRuntimeContext spliceRuntimeContext) throws StandardException,IOException;

	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getMapRowProvider(SpliceOperation top,PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext) throws StandardException, IOException;
	
	/**
	 * Get the mechanism for providing Rows to the SpliceNoPutResultSet
	 * @return the mechanism for providing Rows to the SpliceNoPutResultSet
	 */
	public RowProvider getReduceRowProvider(SpliceOperation top, PairDecoder decoder, SpliceRuntimeContext spliceRuntimeContext, boolean returnDefaultValue) throws StandardException, IOException;

		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException;

		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException;

	/**
	 * Initializes the node with the statement and the language context from the SpliceEngine.
	 * 
	 * @throws StandardException
	 */
	public void init(SpliceOperationContext operationContext) throws IOException, StandardException;

	/**
	 * List of Node Types that determine the Operation's behaviour pattern.
	 * 
	 */
	public List<NodeType> getNodeTypes();

	/**
	 * Set of operations for a node.
	 * 
	 */
	public List<SpliceOperation> getSubOperations();

	/**
	 * Unique node sequence id.  Should move from Zookeeper to uuid generator.
	 * 
	 */
	public byte[] getUniqueSequenceID();
	/**
	 * Execute a sink operation.  Must be a sink node.  This operation will be called from the OperationTree. 
	 * 
	 * @see com.splicemachine.derby.impl.sql.execute.operations.OperationTree
	 */
	public void executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException, IOException;
	/**
	 * 
	 * Executes a scan operation from a node that has either a SCROLL node type or that is called from another node.
	 * 
	 * @return
	 */
	public SpliceNoPutResultSet executeScan(SpliceRuntimeContext runtimeContext) throws StandardException;

	/**
	 * 
	 * Probe scan for hash joins.  This may not belong in the interface and is just a once off.
	 * 
	 * @return
	 */
	public SpliceNoPutResultSet executeProbeScan() throws StandardException;
	/**
	 * 
	 * Gets the left Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getLeftOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateLeftOperationStack(List<SpliceOperation> operations);

	/**
	 * 
	 * Gets the right Operation for a Operation.  They can be named different things in different operations (Source, LeftResultSet, etc.).  
	 * This gives a simple method to retrieve that operation.  This needs to be implemented in each operation.
	 * 
	 * @return
	 */
	public SpliceOperation getRightOperation();
	/**
	 * 
	 * Recursively generates the left operation stack.  This method is implemented properly as long as you inherit from
	 * the SpliceBaseOperation.
	 * 
	 * @return
	 */
	public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations);

    public void generateAllOperationStack(List<SpliceOperation> operations);
	/**
	 * 
	 * The outgoing field definition of the record.  Do we need incoming as well?
	 * 
	 * @return
	 * @throws StandardException 
	 */

	public ExecRow getExecRowDefinition() throws StandardException;

    /**
     * Returns an array containing integer pointers into the columns of the table. This
     * array relates the compact row column location (integer pointer) to the original
     * location (the nth column on the original table).
     *
     *
     * @param tableNumber
     * @return
     */
    int[] getRootAccessedCols(long tableNumber) throws StandardException;

    /**
     * Returns true if this operation references the given table number.  For a join,
     * this means either the left side or the right side involves that table.  For things
     * like table scans, it's true if it is scanning that table.
     */
    boolean isReferencingTable(long tableNumber);

    /**
     * Prints out a string representation of this operation, formatted for easy human consumption.
     *
     * @return a pretty-printed string representation of this operation.
     */
    String prettyPrint(int indentLevel);

    /**
     * Get the recorded metrics for the Operation. This should only be called when trace
     * metrics gathering is enabled.
     *
     * @return the metrics recorded by this operation. Return {@code null} if no
     * metrics have been collected.
     */
    OperationRuntimeStats getMetrics(long statementId,long taskId, boolean isTopOperation);

    /**
     * @return -1l if no statementId has been set on this operation, or the statement
     * id if one has. Generally, a statementId is only set on the top operation
     */
    long getStatementId();

    void setStatementId(long statementId);

    int[] getAccessedNonPkColumns() throws StandardException;
		
	void setActivation(Activation activation) throws StandardException;

    String getInfo();

    double getEstimatedCost();

    double getEstimatedRowCount();

    /**
     * @return true if this operation can provide an RDD that could be operated on using Spark
     */
    public boolean providesRDD();

    /**
     * @return true if this top operation expects the results as a Spark RDD
     */
    public boolean expectsRDD();

    public JavaRDD<LocatedRow> getRDD(SpliceRuntimeContext spliceRuntimeContext, SpliceOperation top) throws StandardException;

    /**
     *
     * @return true if this is pushed and run serverside
     */
    public boolean pushedToServer();


    /**
     *
     * Executes a scan operation from a node that has either a SCROLL node type or that is called from another node.
     */
    public SpliceNoPutResultSet executeRDD(SpliceRuntimeContext runtimeContext) throws StandardException;

    public ExecIndexRow getStartPosition() throws StandardException;

	String getOptimizerOverrides();

	String getOptimizerOverrides(SpliceRuntimeContext ctx);

	/**
	 * @return the "niceness" at which this should run. If this is not set by the user (or the optimizer),
	 * then this will return {@code -1}
     */
	int getQueryNiceness(SpliceRuntimeContext ctx);
}
