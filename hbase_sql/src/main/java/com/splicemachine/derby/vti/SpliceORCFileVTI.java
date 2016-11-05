package com.splicemachine.derby.vti;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.vti.iapi.DatasetProvider;
import com.splicemachine.access.HConfiguration;
//import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;

import org.apache.log4j.Logger;

/**
 * The is a VTI to read hive ORC files. It can read either a single file or
 * files in a folder. The files will need to be in HDFS.
 * 
 * The DatasetProvider is the mechanism for constructing the execution tree
 * 
 * The VTICosting is the interface that the query optimizer uses to determine
 * the cost of executing the table function.
 * 
 * @author jramineni
 * 
 */
public class SpliceORCFileVTI implements DatasetProvider, VTICosting {

	private static final Logger LOG = Logger.getLogger(SpliceORCFileVTI.class);

	// Instance variable that will store the name of the ORC file or folder
	// containing the files in HDFS
	private String fileName;
	// Date Format in ORC file
	private String dateTimeFormat = "yyyy-mm-dd";
	// Timestamp Format in ORC file
	private String timestampFormat = "yyyy-mm-dd hh:mm:ss.fffffffff";

	// The column list to ?
	private int[] columnIndex;

	// Provide external context which can be carried with the operation
	private OperationContext operationContext;

	/**
	 * This empty constructor
	 */
	public SpliceORCFileVTI() {
	}

	/**
	 * This is the signature used by invoking the VTI using the class name.
	 * 
	 * @param fileName
	 *            : Path to file or folder in HDFS corresponding to the ORC
	 *            files to read
	 */
	public SpliceORCFileVTI(String fileName) {
		this.fileName = fileName;
	}

	public static DatasetProvider getSpliceORCFileVTI(String fileName) {
		return new SpliceORCFileVTI(fileName);
	}

	/**
	 * Method is called by the VTIOperation process to get the resultset to be
	 * returned.
	 * 
	 * op - Reference to the operation at the top of the stack dsp - The
	 * mechanism for constructing the execution tree execRow - Structure of the
	 * return row specifying the columns and datatypes of the columns
	 * 
	 */

	@Override
	public DataSet<LocatedRow> getDataSet(SpliceOperation op,
			DataSetProcessor dsp, ExecRow execRow) throws StandardException {
		operationContext = dsp.createOperationContext(op);
		ArrayList<LocatedRow> items = new ArrayList<LocatedRow>();

		try {

			// Get the HDFS File System, to read the ORC files
			FileSystem filesystem;
			filesystem = FileSystem.get(HConfiguration.unwrapDelegate());
			// filesystem = FileSystem.get(new Configuration());

			// Create the iterator to read the records in ORC files.
			ORCRecordReader it;
			it = new ORCRecordReader(filesystem, new Path(fileName), execRow);

			// Changed to split Iterable and iterator, and close is implemented
			// by Iterator
			// op.registerCloseable(it);

			// Create DataSet of the records to return
			return dsp.createDataSet(it);

		} catch (IOException e) {
			LOG.error("Unexpected IO Exception: " + this.fileName, e);

		} finally {
			operationContext.popScope();
		}
		return new ControlDataSet<>(items);
	}

	/**
	 * The estimated number of rows returned
	 */

	@Override
	public double getEstimatedRowCount(VTIEnvironment vtiEnvironment)
			throws SQLException {

		return VTICosting.defaultEstimatedRowCount;
	}

	/**
	 * The estimated cost
	 */
	@Override
	public double getEstimatedCostPerInstantiation(VTIEnvironment vtiEnvironment)
			throws SQLException {

		return VTICosting.defaultEstimatedCost;
	}

	@Override
	public boolean supportsMultipleInstantiations(VTIEnvironment vtiEnvironment)
			throws SQLException {
		return false;
	}

	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		throw new SQLException("not supported");
	}

	@Override
	public OperationContext getOperationContext() {
		return operationContext;
	}

	public String getFileName() {
		return fileName;
	}
}
