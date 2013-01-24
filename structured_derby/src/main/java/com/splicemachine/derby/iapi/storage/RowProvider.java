package com.splicemachine.derby.iapi.storage;

import java.util.Iterator;

import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Provides ExecRows for later stages in the execution computation.
 *
 * Originally, HBase Scans were passed around; This means that situations
 * where there are no scans were unable to function (e.g. inserting a single row, or
 *  a small collection of rows). To alleviate this issue, while still providing for
 * a mechanism for when Scans <em>are</em> useable, This interface exists.
 */
public interface RowProvider extends Iterator<ExecRow>  {

	/**
	 * Calling multiple times should result in a safe re-opening
	 * of the iterator.
	 */
	void open();
	
	/**
	 * Close the iterator
	 */
	void close();
	
	RowLocation getCurrentRowLocation();

    /**
     * Converts this RowProvider into an HBase Scan, if it is possible to do so.
     * If it is not possible to construct a scan with this provider, it will return {@code null}
     *
     * @return a Scan representation for this RowProvider, or {@code null} if a Scan representation
     * is not possible.
     */
    Scan toScan();

    /**
     * Gets the "table name" of the backing storage, or {@code null} if there is none.
     *
     * @return the table name, or {@code null} if no table name exists
     */
    byte[] getTableName();

	/**
	 * @return the number of rows which are being modified, or 0 if no rows are being modified by this (e.g.
	 * if it's a scan).
	 */
	int getModifiedRowCount();
}
