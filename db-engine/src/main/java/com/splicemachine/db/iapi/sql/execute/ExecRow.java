/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql.execute;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Row;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.spark.sql.types.StructType;

import java.util.BitSet;
import java.util.Comparator;

/**
 * Execution sees this extension of Row that provides connectivity
 * to the Storage row interface and additional methods for manipulating
 * Rows in execution's ResultSets.
 *
 */
public interface ExecRow extends Row, KeyableRow, org.apache.spark.sql.Row, Comparable<ExecRow>, Comparator<ExecRow> {

	/**
	 * Clone the Row and its contents.
	 *
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone();

	/**
	 * Clone the Row.  The cloned row will contain clones of the
	 * specified columns and the same object as the original row
	 * for the other columns.
	 *
	 * @param clonedCols	1-based FormatableBitSet representing the columns to clone.
	 *
	 * @return Row	A clone of the Row and its contents.
	 */
	ExecRow getClone(FormatableBitSet clonedCols);

	/**
	 * Get a new row with the same columns type as this one, containing nulls.
	 *
	 */
	ExecRow	getNewNullRow();

    /**
     * Reset all the <code>DataValueDescriptor</code>s in the row array to
     * (SQL) null values. This method may reuse (and therefore modify) the
     * objects currently contained in the row array.
     */
    void resetRowArray();

	/**
	 * Get a clone of a DataValueDescriptor from an ExecRow.
	 *
	 * @param columnPosition (1 based)
	 */
	DataValueDescriptor cloneColumn(int columnPosition);

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone();

	/**
		Return the array of objects that the store needs.
	*/
	public DataValueDescriptor[] getRowArray();

	/**
		Set the array of objects
	*/
	public void setRowArray(DataValueDescriptor[] rowArray);

	/**
		Get a new DataValueDescriptor[]
	 */
	public void getNewObjectArray();

	public StructType createStructType();

	public org.apache.spark.sql.Row getSparkRow();

	public ExecRow fromSparkRow(org.apache.spark.sql.Row row);

	public long getRowSize() throws StandardException;

	public long getRowSize(BitSet validColumns) throws StandardException;

}
