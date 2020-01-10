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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.sql;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.error.StandardException;

/**
 * The Row interface provides methods to get information about the columns
 * in a result row.
 * It uses simple, position (1-based) access to get to columns.
 * Searching for columns by name should be done from the ResultSet
 * interface, where metadata about the rows and columns is available.
 * <p>
 *
 * @see ResultSet
 *
 * @see com.splicemachine.db.iapi.sql.execute.ExecRow
 */

public interface Row
{
	int nColumns();

	/**
	 * Get a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 *
     * @exception   StandardException Thrown on failure.
	 * @return		The DataValueDescriptor, null if no such column exists
	 */
	DataValueDescriptor	getColumn (int position) throws StandardException;

	/**
	 * Set a DataValueDescriptor in a Row by ordinal position (1-based).
	 *
	 * @param position	The ordinal position of the column.
	 */
	void	setColumn (int position, DataValueDescriptor value);

    /**
     * Set a DataValueDescriptor in a Row by ordinal position (1-based),
     * preserving the data type of the target column.
     * For example, if we are setting (SQLDouble)null = (SQLInteger)1,
     * The column will end up holding (SQLDouble)1.0, preserving
     * the data type of the DVD that is being overwritten.
     *
     * @param position  The ordinal position of the column.
	 */
     void   setColumnValue (int position, DataValueDescriptor value) throws StandardException;
}
