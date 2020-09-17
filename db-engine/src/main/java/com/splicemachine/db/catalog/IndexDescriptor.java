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

package com.splicemachine.db.catalog;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.ByteArray;
import com.splicemachine.db.impl.sql.execute.BaseExecutableIndexExpression;

/**
 *	
 * This interface describes an index.
 * 
 * It is used in the column SYS.SYSCONGLOMERATES.DESCRIPTOR
 * and describes everything about an index except the index name and 
 * the table on which the index is defined.
 * That information is available 
 * in the columns NAME and TABLEID of the table SYS.SYSCONGLOMERATES.
 */
public interface IndexDescriptor
{
	/**
	 * Returns true if the index is unique.
	 */
	boolean			isUnique();
	/**
	 * Returns true if the index is duplicate keys only for null key parts. 
     * This is effective only if isUnique is false.
	 */
	boolean			isUniqueWithDuplicateNulls();

	/**
	 * Returns an array of column positions in the base table.  Each index
	 * column corresponds to a column position in the base table, except
	 * the column representing the location of the row in the base table.
	 * The returned array holds the column positions in the
	 * base table, so, if entry 2 is the number 4, the second
	 * column in the index is the fourth column in the table.
	 */
	int[]	baseColumnPositions();

	/**
     * Returns the postion of a column.
     * <p>
	 * Returns the position of a column within the key (1-based).
	 * 0 means that the column is not in the key.  Same as the above
	 * method, but it uses int instead of Integer.
	 */
	int getKeyColumnPosition(int heapColumnPosition);

	/**
	 * Returns the number of ordered columns.  
     * <p>
	 * In the future, it will be
	 * possible to store non-ordered columns in an index.  These will be
	 * useful for covered queries.  The ordered columns will be at the
	 * beginning of the index row, and they will be followed by the
	 * non-ordered columns.
	 *
	 * For now, all columns in an index must be ordered.
	 */
	int				numberOfOrderedColumns();

	/**
	 * Returns the type of the index.  For now, we only support B-Trees,
	 * so the value "BTREE" is returned.
	 */
	String			indexType();

	/**
	 * Returns the index column types.
	 */
	DataTypeDescriptor[] getIndexColumnTypes();

	/**
	 * Returns array of boolean telling asc/desc info for each index
	 * key column for convenience of using together with baseColumnPositions
	 * method.  Both methods return an array with subscript starting from 0.
	 */
	boolean[]	isAscending();

	/**
	 * Returns true if the specified column is ascending in the index
	 * (1-based).
	 */
	boolean			isAscending(Integer keyColumnPosition);

	/**
	 * Returns true if the specified column is descending in the index
	 * (1-based).  In the current release, only ascending columns are
	 * supported.
	 */
	boolean			isDescending(Integer keyColumnPosition);

	/**
	 * set the baseColumnPositions field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where baseColumnPositions is changed.
	 */
	void     setBaseColumnPositions(int[] baseColumnPositions);

	/**
	 * set the isAscending field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where isAscending is changed.
	 */
	void     setIsAscending(boolean[] isAscending);

	/**
	 * set the numberOfOrderedColumns field of the index descriptor.  This
	 * is for updating the field in operations such as "alter table drop
	 * column" where numberOfOrderedColumns is changed.
	 */
	void     setNumberOfOrderedColumns(int numberOfOrderedColumns);

    /**
     * Checks whether the index descriptor is a primary key.
     * @return
     */
	boolean isPrimaryKey();

	boolean excludeNulls();

	boolean excludeDefaults();

	/**
	 * Get the original index expression texts in SQL statement.
	 */
	String[] getExprTexts();

	/**
	 * Get the generated byte code of index expressions.
	 */
	ByteArray[] getExprBytecode();

	/**
	 * Get the class name of the generated byte code.
	 */
	String[] getGeneratedClassNames();

	/**
	 * Checks whether this index is created on expressions.
	 */
	boolean isOnExpression();

	/**
	 * Returns an instance of the generated class of index expressions.
	 * @param indexColumnPosition 0-based index column position number
	 */
	BaseExecutableIndexExpression getExecutableIndexExpression(int indexColumnPosition)
			throws StandardException;

}
