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

package com.splicemachine.db.iapi.sql.dictionary;


import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;

import java.util.ArrayList;
import java.util.Iterator;

/**
 * This represents a list of column descriptors. 
 */
public class ColumnDescriptorList extends ArrayList<ColumnDescriptor>
{
	/**
	 * Add the column.  Currently, the table id is ignored.
	 *
	 * @param tableID the table id (ignored)
	 * @param column the column to add
	 */	
	public void add(UUID tableID, ColumnDescriptor column)
	{
		/*
		** RESOLVE: The interface includes tableID because presumably
		** the primary key for the columns table will be tableID +
		** columnID (or possibly tableID + column name - both column
		** name and ID must be unique within a table).  However, the
		** ColumnDescriptor contains a reference to a tableID, so it
		** seems like we don't need the parameter here.  I am going
		** to leave it here just in case we decide we need it later.
		*/
		add(column);
	}

	/**
	 * Get the column descriptor
	 *
	 * @param tableID the table id (ignored)
	 * @param columnName the column get
	 *
	 * @return the column descriptor if found
	 */	
	public ColumnDescriptor getColumnDescriptor(UUID tableID,
							String columnName)
	{
		ColumnDescriptor	returnValue = null;

        for (ColumnDescriptor columnDescriptor : this) {
            if (columnName.equals(columnDescriptor.getColumnName()) &&
                    tableID.equals(columnDescriptor.getReferencingUUID())) {
                returnValue = columnDescriptor;
                break;
            }
        }

		return returnValue;
	}

	/**
	 * Get the column descriptor
	 *
	 * @param tableID the table id (ignored)
	 * @param columnID the column id
	 *
	 * @return the column descriptor if found
	 */	
	public ColumnDescriptor getColumnDescriptor(UUID tableID, int columnID) {
		ColumnDescriptor returnValue = get(columnID-1);
        return (returnValue !=null && tableID.equals(returnValue.getReferencingUUID()))?returnValue:null;
	}

	/**
	 * Return the nth (0-based) element in the list.
	 *
	 * @param n	Which element to return.
	 *
	 * @return The nth element in the list.
	 */
	public ColumnDescriptor elementAt(int n)
	{
		return get(n);
	}

	/**
	 * Get an array of strings for all the columns
	 * in this CDL.
	 *
	 * @return the array of strings
	 */
	public String[] getColumnNames()
	{
		String strings[] = new String[size()];

		int size = size();

		for (int index = 0; index < size; index++)
		{
			ColumnDescriptor columnDescriptor = elementAt(index);
			strings[index] = columnDescriptor.getColumnName();
		}
		return strings;
	}

    public int[] getFormatIds() throws StandardException {
        int[] formatIds;
        int numCols = size();
        formatIds = new int[numCols];
        for (int j = 0; j < numCols; ++j) {
            ColumnDescriptor columnDescriptor = elementAt(j);
            if(columnDescriptor != null) {
                DataTypeDescriptor type = columnDescriptor.getType();
                formatIds[j] = type.getNull().getTypeFormatId();
            }
        }
        return formatIds;
    }

}
