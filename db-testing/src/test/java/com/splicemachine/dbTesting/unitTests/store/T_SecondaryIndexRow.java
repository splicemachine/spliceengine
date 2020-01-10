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

package com.splicemachine.dbTesting.unitTests.store;

import com.splicemachine.db.iapi.services.sanity.SanityManager;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.DataValueDescriptor;

import com.splicemachine.db.iapi.types.RowLocation;


/**
This class implements a row which will be stored in a secondary index on 
a heap table.  
<p>
This class creates a new DataValueDescriptor array which will be the row used 
to insert into the secondary index.  The fields of this object array are made 
up of references to DataValueDescriptors provided by the caller: the 
DataValueDescriptors in the template and a RowLocation.
The interface is designed to support the standard access method interface
where callers provide a single template and then read rows into that template
over and over.  This class keeps a reference to the objects in the template
and the rowlocation, 
so the state of this object changes whenever the caller changes the template.
The caller provides a template which will contain a heap row,
and a RowLocation which provides the location of the row within the heap table.
<p>
So for example to create an index from a base table by reading the base table
and inserting each row one at a time into the secondary index you would 
do something like:

DataValueDescriptors[] template = get_template_for_base_table();
RowLocation            rowloc   = ScanController_var.newRowLocationTemplate();
T_SecondaryIndexRow    indrow   = new T_SecondaryIndexRow();

indrow.init(template, rowloc, numcols_in_index);

while (ScanController_variable.next())
{ 
    fetch(template)
    fetchLocation(rowloc)

    ConglomerateController_on_btree.insert(indrow.getRow());
}

**/

public class T_SecondaryIndexRow
{

    DataValueDescriptor[]     row;
    RowLocation  init_rowlocation = null;

    /* Constructors for This class: */
    public T_SecondaryIndexRow(){}

    /* Private/Protected methods of This class: */
    /* Public Methods of T_SecondaryIndexRow class: */

    /**
     * get the rows location field.
     *
	 * @return The base table row location field from the secondary index.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    /*
    private RowLocation getRowLocationField()
		throws StandardException
    {
        return(init_rowlocation);
    }
    */

    /**
     * Initialize the class.
     * <p>
     * Save away pointers to the base table template row, and the rowlocation
     * class.  Build default map of base columns to key columns, this map
     * can be changed with setMap().
     * <p>
     *
     * @param template    The template for the base table row.
     * @param rowlocation The template for the row location.
     * @param numkeys     The total number of columns in the secondary index
     *                    including the rowlocation column.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public void init(
    DataValueDescriptor[]   template,
    RowLocation             rowlocation,
    int                     numkeys)
        throws StandardException
    {
        if (SanityManager.DEBUG)
        {
            if (numkeys != (template.length + 1))
                SanityManager.THROWASSERT(
                    "numkeys = " + numkeys + 
                    " template.length = " + template.length);
        }

        init_rowlocation = rowlocation;

        /* create new object array for the row, and copy all object references 
         * from template row to new secondary index row.
         */
        row = new DataValueDescriptor[numkeys];

        System.arraycopy(template, 0, row, 0, template.length);

        /* add the reference to the row location column as the last column */
        row[row.length - 1] = rowlocation;
    }

    /**
     * Return the secondary index row.
     * <p>
     * Return the DataValueDescriptor array that represents the branch row, 
     * for use in raw store calls to fetch, insert, and update.
     * <p>
     *
	 * @return The branch row object array.
     **/
    public DataValueDescriptor[] getRow()
    {
        return(this.row);
    }

	public String toString()
	{
		String s = "{ ";
		for (int colid = 0; colid < row.length; colid++)
		{
			s += row[colid];
			if (colid < (row.length - 1))
				s += ", ";
		}
		s += " }";
		return s;
	}
}
