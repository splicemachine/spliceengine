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

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Vector;

/**
  A set of static utility methods to work with rows.
  <P>
  A row or partial row is described by two or three parameters.
  <OL>
  <LI>DataValueDescriptor[] row - an array of objects, one per column.
  <LI>FormatableBitSet validColumns - 
      an indication of which objects in row map to which columns
  </OL>
  These objects can describe a complete row or a partial row. A partial row is 
  one where a sub-set (e.g. columns 0, 4 and 7) of the columns are supplied 
  for update, or requested to be fetched on a read.  Here's an example
  of code to set up a partial column list to fetch the 0th (type FOO), 
  4th (type BAR), and 7th (type MMM) columns from a row with 10 columns, note
  that the format for a partial row changed from a "packed" representation
  in the 3.0 release to a "sparse" representation in later releases:

  <blockquote><pre>

  // allocate/initialize the row 
  DataValueDescriptor row = new DataValueDescriptor[10]
  row[0] = new FOO();
  row[4] = new BAR();
  row[7] = new MMM();
  
  // allocate/initialize the bit set 
  FormatableBitSet FormatableBitSet = new FormatableBitSet(10);
  
  FormatableBitSet.set(0);
  FormatableBitSet.set(4);
  FormatableBitSet.set(7);
  </blockquote></pre>


  <BR><B>Column mapping<B><BR>
  When validColumns is null:
  <UL>
  <LI> The number of columns is given by row.length
  <LI> Column N maps to row[N], where column numbers start at zero.
  </UL>
  <BR>
  When validColumns is not null, then
  <UL>
  <LI> The number of requested columns is given by the number of bits set in 
       validColumns.
  <LI> Column N is not in the partial row if validColumns.isSet(N) 
       returns false.
  <LI> Column N is in the partial row if validColumns.isSet(N) returns true.
  <LI> If column N is in the partial row then it maps to row[N].
	   If N >= row.length then the column is taken as non existent for an
	   insert or update, and not fetched on a fetch.
  </UL>
  If row.length is greater than the number of columns indicated by validColumns
  the extra entries are ignored.

**/
public class RowUtil
{
	private RowUtil() {}

	/**
		An object that can be used on a fetch to indicate no fields
		need to be fetched.
	*/
	public static final DataValueDescriptor[] EMPTY_ROW = 
        new DataValueDescriptor[0];

	/**
		An object that can be used on a fetch as a FormatableBitSet to indicate no fields
		need to be fetched.
	*/
	public static final FormatableBitSet EMPTY_ROW_BITSET  = 
        new FormatableBitSet(0);

	/**
		Get the object for a column identifer (0 based) from a complete or 
        partial row.

		@param row the row
		@param columnList valid columns in the row
		@param columnId which column to return (0 based)

		@return the obejct for the column, or null if the column is not represented.
	*/
	public static DataValueDescriptor getColumn(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 columnList, 
    int                     columnId) 
    {

		if (columnList == null)
			return columnId < row.length ? row[columnId] : null;


		if (!(columnList.getLength() > columnId && columnList.isSet(columnId)))
			return null;

        return columnId < row.length ? row[columnId] : null;

	}

	public static Object getColumn(
    Object[]   row, 
    FormatableBitSet                 columnList, 
    int                     columnId) 
    {

		if (columnList == null)
			return columnId < row.length ? row[columnId] : null;


		if (!(columnList.getLength() > columnId && columnList.isSet(columnId)))
			return null;

        return columnId < row.length ? row[columnId] : null;

	}

	/**
		Get a FormatableBitSet representing all the columns represented in
		a qualifier list.

		@return a FormatableBitSet describing the valid columns.
	*/
	public static FormatableBitSet getQualifierBitSet(Qualifier[][] qualifiers) 
    {
		FormatableBitSet qualifierColumnList = new FormatableBitSet();

		if (qualifiers != null) 
        {
            for (Qualifier[] qualifier : qualifiers) {
                for (int j = 0; j < qualifier.length; j++) {
                    int colId = qualifier[j].getColumnId();

                    // we are about to set bit colId, need length to be colId+1
                    qualifierColumnList.grow(colId + 1);
                    qualifierColumnList.set(colId);
                }
            }
		}

		return qualifierColumnList;
	}

    /**
     * Get the number of columns represented by a FormatableBitSet.
     * <p>
     * This is simply a count of the number of bits set in the FormatableBitSet.
     * <p>
     *
     * @param maxColumnNumber Because the FormatableBitSet.size() can't be used as
     *                        the number of columns, allow caller to tell
     *                        the maximum column number if it knows.  
     *                        -1  means caller does not know.
     *                        >=0 number is the largest column number.
     *                           
     * @param columnList valid columns in the row
     *
	 * @return The number of columns represented in the FormatableBitSet.
     **/
    public static int getNumberOfColumns(
    int     maxColumnNumber,
    FormatableBitSet  columnList)
    {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(columnList != null);

        int max_col_number = columnList.getLength();

		if (maxColumnNumber > 0 && maxColumnNumber < max_col_number)
			max_col_number = maxColumnNumber;

        int ret_num_cols = 0;

        for (int i = 0; i < max_col_number; i++)
        {
            if (columnList.isSet(i))
                ret_num_cols++;
        }

        return(ret_num_cols);
    }

	/**
		See if a row actually contains no columns.
		Returns true if row is null or row.length is zero.

		@return true if row is empty.
	*/
	public static boolean isRowEmpty(
    DataValueDescriptor[]   row) {

        return row == null || row.length == 0;

    }

	/**
		Return the column number of the first column out of range, or a number
        less than zero if all columns are in range.
	*/
	public static int columnOutOfRange(
    DataValueDescriptor[]   row, 
    FormatableBitSet                 columnList, 
    int                     maxColumns) 
    {

		if (columnList == null) {
			if (row.length > maxColumns)
				return maxColumns;

			return -1;
		}

		int size = columnList.getLength();
		for (int i = maxColumns; i < size; i++) {
			if (columnList.isSet(i))
				return i;
		}

		return -1;
	}

	/**
		Get the next valid column after or including start column.
		Returns -1 if no valid columns exist after startColumn
	*/
	public static int nextColumn(
    Object[]   row, 
    FormatableBitSet                 columnList, 
    int                     startColumn) 
    {

		if (columnList != null) {

			int size = columnList.getLength();

			for (; startColumn < size; startColumn++) {
				if (columnList.isSet(startColumn)) {
					return startColumn;
				}
			}

			return -1;
		}

		if (row == null)
			return -1;

		return startColumn < row.length ? startColumn : -1;
	}

    /**************************************************************************
     * Public Methods dealing with cloning and row copying util functions
     **************************************************************************
     */

    /**
     * Generate a template row of DataValueDescriptor's
     * <p>
     * Generate an array of DataValueDescriptor objects which will be used to 
     * make calls to newRowFromClassInfoTemplate(), to repeatedly and
     * efficiently generate new rows.  This is important for certain 
     * applications like the sorter and fetchSet which generate large numbers
     * of "new" empty rows.
     * <p>
     *
	 * @return The new row.
     *
     * @param column_list A bit set indicating which columns to include in row.
     * @param format_ids  an array of format id's, one per column in row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static DataValueDescriptor[] newTemplate(
    DataValueFactory    dvf,
    FormatableBitSet    column_list,
    int[]               format_ids,
    int[]               collation_ids) 
        throws StandardException
    {
        int                   num_cols = format_ids.length;
        DataValueDescriptor[] ret_row  = new DataValueDescriptor[num_cols];

		int column_listSize = 
            (column_list == null) ? 0 : column_list.getLength();

        for (int i = 0; i < num_cols; i++)
        {
            // does caller want this column?
            if ((column_list != null)   && 
                !((column_listSize > i) && 
                (column_list.isSet(i))))
            {
                // no - column should be skipped.
            }
            else
            {
                // yes - create the column 

                // get empty instance of object identified by the format id.

                ret_row[i] = dvf.getNull(format_ids[i], collation_ids[i]);
            }
        }

        return(ret_row);
    }

    /**
     * Generate an "empty" row from an array of DataValueDescriptor objects.
     * <p>
     * Generate an array of new'd objects by using the getNewNull()
     * method on each of the DataValueDescriptor objects.  
     * <p>
     *
	 * @return The new row.
     *
     * @param  template            An array of DataValueDescriptor objects 
     *                             each of which can be used to create a new 
     *                             instance of the appropriate type to build a 
     *                             new empty template row.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
    public static DataValueDescriptor[] newRowFromTemplate(
    DataValueDescriptor[]    template) 
        throws StandardException
    {

        DataValueDescriptor[] columns = 
            new DataValueDescriptor[template.length];

        
        for (int column_index = template.length; column_index-- > 0;)
        {
            if (template[column_index] != null)
            {
                // get empty instance of DataValueDescriptor identified by 
                // the format id.
                columns[column_index] = template[column_index].getNewNull();
            }
        }

		return columns;
    }


    /**
     * return string version of row.
     * <p>
     * For debugging only. 
     *
	 * @return The string version of row.
     *
     * @param row The row.
     *
     **/
    public static String toString(Object[] row)
    {
        if (SanityManager.DEBUG)
        {

            StringBuilder str = new StringBuilder();

            if (row != null)
            {
                if (row.length == 0)
                {
                    str = new StringBuilder("empty row");
                }
                else
                {
                    for (int i = 0; i < row.length; i++)
                        str.append("col[").append(i).append("]=").append(row[i]);
                }
            }
            else
            {
                str = new StringBuilder("row is null");
            }

            return(str.toString());
        }
        else
        {
            return(null);
        }
    }

    /**
     * return string version of a HashTable returned from a FetchSet.
     * <p>
     *
	 * @return The string version of row.
     *
     *
     **/

    // For debugging only. 
    public static String toString(Hashtable hash_table)
    {
        if (SanityManager.DEBUG)
        {
            StringBuilder str = new StringBuilder();

            Object  row_or_vector;

            for (Enumeration e = hash_table.elements(); e.hasMoreElements();)
            {
                row_or_vector = e.nextElement();

                if (row_or_vector instanceof Object[])
                {
                    // it's a row
                    str.append(RowUtil.toString((Object[]) row_or_vector));
                    str.append("\n");
                }
                else if (row_or_vector instanceof Vector)
                {
                    // it's a vector
                    Vector vec = (Vector) row_or_vector;

                    for (int i = 0; i < vec.size(); i++)
                    {
                        str.append("vec[").append(i).append("]:").append(RowUtil.toString((Object[]) vec.get(i)));

                        str.append("\n");
                    }
                }
                else
                {
                    str.append("BAD ENTRY\n");
                }
            }
            return(str.toString());
        }
        else
        {
            return(null);
        }
    }

    /**
     * Process the qualifier list on the row, return true if it qualifies.
     * <p>
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is 
     * optimized for the more frequent where no OR's are present.  The first 
     * array slot is always a list of AND's to be treated as described above 
     * for single dimensional AND qualifier arrays.  The subsequent slots are 
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array 
     * qual[][] argument is to be treated as the following, note if 
     * qual.length = 1 then only the first array is valid and it is and an 
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
     * 
	 * @return true if the row qualifies.
     *
     * @param row               The row being qualified.
     * @param qual_list         2 dimensional array representing conjunctive
     *                          normal form of simple qualifiers.
     *
	 * @exception  StandardException  Standard exception policy.
     **/
	public static final boolean qualifyRow(
    DataValueDescriptor[]        row, 
    Qualifier[][]   qual_list)
		 throws StandardException
	{
        boolean     row_qualifies = true;

        if (SanityManager.DEBUG)
        {
            SanityManager.ASSERT(row != null);
        }

        // First do the qual[0] which is an array of qualifer terms.

        if (SanityManager.DEBUG)
        {
            // routine should not be called if there is no qualifier
            SanityManager.ASSERT(qual_list != null);
            SanityManager.ASSERT(qual_list.length > 0);
        }

        for (int i = 0; i < qual_list[0].length; i++)
        {
            // process each AND clause 

            row_qualifies = false;

            // process each OR clause.

            Qualifier q = qual_list[0][i];

            // Get the column from the possibly partial row, of the 
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue = row[q.getColumnId()];

            row_qualifies =
                columnValue.compare(
                    q.getOperator(),
                    q.getOrderable(),
                    q.getOrderedNulls(),
                    q.getUnknownRV());

            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;

            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses

        for (int and_idx = 1; and_idx < qual_list.length; and_idx++)
        {
            // loop through each of the "and" clause.

            row_qualifies = false;

            if (SanityManager.DEBUG)
            {
                // Each OR clause must be non-empty.
                SanityManager.ASSERT(qual_list[and_idx].length > 0);
            }

            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++)
            {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                int       col_id = q.getColumnId();

                if (SanityManager.DEBUG)
                {
                    SanityManager.ASSERT(
                        (col_id < row.length),
                        "Qualifier is referencing a column not in the row.");
                }

                // Get the column from the possibly partial row, of the 
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue = row[q.getColumnId()];

                if (SanityManager.DEBUG)
                {
                    if (columnValue == null)
                        SanityManager.THROWASSERT(
                            "1:row = " + RowUtil.toString(row) +
                            "row.length = " + row.length +
                            ";q.getColumnId() = " + q.getColumnId());
                }

                // do the compare between the column value and value in the
                // qualifier.
                row_qualifies = 
                    columnValue.compare(
                            q.getOperator(),
                            q.getOrderable(),
                            q.getOrderedNulls(),
                            q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "processing qual[" + and_idx + "][" + or_idx + "] = " + qual_list[and_idx][or_idx] );

                // SanityManager.DEBUG_PRINT("StoredPage.qual", "value = " + row_qualifies);

                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd" 
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }

        return(row_qualifies);
    }

}
