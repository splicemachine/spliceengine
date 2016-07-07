/*

   Derby - Class org.apache.derby.impl.sql.execute.ValueRow

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.impl.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import scala.util.hashing.MurmurHash3;

/**
	Basic implementation of ExecRow.

 */
public class ValueRow implements ExecRow, Externalizable, Comparable<ExecRow> {
	///////////////////////////////////////////////////////////////////////
	//
	//	STATE
	//
	///////////////////////////////////////////////////////////////////////

	private DataValueDescriptor[] column;
	private int ncols;

	///////////////////////////////////////////////////////////////////////
	//
	//	CONSTRUCTORS
	//
	///////////////////////////////////////////////////////////////////////

    /** Empty constructor for serialization */
    public ValueRow() {
    }

	/**
	  *	Make a value row with a designated number of column slots.
	  *
	  *	@param	ncols	number of columns to allocate
	  */
	public ValueRow(int ncols)
	{
		 column = new DataValueDescriptor[ncols];
		 this.ncols = ncols;
	}


	///////////////////////////////////////////////////////////////////////
	//
	//	EXECROW INTERFACE
	//
	///////////////////////////////////////////////////////////////////////

	// this is the actual current # of columns
	public int nColumns() {
		return ncols;
	}

	// get a new Object[] for the row
	public void getNewObjectArray()
	{
		column = new DataValueDescriptor[ncols];
	}

	/*
	 * Row interface
	 */
	// position is 1-based
	public DataValueDescriptor	getColumn (int position) {
        try {
            return column[position-1];
        } catch (Exception e) {
            return (DataValueDescriptor)null;
        }
    }

	// position is 1-based.
	public void setColumn(int position, DataValueDescriptor col) {
        try {
            column[position-1] = col;
        } catch (Exception e) {
            realloc(position);
            column[position-1] = col;
        }
    }


	/*
	** ExecRow interface
	*/

	// position is 1-based
	public ExecRow getClone() 
	{
		return getClone((FormatableBitSet) null);
	}

	public ExecRow getClone(FormatableBitSet clonedCols)
	{
		int numColumns = column.length;

		/* Get the right type of row */
		ExecRow rowClone = cloneMe();

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			// Copy those columns whose bit isn't set (and there is a FormatableBitSet)
			if (clonedCols != null && !(clonedCols.get(colCtr + 1)))
			{
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, (DataValueDescriptor) column[colCtr]);
				continue;
			}

			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
                rowClone.setColumn(colCtr +1, column[colCtr].cloneValue(false));
			}
		}
		return rowClone;
	}

	// position is 1-based
	public ExecRow getNewNullRow()
	{
		int numColumns = column.length;
		ExecRow rowClone = cloneMe();


		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
				/* Rows are 1-based, column[] is 0-based */
                rowClone.setColumn(colCtr + 1, column[colCtr].getNewNull());
			}
		}
		return rowClone;
	}

	public ExecRow cloneMe() {
		return new ValueRow(ncols);
	}

    /**
     * Reset all columns in the row array to null values.
     */
    public void resetRowArray() {
        for (int i = 0; i < column.length; i++) {
            if (column[i] != null) {
                column[i] = column[i].recycle();
            }
        }
    }

	// position is 1-based
	public final DataValueDescriptor cloneColumn(int columnPosition)
	{
        return column[columnPosition -1].cloneValue(false);
	}

	/*
	 * class interface
	 */
	public String toString() {
		// NOTE: This method is required for external functionality (the
		// consistency checker), so do not put it under SanityManager.DEBUG.
		String s = "{ ";
		for (int i = 0; i < column.length; i++)
		{
			if (column[i] == null)
				s += "null";
			else
				s += column[i].toString();
			if (i < (column.length - 1))
				s += ", ";
		}
		s += " }";
		return s;
	}


	/**
		Get the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArray() {
		return column;
	}

	/**
		Get a clone of the array form of the row that Access expects.

		@see ExecRow#getRowArray
	*/
	public DataValueDescriptor[] getRowArrayClone() 
	{
		int numColumns = column.length;
		DataValueDescriptor[] columnClones = new DataValueDescriptor[numColumns];

		for (int colCtr = 0; colCtr < numColumns; colCtr++) 
		{
			if (column[colCtr] != null)
			{
                columnClones[colCtr] = column[colCtr].cloneValue(false);
			}
		}

		return columnClones;
	}

	/**
	 * Set the row array
	 *
	 * @see ExecRow#setRowArray
	 */
	public void setRowArray(DataValueDescriptor[] value)
	{
		column = value;
	}
		
	// Set the number of columns in the row to ncols, preserving
	// the existing contents.
	protected void realloc(int ncols) {
		DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];

		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;
	}
	
	@Override
	public int compareTo(ExecRow row) {
		if (row == null)
			return -1;
		if (ncols != row.nColumns())
			return -1;
		int compare;
		for (int i = 1; i == ncols; i++ ) {
			try {
				compare = getColumn(i).compare(row.getColumn(i));
				if (compare != 0)
					return compare;
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return 0;
	}

    @Override
    public int compareTo(int[] compareKeys, ExecRow row) {
        if (row == null)
            return -1;
        if (ncols != row.nColumns())
            return -1;
        int compare;
        for (int i = 0; i < compareKeys.length; i++ ) {
            try {
                compare = getColumn(compareKeys[i]).compare(row.getColumn(compareKeys[i]));
                if (compare != 0)
                    return compare;
            } catch (StandardException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return 0;
    }


	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(ncols);
		for (DataValueDescriptor desc: column) {
			out.writeObject(desc);
		}
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		ncols = in.readInt();
		column = new DataValueDescriptor[ncols];
		for (int i = 0; i < ncols; i++) {
			column[i] = (DataValueDescriptor) in.readObject();
		}
	}

    public int hashCode() {
		return MurmurHash3.arrayHashing().hash(column);
    }

    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        ValueRow other = (ValueRow) obj;
        if (!Arrays.equals(column, other.column))
            return false;
        if (ncols != other.ncols)
            return false;
        return true;
    }

    @Override
    public ExecRow getKeyedExecRow(int[] keyColumns) throws StandardException {
        ValueRow key = new ValueRow(keyColumns.length);
        int position = 1;
        for (int keyColumn : keyColumns) {
            key.setColumn(position++, getColumn(keyColumn + 1));
        }
        return key;
    }

    @Override
    public int hashCode(int[] keysToHash) {
        final int prime = 31;
        int result = 1;
        if (column == null)
            return 0;
        for (int hashKey: keysToHash) {
            result = 31 * result + (column[hashKey] == null ? 0 : column[hashKey].hashCode());
        }
        result = prime * result + keysToHash.length;
        return result;
    }

}
