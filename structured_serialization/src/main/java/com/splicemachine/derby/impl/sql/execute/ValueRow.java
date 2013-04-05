package com.splicemachine.derby.impl.sql.execute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
Basic implementation of ExecRow.

*/
public class ValueRow implements ExecRow, Externalizable, Comparable<ExecRow> {
	private DataValueDescriptor[] column;
	private int ncols;
	public ValueRow() {
		super();
	}
	/**
	 *	Make a value row with a designated number of column slots.
	 *
	 *	@param	ncols	number of columns to allocate
	 */	
	public ValueRow(int ncols) {
		column = new DataValueDescriptor[ncols];
	 	this.ncols = ncols;
	}

	public int nColumns() {
		return ncols;
	}

	public void getNewObjectArray() {
		column = new DataValueDescriptor[ncols];
	}

	public DataValueDescriptor	getColumn (int position) {
		if (position <= column.length)
			return column[position-1];
		else
			return (DataValueDescriptor)null;
	}

	public void setColumn(int position, DataValueDescriptor col) {
		if (position > column.length)
			realloc(position); // enough for this column
		column[position-1] = col;
	}

	public ExecRow getClone() {
		return getClone((FormatableBitSet) null);
	}

	public ExecRow getClone(FormatableBitSet clonedCols) {
		int numColumns = column.length;
		/* Get the right type of row */
		ExecRow rowClone = cloneMe();

		for (int colCtr = 0; colCtr < numColumns; colCtr++) {
			// Copy those columns whose bit isn't set (and there is a FormatableBitSet)
			if (clonedCols != null && !(clonedCols.get(colCtr + 1))) {
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, (DataValueDescriptor) column[colCtr]);
				continue;
			}
			if (column[colCtr] != null) {
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr +1, column[colCtr].cloneValue(false));
			}
		}
		return rowClone;
	}

	public ExecRow getNewNullRow() {
		int numColumns = column.length;
		ExecRow rowClone = cloneMe();
		for (int colCtr = 0; colCtr < numColumns; colCtr++) {
			if (column[colCtr] != null) {
				/* Rows are 1-based, column[] is 0-based */
				rowClone.setColumn(colCtr + 1, column[colCtr].getNewNull());
			}
		}
		return rowClone;
	}

	ExecRow cloneMe() {
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

	public String toString() {
		// NOTE: This method is required for external functionality (the
		// consistency checker), so do not put it under SanityManager.DEBUG.
		String s = "{ ";
		for (int i = 0; i < column.length; i++) {
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


	public DataValueDescriptor[] getRowArray() {
		return column;
	}

	public DataValueDescriptor[] getRowArrayClone()  {
		int numColumns = column.length;
		DataValueDescriptor[] columnClones = new DataValueDescriptor[numColumns];
		for (int colCtr = 0; colCtr < numColumns; colCtr++) {
			if (column[colCtr] != null)
	            columnClones[colCtr] = column[colCtr].cloneValue(false);
		}
		return columnClones;
	}

	public void setRowArray(DataValueDescriptor[] value) {
		column = value;
	}
	
	protected void realloc(int ncols) {
		DataValueDescriptor[] newcol = new DataValueDescriptor[ncols];
		System.arraycopy(column, 0, newcol, 0, column.length);
		column = newcol;
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
	@Override
	public int compareTo(ExecRow row) {
		if (row == null)
			return -1;
		if (ncols != row.nColumns())
			return -1;
		int compare;
		System.out.println("Comparing");
		System.out.println(this);
		System.out.println(this.nColumns());
		System.out.println(row);
		System.out.println(row.nColumns());
	
		System.out.println(row);
		
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
}

