package com.splicemachine.derby.utils;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Serializable version of an IndexRow.
 *
 * @author Scott Fines
 * Created: 1/29/13 11:20 AM
 */
public class SerializingIndexRow implements ExecIndexRow,Externalizable {
	private static final long serialVersionUID = 2l;
	private ExecRow valueRow;

    private boolean[] orderedNulls;

    @Deprecated
    public SerializingIndexRow(){}

	public SerializingIndexRow(ExecRow valueRow) {
		this.valueRow = valueRow instanceof SerializingExecRow ? (SerializingExecRow)valueRow:new SerializingExecRow(valueRow);
        orderedNulls = new boolean[valueRow.nColumns()];
        if(valueRow instanceof ExecIndexRow){
            ExecIndexRow val = (ExecIndexRow)valueRow;
            for(int col=0;col<valueRow.nColumns();col++){
                orderedNulls[col] = val.areNullsOrdered(col);
            }
        }
	}


	/*
	 * class interface
	 */
	@Override public String toString() { return valueRow.toString(); }
	@Override public DataValueDescriptor[] getRowArray() { return valueRow.getRowArray(); }
	@Override public void setRowArray(DataValueDescriptor[] value) { valueRow.setRowArray(value); }
	@Override public DataValueDescriptor[] getRowArrayClone() { return valueRow.getRowArrayClone(); }

	// this is the actual current # of columns
	@Override public int nColumns() { return valueRow.nColumns(); }

	/*
	 * Row interface
	 */
	// position is 1-based
	@Override
	public DataValueDescriptor	getColumn (int position) throws StandardException {
		return valueRow.getColumn(position);
	}

	// position is 1-based.
	@Override
	public void setColumn(int position, DataValueDescriptor col) {
		valueRow.setColumn(position, col);
	}

	// position is 1-based
	@Override
	public ExecRow getClone() {
		return new SerializingIndexRow(valueRow.getClone());
	}

	@Override
	public ExecRow getClone(FormatableBitSet clonedCols) {
		return new SerializingIndexRow(valueRow.getClone(clonedCols));
	}

	@Override
	public ExecRow getNewNullRow() {
		return new SerializingIndexRow(valueRow.getNewNullRow());
	}

	/**
	 * Reset all columns in the row array to null values.
	 */
	@Override public void resetRowArray() { valueRow.resetRowArray(); }

	// position is 1-based
	@Override
	public DataValueDescriptor cloneColumn(int columnPosition) {
		return valueRow.cloneColumn(columnPosition);
	}

	/*
	 * ExecIndexRow interface
	 */
	@Override public void orderedNulls(int columnPosition) {
        orderedNulls[columnPosition] = true;
    }
	@Override public boolean areNullsOrdered(int columnPosition) {
        return orderedNulls[columnPosition];
    }

	@Override public void execRowToExecIndexRow(ExecRow valueRow) { this.valueRow = valueRow; }
	@Override public void getNewObjectArray() { valueRow.getNewObjectArray(); }

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		//delegate to the valueRow, and rely on the fact that we wrap out the value
		//row with a serializable form in the constructor.
		out.writeObject(valueRow);
        ArrayUtil.writeBooleanArray(out,orderedNulls);
    }

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		valueRow = (ExecRow)in.readObject();
        orderedNulls = ArrayUtil.readBooleanArray(in);
	}

    public void populateNullValues(DataValueFactory dvf) throws StandardException {
        if(valueRow instanceof SerializingExecRow){ //always true
            ((SerializingExecRow)valueRow).populateNulls(dvf);
        }
    }
}
