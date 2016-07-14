/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils;

import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactory;
import com.splicemachine.db.iapi.types.StringDataValue;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Serializable version of an ExecRow.
 *
 * @author Scott Fines
 * Created: Jan 29, 2013
 */
public class SerializingExecRow implements ExecRow, Externalizable {
    private static final long serialVersionUID = 2l;
    private ExecRow delegate;
    private int[] nullEntries;

    /**
     * Used only for serialization, do not use!
     */
    @Deprecated
    public SerializingExecRow() {
    }

    public SerializingExecRow(ExecRow delegate) {
        this.delegate = delegate;
    }

    @Override
    public ExecRow getClone() {
        return delegate.getClone();
    }

    @Override
    public ExecRow getClone(FormatableBitSet clonedCols) {
        return delegate.getClone(clonedCols);
    }

    @Override
    public ExecRow getNewNullRow() {
        return delegate.getNewNullRow();
    }

    @Override
    public void resetRowArray() {
        delegate.resetRowArray();
    }

    @Override
    public DataValueDescriptor cloneColumn(int columnPosition) {
        return delegate.cloneColumn(columnPosition);
    }

    @Override
    public DataValueDescriptor[] getRowArrayClone() {
        return delegate.getRowArrayClone();
    }

    @Override
    public DataValueDescriptor[] getRowArray() {
        return delegate.getRowArray();
    }

    @Override
    public void setRowArray(DataValueDescriptor[] rowArray) {
        delegate.setRowArray(rowArray);
    }

    @Override
    public void getNewObjectArray() {
        delegate.getNewObjectArray();
    }

    @Override
    public int nColumns() {
        return delegate.nColumns();
    }

    @Override
    public DataValueDescriptor getColumn(int position) throws StandardException {
        return delegate.getColumn(position);
    }

    @Override
    public void setColumn(int position, DataValueDescriptor value) {
        delegate.setColumn(position, value);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //write the total number of columns
        out.writeInt(nColumns());

		/*
		 * Write the DataValueDescriptors.
		 *
		 * Because some DataValueDescriptors don't serialize unless they are non-null,
		 * this block sparsely-writes only the non-null Descriptors using the format
		 * < total number of columns>
		 * < number of non-null columns>
		 * 	for each non-null Descriptor:
		 *    < position of the descriptor>
		 *    < value of the descriptor>
		 *  for each null descriptor:
		 *      <position of the descriptor>
		 *      <jdbc type of the descriptor>
		 */
        DataValueDescriptor[] dvds = getRowArray();
        Map<DataValueDescriptor, Integer> nonNullEntries = new HashMap<DataValueDescriptor, Integer>(dvds.length);
        Map<Integer, DataValueDescriptor> nullEntries = new HashMap<Integer, DataValueDescriptor>(dvds.length);
        for (int pos = 0; pos < dvds.length; pos++) {
            DataValueDescriptor dvd = dvds[pos];
            if (dvd != null && !dvd.isNull()) {
                nonNullEntries.put(dvd, pos);
            } else if (dvd != null) {
                nullEntries.put(pos, dvd);
            }
        }
        out.writeInt(nonNullEntries.size());
        for(Map.Entry<DataValueDescriptor,Integer> entry:nonNullEntries.entrySet()){
            DataValueDescriptor dvd = entry.getKey();
            out.writeInt(entry.getValue());
            out.writeObject(dvd);
        }
        out.writeInt(nullEntries.size());
        for(Map.Entry<Integer,DataValueDescriptor> entry:nullEntries.entrySet()){
            out.writeInt(entry.getKey());
            DataValueDescriptor dvd = entry.getValue();
            out.writeInt(dvd.getTypeFormatId());
        }

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        delegate = new ValueRow(in.readInt());
        int nonNull = in.readInt();
        for (int i = 0; i < nonNull; i++) {
            int pos = in.readInt();
            DataValueDescriptor dvd = (DataValueDescriptor) in.readObject();
            delegate.getRowArray()[pos] = dvd;
        }
        nullEntries = new int[delegate.nColumns()];
        for (int i = 0; i < nullEntries.length; i++) {
            nullEntries[i] = -1;
        }
        int nullSize = in.readInt();
        for (int i = 0; i < nullSize; i++) {
            int pos = in.readInt();
            int storedFormatId = in.readInt();
            nullEntries[pos] = storedFormatId;
        }
    }

    public void populateNulls(DataValueFactory dataValueFactory) throws StandardException {
        for (int i = 0; i < nullEntries.length; i++) {
            int nullEntry = nullEntries[i];
            if (nullEntry != -1) {
                delegate.getRowArray()[i] = dataValueFactory.getNull(nullEntry, 0);
            }
        }
    }

    @Override
    public ExecRow getKeyedExecRow(int[] ints) throws StandardException {
        return delegate.getKeyedExecRow(ints);
    }


    @Override
    public int hashCode(int[] ints) {
        return delegate.hashCode(ints);
    }

    @Override
    public int compareTo(int[] ints, ExecRow execRow) {
        return delegate.compareTo(ints, execRow);
    }
}