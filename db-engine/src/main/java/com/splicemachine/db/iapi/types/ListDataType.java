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

package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl.Format;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

import java.io.*;
import java.sql.ResultSet;

/**
 * ListDataType satisfies the DataValueDescriptor
 * interfaces (i.e., OrderableDataType). It implements a list of
 * DataValueDescriptors, for example (SQLLongint, SQLTimestamp, SQLChar).
 * <p>
 * The main purpose for this class is to facilitate representation
 * of multicolumn IN lists, e.g. (col_1, col2) IN ((1,2), (3,4))
 * One use of this is the conversion of equality predicates in DNF form,
 * e.g.  (c1 = 1 and c2 = 2) or (c1 = 2 and c2 = 3)
 * ... into an IN list predicate:  (c1,c2) in ((1,2), (2,3))
 * in order to enable optimizations such as MultiProbeTableScan.
 * <p>
 * A comparison operation between two ListDataTypes is true iff
 * the two ListDataTypes being compared have the same number of
 * elements and the comparison operation is true for each pairwise
 * comparison of same-positioned elements from the two lists,
 * for example, the comparison:
 * (1, '2001-01-01', 'Widget') = (1, '2001-01-01', 'Sprocket')
 * is evaluated as the following boolean expression:
 * (1 = 1) AND ('2001-01-01' = '2001-01-01') and 'Widget' = 'Sprocket'
 * <p>
 * If any of the elements of the ListDataType holds a null, the
 * ListDataType item itself can be considered as a null value since
 * (null OP X) AND (A OP B) and (C OP D) ... is never TRUE,
 * and a multicolumn IN list is never negated (we never apply
 * the NOT operator on it).
 * <p>
 */
public final class ListDataType extends DataType {

	int numElements = 0;
	DataValueDescriptor[] dvd = null;

    @Override
	public int getLength()
	{
		return numElements;
	}

	public void setLength(int len) {
        numElements = len;
        dvd = new DataValueDescriptor[len];
    }


    // this is for DataType's error generator
    @Override
    public String getTypeName() {
        return TypeId.LIST_NAME;
    }

    @Override
    public String getString() throws StandardException
    {
        String theString = new String();
        theString.concat("( ");
        for (int i = 0; i < numElements; i++) {
            String stringToAdd = dvd[i] == null ? null : dvd[i].getString();
            theString.concat(stringToAdd != null ? stringToAdd : "NULL");
            if (i != numElements - 1)
                theString.concat(", ");
        }
        theString.concat(")");
        return theString;
    }

    @Override
    public String getTraceString() throws StandardException
    {
        return getString();
    }

    @Override
    public boolean isNull() {
        boolean localIsNull = true;
        
        int i;
        for (i = 0; i < numElements; i++) {
            if (dvd[i] == null || dvd[i].isNull())
                break;
        }
        if (i == numElements)
            localIsNull = false;
        setIsNull(localIsNull);
        return localIsNull;
    }

    @Override
    public void restoreToNull()
    {
        for (int i = 0; i < numElements; i++) {
            if (dvd[i] != null)
                dvd[i].restoreToNull();
        }
        setIsNull(true);
    }

	/**
		Return my format identifier.

		@see com.splicemachine.db.iapi.services.io.TypedFormat#getTypeFormatId
	*/
    @Override
	public int getTypeFormatId() {
		return StoredFormatIds.LIST_ID;
	}

    @Override
	public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isNull);
		out.writeInt(numElements);
        for (int i = 0; i < numElements; i++) {
            out.writeBoolean(dvd[i] != null);
            if (dvd[i] != null)
                out.writeObject(dvd[i]);
        }
	}

	/** @see java.io.Externalizable#readExternal */
	@Override
	public void readExternal(ObjectInput in) throws IOException {
        isNull = in.readBoolean();
        setLength(in.readInt());
        try {
            for (int i = 0; i < numElements; i++) {
                if (in.readBoolean())
                    dvd[i] = (DataValueDescriptor) in.readObject();
            }
        }
        catch (ClassNotFoundException e) {
            throw new IOException(
                String.format("Class not found while deserializing %s", this.getClass().getName()), e);
        }
	}

    @Override
	public void readExternalFromArray(ArrayInputStream in) throws IOException {
        isNull = in.readBoolean();
        numElements = in.readInt();
        try {
            for (int i = 0; i < numElements; i++)
                if (in.readBoolean())
                    dvd[i] = (DataValueDescriptor) in.readObject();
        }
        catch(ClassNotFoundException e) {
            throw new IOException(
                String.format("Class not found while deserializing %s", this.getClass().getName()), e);
        }
	}

	/*
	 * DataValueDescriptor interface
	 */

    public void setFrom(DataValueDescriptor theValue, int index)
        throws StandardException
    {
        if (index >= numElements || index < 0)
            throw StandardException.newException(SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, "LIST");

        dvd[index] = theValue;
    }
    
    public void setFrom(ExecRow row)
        throws StandardException {
        if (row.size() != numElements)
            throw StandardException.newException(SQLState.LANG_INVALID_FUNCTION_ARGUMENT);
        
        for (int i = 0; i < row.size(); i++) {
            dvd[i] = row.getColumn(i + 1);
        }
    }

	/** @see DataValueDescriptor#cloneValue */
	@Override
	public DataValueDescriptor cloneValue(boolean forceMaterialization)
	{
	    ListDataType ldt = new ListDataType();
        ldt.setLength(numElements);
        ldt.setIsNull(getIsNull());
        for (int i = 0; i < numElements; i++)
            if (dvd[i] != null)
                ldt.dvd[i] = dvd[i].cloneValue(forceMaterialization);
		return ldt;
	}

	/**
	 * @see DataValueDescriptor#getNewNull
	 */
    @Override
	public DataValueDescriptor getNewNull()
	{
		return new ListDataType();
	}


	/*
	 * class interface
	 */

	/*
	 * constructors
	 */

	/** no-arg constructor, required by Formattable */
	public ListDataType()
	{
		isNull = true;
	}

	public ListDataType(int numElements)
	{
        isNull = true;
        setLength(numElements);
	}

	
    public DataValueDescriptor getDVD(int index) {
        return dvd[index];
    }

    public void setDVD(int index, DataValueDescriptor value) {
	    dvd[index] = value;
    }
    
	/*
	 * DataValueDescriptor interface
	 */

	/** @see DataValueDescriptor#typePrecedence */
    @Override
	public int typePrecedence()
	{
		return TypeId.LIST_PRECEDENCE;
	}
 
	private boolean predSatisfied(int result, int op)
    
    {
        switch (op) {
            case ORDER_OP_LESSTHAN:
                return (result < 0);   // this <  other
            case ORDER_OP_EQUALS:
                return (result == 0);  // this == other
            case ORDER_OP_LESSOREQUALS:
                return (result <= 0);  // this <= other
            // flipped operators
            case ORDER_OP_GREATERTHAN:
                return (result > 0);   // this > other
            case ORDER_OP_GREATEROREQUALS:
                return (result >= 0);  // this >= other
            default:
                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Invalid Operator");
                return false;
        }
    }
    /** @exception StandardException		Thrown on error */
    @Override
    public final int compare(DataValueDescriptor arg) throws StandardException {
        return compare(arg, false);
    }
    
    @Override
    public int compare(DataValueDescriptor other, boolean nullsOrderedLow)
        throws StandardException {
        assert other instanceof ListDataType : "illegal comparison of list with non-list.";
        assert other.getLength() == getLength() : "Trying to compare unequal length lists.";
        if (this.isNull() || other.isNull()) {
            if (!isNull())
                return nullsOrderedLow ? 1 : -1;
            if (!other.isNull())
                return nullsOrderedLow ? -1 : 1;
            return 0; // both null
        }
        int result;
        for (int i = 0; i < numElements; i++) {
            result = dvd[i].compare(((ListDataType) other).getDVD(i));
            // List data can be thought of as a concatenation of all the
            // list data items, so as soon as one part of that "key" is
            // greater than or less than the other, we know the result.
            if (result != 0)
                return result;
        }
        return 0;
    }
    
    @Override
    public boolean compare(int op,
						   DataValueDescriptor other,
						   boolean orderedNulls,
						   boolean unknownRV)
		throws StandardException
	{
        return compare(op, other, orderedNulls, false, unknownRV);
	}
 
	@Override
    public boolean compare(int op,
                           DataValueDescriptor other,
                           boolean orderedNulls,
                           boolean nullsOrderedLow,
                           boolean unknownRV) throws StandardException {
        
        assert other != null : "argument is null";
        assert other instanceof ListDataType : "illegal comparison of list with non-list.";
        assert other.getLength() == getLength() : "Trying to compare unequal length lists.";
        
        // If any of the DVDs in the left or right are null, the
        // entire ANDed expression is null.
        if (!orderedNulls && (this.isNull() || other.isNull()))
            return unknownRV;
        
        int result;
        
        for (int i = 0; i < numElements; i++) {
            result = dvd[i].compare(((ListDataType) other).getDVD(i), nullsOrderedLow);
            // If the "i"th predicate is false, the entire ANDed
            // expression is false.
            if (!predSatisfied(result, op))
                return false;
        }
        return true;
    }
	/*
	 * String display of values
	 */

	public String toString()
	{
	    try {
            return getString();
        }
        catch (Exception e) {
	        return "";
        }
	}
    
    /*
     * Hash code
     */
    @Override
    public int hashCode() {
        final int prime = 37;
        int result = 17;
    
        for (int i = 0; i < numElements; i++) {
            if (dvd[i] != null)
                result = result * prime + dvd[i].hashCode();
        }
        return result;
    }
    
    @Override
	public Format getFormat() {
		return Format.LIST;
		
	}
    
    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog(ListDataType.class);
    
    public int estimateMemoryUsage() {
        int memEstimate = BASE_MEMORY_USAGE;
        for (int i = 0; i < numElements; i++) {
            if (dvd[i] != null)
                memEstimate += dvd[i].estimateMemoryUsage();
        }
        return memEstimate;
    }
    

    // ListDataType cannot be in a ResultSet, so do nothing (for now).
    public void setValueFromResultSet(ResultSet resultSet, int colNumber,
                                      boolean isNullable) {

    }
    

    public byte[] serialize() throws StandardException{
        byte[] theBytes;
        try {
            try (ByteArrayOutputStream bout = new ByteArrayOutputStream(512)) {
                ObjectOutputStream out = new ObjectOutputStream(bout);
                out.writeBoolean(isNull);
                out.write(numElements);
                for (int i = 0; i < numElements; i++) {
                    out.writeBoolean(dvd[i] != null);
                    if (dvd[i] != null)
                        out.writeObject(dvd[i]);
                }
                out.close();
                theBytes = bout.toByteArray();
            }
        }
        catch (IOException e) {
            throw StandardException.newException(SQLState.LANG_UNKNOWN, e);
        }
        return theBytes;
    }
    
    public void deserialize(byte[] data) throws StandardException {
        try {
            try (ByteArrayInputStream bin = new ByteArrayInputStream(data)) {
                ObjectInputStream in = new ObjectInputStream(bin);
                isNull = in.readBoolean();
                setLength(in.read());
                for (int i = 0; i < numElements; i++) {
                    if (in.readBoolean())
                        dvd[i] = (DataValueDescriptor) in.readObject();
                    else
                        dvd[i] = null;
                }
                in.close();
            }
        }
        catch (Exception e) {
            throw StandardException.newException(SQLState.LANG_UNKNOWN, e);
        }
    }
    
    @Override
    public Object getSparkObject() throws StandardException {
        return serialize();
    }
    
    @Override
    public void setSparkObject(Object sparkObject) throws StandardException {
        deserialize((byte []) sparkObject);
    }
    
    @Override
    public void read(Row row, int ordinal) throws StandardException {
        if (row.isNullAt(ordinal))
            setToNull();
        else {
            isNull = false;
            Object object = row.get(ordinal);
            deserialize((byte[]) object);
        }
    }
    
    @Override
    public StructField getStructField(String columnName) {
        return DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
    }
    
    // ListDataType is not a valid column value for a row in a table,
    // so do nothing for theta sketch.
    public void updateThetaSketch(UpdateSketch updateSketch) {
    
    }

}
