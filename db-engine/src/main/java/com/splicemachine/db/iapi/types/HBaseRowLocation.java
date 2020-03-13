/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.db.iapi.types;

import com.splicemachine.access.util.ByteComparisons;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.cache.ClassSize;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import com.splicemachine.utils.ByteSlice;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Objects;
/**
 *
 */
public class HBaseRowLocation extends DataType implements RowLocation {

    private static final int BASE_MEMORY_USAGE = ClassSize.estimateBaseFromCatalog(HBaseRowLocation.class);

    private ByteSlice slice;

    /**
     * Factory method for creating a new HBaseRowLocation from an existing where the new shares NO references with the old.
     */
    public static HBaseRowLocation deepClone(HBaseRowLocation srcLocation) {
        ByteSlice sourceSlice = srcLocation.getSlice();
        ByteSlice newSlice = sourceSlice == null ? new ByteSlice() : ByteSlice.wrap(sourceSlice.getByteCopy());
        return new HBaseRowLocation(newSlice);
    }

    public HBaseRowLocation() {
    }

    public HBaseRowLocation(byte[] rowKey) {
        this.slice = ByteSlice.wrap(rowKey);
        isNull = evaluateNull();
    }

    public HBaseRowLocation(ByteSlice slice) {
        this.slice = slice;
        isNull = evaluateNull();
    }

    /**
     * For cloning
     *
     * CAUTION: returned object will share mutable ByteSlice and mutable byte[] array with this.
     */
    public HBaseRowLocation(HBaseRowLocation other) {

        this.slice = other.slice;
        isNull = evaluateNull();
    }

    @Override
    public int estimateMemoryUsage() {
        return BASE_MEMORY_USAGE;
    }

    @Override
    public final void setValue(byte[] theValue) {
        if (slice == null) {
            slice = ByteSlice.wrap(theValue);
        }
        else {
            slice.set(theValue);
        }
        isNull = evaluateNull();
    }

    @Override
    public final byte[] getBytes() throws StandardException {
        return slice != null ? slice.getByteCopy() : null;
    }

    @Override
    public String getTypeName() {
        return "HBaseRowLocation";
    }

    @Override
    public void setValueFromResultSet(java.sql.ResultSet resultSet, int colNumber, boolean isNullable) {
    }

    public DataValueDescriptor getNewNull() {
        return new HBaseRowLocation();
    }

    @Override
    public Object getObject() {
        return this.slice;
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        this.slice = (ByteSlice) theValue;
        isNull = evaluateNull();
    }

    /**
     * CAUTION: returned object will share mutable ByteSlice and mutable byte[] array with this.
     */
    @Override
    public HBaseRowLocation cloneValue(boolean forceMaterialization) {
        return deepClone(this);
    }

    @Override
    public String getString() {
        return toString();
    }

    @Override
    public int getLength() {
        return this.slice == null ? 0 : this.slice.length();//what is the length of the primary key?
    }

    @Override
    public boolean compare(int op,
                           DataValueDescriptor other,
                           boolean orderedNulls,
                           boolean unknownRV) throws StandardException {
        // HeapRowLocation should not be null, ignore orderedNulls
        int result = compare(other);

        switch (op) {
            case ORDER_OP_LESSTHAN:
                return (result < 0); // this < other
            case ORDER_OP_EQUALS:
                return (result == 0);  // this == other
            case ORDER_OP_LESSOREQUALS:
                return (result <= 0);  // this <= other
            default:

                if (SanityManager.DEBUG)
                    SanityManager.THROWASSERT("Unexpected operation");
                return false;
        }
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException {
        return ByteComparisons.comparator().compare(getBytes(),other.getBytes());
    }

    /**
     * Return my format identifier.
     */
    @Override
    public int getTypeFormatId() {
        return StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID;
    }

	private final boolean evaluateNull()
	{
        return slice == null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(slice!=null);
        if (slice!=null)
            out.writeObject(slice);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        if (in.readBoolean())
            slice = (ByteSlice) in.readObject();
    }

    @Override
    public void readExternalFromArray(ArrayInputStream in) throws IOException, ClassNotFoundException {
    }

    @Override
    public void restoreToNull() {
    }

    @Override
    protected void setFrom(DataValueDescriptor theValue) {
        if (SanityManager.DEBUG)
            SanityManager.ASSERT(theValue instanceof HBaseRowLocation,
                    "Should only be set from another HeapRowLocation");
        HBaseRowLocation that = (HBaseRowLocation) theValue;
        ByteSlice otherSlice = that.slice;
        this.slice.set(otherSlice.array(), otherSlice.offset(), otherSlice.length());
    }

    @Override
    public boolean equals(Object ref) {
        return (this == ref) || (ref instanceof HBaseRowLocation) &&
                Objects.equals(((HBaseRowLocation) ref).slice, this.slice);
    }

    @Override
    public int hashCode() {
        return this.slice == null ? 0 : this.slice.hashCode();
    }

    @Override
    public String toString() {
        return slice != null ? slice.toHexString() : "null";
    }

    @Override
    public DataValueFactoryImpl.Format getFormat() {
        return DataValueFactoryImpl.Format.ROW_LOCATION;
    }

    public ByteSlice getSlice() {
        return slice;
    }

    @Override
    public void read(Row row, int ordinal) throws StandardException {
        if (row.isNullAt(ordinal))
            setToNull();
        else
            slice = ByteSlice.wrap((byte[]) row.get(ordinal));
    }

    @Override
    public StructField getStructField(String columnName) {
        return DataTypes.createStructField(columnName, DataTypes.BinaryType, true);
    }


    public void updateThetaSketch(UpdateSketch updateSketch) {
        updateSketch.update(slice.getByteCopy());
    }

    @Override
    public void setSparkObject(Object sparkObject) throws StandardException {
        if (sparkObject == null)
            setToNull();
        else {
            slice = ByteSlice.wrap((byte[]) sparkObject); // Autobox, must be something better.
            setIsNull(false);
        }
    }

    @Override
    public Object getSparkObject() throws StandardException {
        if (isNull() || slice == null)
            return null;
        return slice.getByteCopy();
    }

}
