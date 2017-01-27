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
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayInputStream;
import com.splicemachine.db.iapi.types.BooleanDataValue;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.DataValueFactoryImpl;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.hadoop.hbase.util.Order;
import org.apache.hadoop.hbase.util.PositionedByteRange;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.sql.types.StructField;
import org.joda.time.DateTime;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Calendar;

/**
 * Created by jleach on 10/18/16.
 */
public class StatsBoundaryDataValueDescriptor implements DataValueDescriptor {
    DataValueDescriptor dvd;
    public StatsBoundaryDataValueDescriptor(DataValueDescriptor dvd) {
        this.dvd = dvd;
    }

    @Override
    public int getLength() throws StandardException {
        return dvd.getLength();
    }

    @Override
    public String getString() throws StandardException {
        return dvd.getString();
    }

    @Override
    public String getTraceString() throws StandardException {
        return dvd.getTraceString();
    }

    @Override
    public boolean getBoolean() throws StandardException {
        return dvd.getBoolean();
    }

    @Override
    public byte getByte() throws StandardException {
        return dvd.getByte();
    }

    @Override
    public short getShort() throws StandardException {
        return dvd.getShort();
    }

    @Override
    public int getInt() throws StandardException {
        return dvd.getInt();
    }

    @Override
    public long getLong() throws StandardException {
        return dvd.getLong();
    }

    @Override
    public float getFloat() throws StandardException {
        return dvd.getFloat();
    }

    @Override
    public double getDouble() throws StandardException {
        return dvd.getDouble();
    }

    @Override
    public int typeToBigDecimal() throws StandardException {
        return dvd.typeToBigDecimal();
    }

    @Override
    public byte[] getBytes() throws StandardException {
        return dvd.getBytes();
    }

    @Override
    public Date getDate(Calendar cal) throws StandardException {
        return dvd.getDate(cal);
    }

    @Override
    public Time getTime(Calendar cal) throws StandardException {
        return dvd.getTime(cal);
    }

    @Override
    public Timestamp getTimestamp(Calendar cal) throws StandardException {
        return dvd.getTimestamp(cal);
    }

    @Override
    public DateTime getDateTime() throws StandardException {
        return dvd.getDateTime();
    }

    @Override
    public Object getObject() throws StandardException {
        return dvd.getObject();
    }

    @Override
    public InputStream getStream() throws StandardException {
        return dvd.getStream();
    }

    @Override
    public boolean hasStream() {
        return dvd.hasStream();
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        return dvd.cloneHolder();
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        return dvd.cloneValue(forceMaterialization);
    }

    @Override
    public DataValueDescriptor recycle() {
        return dvd.recycle();
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return dvd.getNewNull();
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
        dvd.setValueFromResultSet(resultSet,colNumber,isNullable);
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {
        dvd.setInto(ps,position);
    }

    @Override
    public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
        dvd.setInto(rs,position);
    }

    @Override
    public void setValue(int theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(double theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(float theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(short theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(long theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(byte theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(boolean theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(RowId theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(byte[] theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException {
        dvd.setBigDecimal(bigDecimal);
    }

    @Override
    public void setValue(String theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Blob theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Clob theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Time theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Time theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue,cal);
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Timestamp theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue,cal);
    }

    @Override
    public void setValue(DateTime theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Date theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setValue(Date theValue, Calendar cal) throws StandardException {
        dvd.setValue(theValue,cal);
    }

    @Override
    public void setValue(DataValueDescriptor theValue) throws StandardException {
        dvd.setValue(theValue);
    }

    @Override
    public void setToNull() {
        dvd.setToNull();
    }

    @Override
    public void normalize(DataTypeDescriptor dtd, DataValueDescriptor source) throws StandardException {
        dvd.normalize(dtd,source);
    }

    @Override
    public BooleanDataValue isNullOp() {
        return dvd.isNullOp();
    }

    @Override
    public BooleanDataValue isNotNull() {
        return dvd.isNotNull();
    }

    @Override
    public String getTypeName() {
        return dvd.getTypeName();
    }

    @Override
    public void setObjectForCast(Object value, boolean instanceOfResultType, String resultTypeClassName) throws StandardException {
        dvd.setObjectForCast(value, instanceOfResultType, resultTypeClassName);
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException {
        dvd.readExternalFromArray(ais);
    }

    @Override
    public int typePrecedence() {
        return dvd.typePrecedence();
    }

    @Override
    public BooleanDataValue equals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.equals(left,right);
    }

    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.notEquals(left,right);
    }

    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.lessThan(left,right);
    }

    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.greaterThan(left,right);
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.lessOrEquals(left,right);
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return dvd.greaterOrEquals(left,right);
    }

    @Override
    public DataValueDescriptor coalesce(DataValueDescriptor[] list, DataValueDescriptor returnValue) throws StandardException {
        return dvd.coalesce(list,returnValue);
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left, DataValueDescriptor[] inList, boolean orderedList) throws StandardException {
        return dvd.in(left,inList,orderedList);
    }

    @Override
    public int compare(DataValueDescriptor other) throws StandardException {
        return dvd.compare(other);
    }

    @Override
    public int compare(DataValueDescriptor other, boolean nullsOrderedLow) throws StandardException {
        return dvd.compare(other,nullsOrderedLow);
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean unknownRV) throws StandardException {
        return dvd.compare(op,other,orderedNulls,unknownRV);
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean nullsOrderedLow, boolean unknownRV) throws StandardException {
        return dvd.compare(op,other,orderedNulls,nullsOrderedLow,unknownRV);
    }

    @Override
    public void setValue(InputStream theStream, int valueLength) throws StandardException {
        dvd.setValue(theStream,valueLength);
    }

    @Override
    public void checkHostVariable(int declaredLength) throws StandardException {
        dvd.checkHostVariable(declaredLength);
    }

    @Override
    public int estimateMemoryUsage() {
        return dvd.estimateMemoryUsage();
    }

    @Override
    public boolean isLazy() {
        return dvd.isLazy();
    }

    @Override
    public boolean isDoubleType() {
        return dvd.isDoubleType();
    }

    @Override
    public DataValueFactoryImpl.Format getFormat() {
        return dvd.getFormat();
    }

    @Override
    public void write(UnsafeRowWriter unsafeRowWriter, int ordinal) throws StandardException {
        dvd.write(unsafeRowWriter,ordinal);
    }

    @Override
    public void read(UnsafeRow unsafeRow, int ordinal) throws StandardException {
        dvd.read(unsafeRow,ordinal);
    }

    @Override
    public void read(Row row, int ordinal) throws StandardException {
        dvd.read(row,ordinal);
    }

    @Override
    public int encodedKeyLength() throws StandardException {
        return dvd.encodedKeyLength();
    }

    @Override
    public void encodeIntoKey(PositionedByteRange builder, Order order) throws StandardException {
        dvd.encodeIntoKey(builder,order);
    }

    @Override
    public void decodeFromKey(PositionedByteRange builder) throws StandardException {
        dvd.decodeFromKey(builder);
    }

    @Override
    public StructField getStructField(String columnName) {
        return dvd.getStructField(columnName);
    }

    @Override
    public ItemsSketch getQuantilesSketch() throws StandardException {
        return dvd.getQuantilesSketch();
    }

    @Override
    public com.yahoo.sketches.frequencies.ItemsSketch getFrequenciesSketch() throws StandardException {
        return dvd.getFrequenciesSketch();
    }

    @Override
    public UpdateSketch getThetaSketch() throws StandardException {
        return dvd.getThetaSketch();
    }

    @Override
    public void updateThetaSketch(UpdateSketch updateSketch) {
        dvd.updateThetaSketch(updateSketch);
    }

    @Override
    public int getRowWidth() {
        return dvd.getRowWidth();
    }

    @Override
    public int compare(DataValueDescriptor o1, DataValueDescriptor o2) {
        return dvd.compare(o1,o2);
    }

    @Override
    public boolean isNull() {
        return dvd.isNull();
    }

    @Override
    public void restoreToNull() {
        dvd.restoreToNull();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        dvd.writeExternal(out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dvd.readExternal(in);
    }

    @Override
    public int getTypeFormatId() {
        return dvd.getTypeFormatId();
    }

    @Override
    public Object getSparkObject() throws StandardException {
        return dvd.getSparkObject();
    }
}
