package com.splicemachine.derby.iapi.types;

import com.gotometrics.orderly.IntegerRowKey;
import com.gotometrics.orderly.RowKey;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.types.*;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.*;
import java.util.Calendar;

/**
 * @author Scott Fines
 *         Created on: 4/19/13
 */
public class LazySQLInteger extends LazyNumber {

    private int value;
    private byte[] serializedValue;
    private RowKey serializer;

    private boolean dirtyBytes = false;

    @Override
    public int getInt() throws StandardException {
        deserializeIfNeeded();
        return value;
    }

    @Override
    public void setValue(short theValue) throws StandardException {
        this.value = theValue;
        dirtyBytes=true;
        dirtyValue=false;
    }

    @Override
    public void setValue(byte theValue) throws StandardException {
        this.value = theValue;
        dirtyBytes = true;
        dirtyValue=false;
    }

    @Override
    public void setValue(Number theValue) throws StandardException {
        this.value = theValue.intValue();
        dirtyBytes = true;
        dirtyValue = false;
    }

    @Override
    public void setBigDecimal(Number bigDecimal) throws StandardException {
        this.value = bigDecimal.intValue();
        dirtyBytes = true;
        dirtyValue = false;
    }

    @Override
    public int typeToBigDecimal() {
        return super.typeToBigDecimal();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int getDecimalValuePrecision() {
        return super.getDecimalValuePrecision();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int getDecimalValueScale() {
        return super.getDecimalValueScale();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean getBoolean() throws StandardException {
        deserializeIfNeeded();
        return value!=0;
    }

    @Override
    public byte getByte() throws StandardException {
        deserializeIfNeeded();
        return (byte)value;
    }

    @Override
    public short getShort() throws StandardException {
        deserializeIfNeeded();
        return (short)value;
    }

    @Override
    public long getLong() throws StandardException {
        deserializeIfNeeded();
        return value;
    }

    @Override
    public float getFloat() throws StandardException {
        deserializeIfNeeded();
        return value;
    }

    @Override
    public double getDouble() throws StandardException {
        deserializeIfNeeded();
        return value;
    }

    @Override
    public byte[] getBytes() throws StandardException {
        serializeIfNeeded();
        return serializedValue;
    }

    @Override
    public Date getDate(Calendar cal) throws StandardException {
        return super.getDate(cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Time getTime(Calendar cal) throws StandardException {
        return super.getTime(cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Timestamp getTimestamp(Calendar cal) throws StandardException {
        return super.getTimestamp(cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public InputStream getStream() throws StandardException {
        return super.getStream();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean hasStream() {
        return super.hasStream();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public String getTraceString() throws StandardException {
        return super.getTraceString();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor recycle() {
        return super.recycle();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Time theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Time theValue, Calendar cal) throws StandardException {
        super.setValue(theValue, cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Timestamp theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Timestamp theValue, Calendar cal) throws StandardException {
        super.setValue(theValue, cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Date theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Date theValue, Calendar cal) throws StandardException {
        super.setValue(theValue, cal);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Object theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(String theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Blob theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(Clob theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(int theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(double theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(float theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(long theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(boolean theValue) throws StandardException {
        super.setValue(theValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    protected void setFrom(DataValueDescriptor dvd) throws StandardException {
        super.setFrom(dvd);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setToNull() {
        super.setToNull();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setObjectForCast(Object theValue, boolean instanceOfResultType, String resultTypeClassName) throws StandardException {
        super.setObjectForCast(theValue, instanceOfResultType, resultTypeClassName);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public Object getObject() throws StandardException {
        return super.getObject();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor cloneHolder() {
        return super.cloneHolder();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void throwLangSetMismatch(Object value) throws StandardException {
        super.throwLangSetMismatch(value);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setInto(PreparedStatement ps, int position) throws SQLException, StandardException {
        super.setInto(ps, position);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setInto(ResultSet rs, int position) throws SQLException, StandardException {
        super.setInto(rs, position);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void normalize(DataTypeDescriptor desiredType, DataValueDescriptor source) throws StandardException {
        super.normalize(desiredType, source);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int typePrecedence() {
        return super.typePrecedence();    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue equals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.equals(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue notEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.notEquals(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue lessThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.lessThan(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue greaterThan(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.greaterThan(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue lessOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.lessOrEquals(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue greaterOrEquals(DataValueDescriptor left, DataValueDescriptor right) throws StandardException {
        return super.greaterOrEquals(left, right);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean compare(int op, DataValueDescriptor other, boolean orderedNulls, boolean nullsOrderedLow, boolean unknownRV) throws StandardException {
        return super.compare(op, other, orderedNulls, nullsOrderedLow, unknownRV);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int compare(DataValueDescriptor other, boolean nullsOrderedLow) throws StandardException {
        return super.compare(other, nullsOrderedLow);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public int compareTo(Object otherDVD) {
        return super.compareTo(otherDVD);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor coalesce(DataValueDescriptor[] argumentsList, DataValueDescriptor returnValue) throws StandardException {
        return super.coalesce(argumentsList, returnValue);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue in(DataValueDescriptor left, DataValueDescriptor[] inList, boolean orderedList) throws StandardException {
        return super.in(left, inList, orderedList);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public boolean equals(Object other) {
        return super.equals(other);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void setValue(InputStream theStream, int valueLength) throws StandardException {
        super.setValue(theStream, valueLength);    //To change body of overridden methods use File | Settings | File Templates.
    }

    @Override
    public void checkHostVariable(int declaredLength) throws StandardException {
        super.checkHostVariable(declaredLength);    //To change body of overridden methods use File | Settings | File Templates.
    }

    private void deserializeIfNeeded() throws StandardException {
        if(dirtyInt){
            if(serializer==null)serializer = new IntegerRowKey();
            try {
                value = (Integer)serializer.deserialize(serializedValue);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    @Override
    protected int typeCompare(DataValueDescriptor arg) throws StandardException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    protected boolean isNegative() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue times(NumberDataValue left, NumberDataValue right, NumberDataValue result) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue minus(NumberDataValue result) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getLength() throws StandardException {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getString() throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor cloneValue(boolean forceMaterialization) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor getNewNull() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void setValueFromResultSet(ResultSet resultSet, int colNumber, boolean isNullable) throws StandardException, SQLException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public String getTypeName() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readExternalFromArray(ArrayInputStream ais) throws IOException, ClassNotFoundException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int estimateMemoryUsage() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isNull() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void restoreToNull() {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int getTypeFormatId() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
