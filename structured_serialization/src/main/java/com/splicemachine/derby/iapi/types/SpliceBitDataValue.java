package com.splicemachine.derby.iapi.types;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.ArrayInputStream;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.ConcatableDataValue;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.NumberDataValue;

public class SpliceBitDataValue implements BitDataValue {

	@Override
	public NumberDataValue charLength(NumberDataValue arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConcatableDataValue substring(NumberDataValue arg0,
			NumberDataValue arg1, ConcatableDataValue arg2, int arg3)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void checkHostVariable(int arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataValueDescriptor cloneHolder() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataValueDescriptor cloneValue(boolean arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataValueDescriptor coalesce(DataValueDescriptor[] arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int compare(DataValueDescriptor arg0) throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int compare(DataValueDescriptor arg0, boolean arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean compare(int arg0, DataValueDescriptor arg1, boolean arg2,
			boolean arg3) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean compare(int arg0, DataValueDescriptor arg1, boolean arg2,
			boolean arg3, boolean arg4) throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BooleanDataValue equals(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int estimateMemoryUsage() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean getBoolean() throws StandardException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public byte getByte() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public byte[] getBytes() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(Calendar arg0) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public double getDouble() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getInt() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int getLength() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public DataValueDescriptor getNewNull() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object getObject() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public InputStream getStream() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Time getTime(Calendar arg0) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Timestamp getTimestamp(Calendar arg0) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTraceString() throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getTypeName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue greaterOrEquals(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue greaterThan(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean hasStream() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public BooleanDataValue in(DataValueDescriptor arg0,
			DataValueDescriptor[] arg1, boolean arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue isNotNull() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue isNullOp() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue lessOrEquals(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue lessThan(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void normalize(DataTypeDescriptor arg0, DataValueDescriptor arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BooleanDataValue notEquals(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void readExternalFromArray(ArrayInputStream arg0)
			throws IOException, ClassNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public DataValueDescriptor recycle() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setBigDecimal(Number arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInto(PreparedStatement arg0, int arg1) throws SQLException,
			StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setInto(ResultSet arg0, int arg1) throws SQLException,
			StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setObjectForCast(Object arg0, boolean arg1, String arg2)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setToNull() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(int arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(double arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(float arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(short arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(long arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(byte arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(boolean arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Object arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(byte[] arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(String arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Clob arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Time arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Timestamp arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Date arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(DataValueDescriptor arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Time arg0, Calendar arg1) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Timestamp arg0, Calendar arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(Date arg0, Calendar arg1) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValue(InputStream arg0, int arg1) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setValueFromResultSet(ResultSet arg0, int arg1, boolean arg2)
			throws StandardException, SQLException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int typePrecedence() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public int typeToBigDecimal() throws StandardException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean isNull() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void restoreToNull() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException,
			ClassNotFoundException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int getTypeFormatId() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setWidth(int arg0, int arg1, boolean arg2)
			throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void loadStream() throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public InputStream returnStream() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setStream(InputStream arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BitDataValue concatenate(BitDataValue arg0, BitDataValue arg1,
			BitDataValue arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(Blob arg0) throws StandardException {
		// TODO Auto-generated method stub
		
	}

}
