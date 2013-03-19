package com.splicemachine.derby.iapi.types;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.RuleBasedCollator;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.monitor.ModuleControl;
import org.apache.derby.iapi.types.BitDataValue;
import org.apache.derby.iapi.types.BooleanDataValue;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.DataValueFactory;
import org.apache.derby.iapi.types.DateTimeDataValue;
import org.apache.derby.iapi.types.NumberDataValue;
import org.apache.derby.iapi.types.RefDataValue;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.StringDataValue;
import org.apache.derby.iapi.types.UserDataValue;
import org.apache.derby.iapi.types.XMLDataValue;

public class SpliceDataValueFactoryImpl implements DataValueFactory, ModuleControl {

	@Override
	public void boot(boolean arg0, Properties arg1) throws StandardException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public BitDataValue getBitDataValue(byte[] value) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getBitDataValue(byte[] value, BitDataValue previous) throws StandardException {
		return null;
	}

	@Override
	public BitDataValue getBlobDataValue(byte[] value, BitDataValue previous) throws StandardException {
		return null;
	}

	@Override
	public BitDataValue getBlobDataValue(Blob value, BitDataValue previous) throws StandardException {
		return null;
	}

	@Override
	public StringDataValue getCharDataValue(String value) {
		return null;
	}

	@Override
	public StringDataValue getCharDataValue(String arg0, StringDataValue arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getCharDataValue(String arg0, StringDataValue arg1,
			int arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RuleBasedCollator getCharacterCollator(int arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getClobDataValue(String arg0, StringDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getClobDataValue(Clob arg0, StringDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getClobDataValue(String arg0, StringDataValue arg1,
			int arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getClobDataValue(Clob arg0, StringDataValue arg1,
			int arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Integer arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(char arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Short arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Byte arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Long arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Float arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(Double arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue getDataValue(Boolean arg0, BooleanDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserDataValue getDataValue(Object arg0, UserDataValue arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RefDataValue getDataValue(RowLocation arg0, RefDataValue arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(int arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(long arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(float arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(double arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(short arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDataValue(byte arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue getDataValue(boolean arg0, BooleanDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getDataValue(Date arg0, DateTimeDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getDataValue(Time arg0, DateTimeDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getDataValue(Timestamp arg0, DateTimeDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getDate(DataValueDescriptor arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getDateValue(String arg0, boolean arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDecimalDataValue(Number arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDecimalDataValue(String arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDecimalDataValue(Number arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDecimalDataValue(Long arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getDecimalDataValue(String arg0, NumberDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getLongVarbitDataValue(byte[] arg0, BitDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getLongvarcharDataValue(String arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getLongvarcharDataValue(String arg0,
			StringDataValue arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getLongvarcharDataValue(String arg0,
			StringDataValue arg1, int arg2) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DataValueDescriptor getNull(int arg0, int arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getNullBit(BitDataValue arg0) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getNullBlob(BitDataValue arg0) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BooleanDataValue getNullBoolean(BooleanDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullByte(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullChar(StringDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullChar(StringDataValue arg0, int arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullClob(StringDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullClob(StringDataValue arg0, int arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getNullDate(DateTimeDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullDecimal(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullDouble(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullFloat(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullInteger(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullLong(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getNullLongVarbit(BitDataValue arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullLongvarchar(StringDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullLongvarchar(StringDataValue arg0, int arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public UserDataValue getNullObject(UserDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public RefDataValue getNullRef(RefDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public NumberDataValue getNullShort(NumberDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getNullTime(DateTimeDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getNullTimestamp(DateTimeDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getNullVarbit(BitDataValue arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullVarchar(StringDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getNullVarchar(StringDataValue arg0, int arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public XMLDataValue getNullXML(XMLDataValue arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getTimeValue(String arg0, boolean arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getTimestamp(DataValueDescriptor arg0)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getTimestamp(DataValueDescriptor arg0,
			DataValueDescriptor arg1) throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DateTimeDataValue getTimestampValue(String arg0, boolean arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BitDataValue getVarbitDataValue(byte[] arg0, BitDataValue arg1)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getVarcharDataValue(String value) {
	// TODO Auto-generated method stub
		return null;
	}

	@Override
	public StringDataValue getVarcharDataValue(String value, StringDataValue previous) throws StandardException {
		return null;
	}

	@Override
	public StringDataValue getVarcharDataValue(String arg0,StringDataValue arg1, int arg2) throws StandardException {
		return null;
	}

	@Override
	public XMLDataValue getXMLDataValue(XMLDataValue value)
			throws StandardException {
		// TODO Auto-generated method stub
		return null;
	}

}
