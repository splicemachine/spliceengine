package com.splicemachine.derby.iapi.types;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.i18n.LocaleFinder;
import org.apache.derby.iapi.types.*;

import java.sql.*;
import java.text.RuleBasedCollator;
import java.util.Locale;

/**
 * @author Scott Fines
 *         Created on: 4/19/13
 */
public class LazyDataValueFactory implements DataValueFactory {

    //TODO -sf- implement Locales!
//    LocaleFinder localeFinder;
//
//    private Locale databaseLocale;
//    private RuleBasedCollator collatorForCharacterTypes;


    @Override
    public NumberDataValue getDataValue(Integer value, NumberDataValue previous) throws StandardException {
        if(previous==null)
            return new LazyInteger(value);
    }

    @Override
    public NumberDataValue getDataValue(char value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(Short value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(Byte value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(Long value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(Float value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(Double value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue getDataValue(Boolean value, BooleanDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getLongVarbitDataValue(byte[] value, BitDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getBlobDataValue(byte[] value, BitDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getBlobDataValue(Blob value, BitDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getVarcharDataValue(String value) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getVarcharDataValue(String value, StringDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getVarcharDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getLongvarcharDataValue(String value) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getLongvarcharDataValue(String value, StringDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getLongvarcharDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getClobDataValue(String value, StringDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getClobDataValue(Clob value, StringDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getClobDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getClobDataValue(Clob value, StringDataValue previous, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public UserDataValue getDataValue(Object value, UserDataValue previous) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RefDataValue getDataValue(RowLocation value, RefDataValue previous) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(int value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(long value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(float value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(double value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(short value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDataValue(byte value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDecimalDataValue(Number value) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDecimalDataValue(Number value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDecimalDataValue(Long value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDecimalDataValue(String value) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getDecimalDataValue(String value, NumberDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue getDataValue(boolean value, BooleanDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getBitDataValue(byte[] value) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getBitDataValue(byte[] value, BitDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getVarbitDataValue(byte[] value, BitDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getCharDataValue(String value) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getCharDataValue(String value, StringDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getCharDataValue(String value, StringDataValue previous, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getDataValue(Date value, DateTimeDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getDataValue(Time value, DateTimeDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getDataValue(Timestamp value, DateTimeDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getTimestamp(DataValueDescriptor operand) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getTimestamp(DataValueDescriptor date, DataValueDescriptor time) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getDate(DataValueDescriptor operand) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getDateValue(String dateStr, boolean isJdbcEscape) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getTimeValue(String timeStr, boolean isJdbcEscape) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getTimestampValue(String timestampStr, boolean isJdbcEscape) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public XMLDataValue getXMLDataValue(XMLDataValue previous) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullInteger(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullShort(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullByte(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullLong(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullFloat(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullDouble(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public NumberDataValue getNullDecimal(NumberDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BooleanDataValue getNullBoolean(BooleanDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getNullBit(BitDataValue dataValue) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getNullVarbit(BitDataValue dataValue) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getNullLongVarbit(BitDataValue dataValue) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public BitDataValue getNullBlob(BitDataValue dataValue) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullChar(StringDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullChar(StringDataValue dataValue, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullVarchar(StringDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullVarchar(StringDataValue dataValue, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullLongvarchar(StringDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullLongvarchar(StringDataValue dataValue, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullClob(StringDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public StringDataValue getNullClob(StringDataValue dataValue, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public UserDataValue getNullObject(UserDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RefDataValue getNullRef(RefDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getNullDate(DateTimeDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getNullTime(DateTimeDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DateTimeDataValue getNullTimestamp(DateTimeDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public XMLDataValue getNullXML(XMLDataValue dataValue) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public RuleBasedCollator getCharacterCollator(int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public DataValueDescriptor getNull(int formatId, int collationType) throws StandardException {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }
}
