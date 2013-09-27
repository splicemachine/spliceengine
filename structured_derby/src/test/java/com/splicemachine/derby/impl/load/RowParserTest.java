package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.utils.ErrorState;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Date;

/**
 * @author Scott Fines
 * Created on: 9/26/13
 */
@SuppressWarnings("deprecation")
public class RowParserTest {

    @Test(expected = StandardException.class)
    public void testParseNullCharFails() throws Exception {
        testNullIntoNonNull(new SQLChar(), Types.CHAR);
    }

    @Test
    public void testParseEmptyCharAsNull() throws Exception {
        testParsesEmptyAsNull(new SQLChar(), Types.CHAR, false);
    }

    @Test(expected = StandardException.class)
    public void testParseIntoNonNullColumnFails() throws Exception {
        testNullIntoNonNull(new SQLVarchar(), Types.VARCHAR);
    }

    @Test
    public void testParseNullStringIntoNull() throws Exception {
        testParsesEmptyAsNull(new SQLVarchar(), Types.VARCHAR, false);
    }

    @Test
    public void testParseNullStringToNullTinyint() throws Exception {
        testParsesEmptyAsNull(new SQLTinyint(), Types.TINYINT, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullTinyintIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLTinyint(), Types.TINYINT);
    }

    @Test
    public void testParseNullStringToNullSmallint() throws Exception {
        testParsesEmptyAsNull(new SQLSmallint(), Types.SMALLINT, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullSmallintIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLSmallint(), Types.SMALLINT);
    }

    @Test
    public void testParseNullStringToNullInt() throws Exception {
        testParsesEmptyAsNull(new SQLInteger(), Types.INTEGER, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullIntIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLInteger(), Types.INTEGER);
    }

    @Test
    public void testParseNullStringToNullBigInt() throws Exception {
        testParsesEmptyAsNull(new SQLLongint(), Types.BIGINT, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullBigIntIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLLongint(), Types.BIGINT);
    }

    @Test
    public void testParseNullStringToNullReal() throws Exception {
        testParsesEmptyAsNull(new SQLReal(), Types.REAL, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullRealIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLReal(), Types.REAL);
    }

    @Test
    public void testParseNullStringToNullDouble() throws Exception {
        testParsesEmptyAsNull(new SQLDouble(), Types.DOUBLE, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullDoubleIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLDouble(), Types.DOUBLE);
    }

    @Test
    public void testParseNullStringToNullDecimal() throws Exception {
        testParsesEmptyAsNull(new SQLDecimal(), Types.DECIMAL, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullDecimalIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLDecimal(), Types.DECIMAL);
    }

    @Test
    public void testParseNullStringToNullBoolean() throws Exception {
        testParsesEmptyAsNull(new SQLBoolean(), Types.BOOLEAN, true);
    }

    @Test(expected = StandardException.class)
    public void testParseNullBooleanIntoNonNullField() throws Exception {
        testNullIntoNonNull(new SQLBoolean(), Types.BOOLEAN);
    }

    @Test
    public void testCanParseStringIntoRow() throws Exception {
        String testValue = "test";
        ExecRow parsed = parseNonNullField(new SQLVarchar(),Types.VARCHAR,testValue);
        String parsedValue = parsed.getColumn(1).getString();
        Assert.assertEquals("Incorrectly parsed value", testValue, parsedValue);
    }

    @Test(expected = StandardException.class)
    public void testLengthTooShortVarchar() throws Exception {
        String testValue = "abcdefghijklmnopqrstuvwxyz";
        try{
            parseNonNullField(new SQLVarchar(),Types.VARCHAR,testValue,10);
        }catch(StandardException se){
            Assert.assertEquals(ErrorState.LANG_STRING_TRUNCATION.getSqlState(),se.getSqlState());
            throw se;
        }
    }

    @Test
    public void testCanParseChar() throws Exception {
        String testValue = "test";
        ExecRow parsed = parseNonNullField(new SQLChar(),Types.CHAR,testValue);
        String parsedValue = parsed.getColumn(1).getString();
        Assert.assertEquals("Incorrectly parsed value", testValue, parsedValue);
    }

    @Test(expected = StandardException.class)
    public void testLengthTooShortChar() throws Exception {
        String testValue = "abcdefghijklmnopqrstuvwxyz";
        try{
            parseNonNullField(new SQLChar(),Types.CHAR,testValue,10);
        }catch(StandardException se){
            Assert.assertEquals(ErrorState.LANG_STRING_TRUNCATION.getSqlState(),se.getSqlState());
            throw se;
        }
    }

    @Test
    public void testCanParseStringIntoTinyint() throws Exception {
        String testValue = "1";
        ExecRow parsed = parseNonNullField(new SQLTinyint(),Types.TINYINT,testValue);
        int parsedValue = parsed.getColumn(1).getByte();
        Assert.assertEquals("Incorrectly parsed value",Byte.parseByte(testValue),parsedValue);
    }

    @Test
    public void testCanParseStringIntoSmallint() throws Exception {
        String testValue = "10";
        ExecRow parsed = parseNonNullField(new SQLSmallint(),Types.SMALLINT,testValue);
        int parsedValue = parsed.getColumn(1).getShort();
        Assert.assertEquals("Incorrectly parsed value",Short.parseShort(testValue),parsedValue);
    }

    @Test
    public void testCanParseStringIntoInteger() throws Exception {
        String testValue = "100";
        ExecRow parsed = parseNonNullField(new SQLInteger(),Types.INTEGER,testValue);
        int parsedValue = parsed.getColumn(1).getInt();
        Assert.assertEquals("Incorrectly parsed value",Integer.parseInt(testValue),parsedValue);
    }

    @Test
    public void testCanParseStringIntoLongint() throws Exception {
        String testValue = Long.toString(Long.MAX_VALUE/2);
        ExecRow parsed = parseNonNullField(new SQLLongint(),Types.BIGINT,testValue);
        long parsedValue = parsed.getColumn(1).getLong();
        Assert.assertEquals("Incorrectly parsed value",Long.parseLong(testValue),parsedValue);
    }

    @Test
    public void testCanParseStringIntoReal() throws Exception {
        String testValue = "1.2";
        ExecRow parsed = parseNonNullField(new SQLReal(),Types.REAL,testValue);
        float parsedValue = parsed.getColumn(1).getFloat();
        Assert.assertEquals("Incorrectly parsed value",Float.parseFloat(testValue),parsedValue,Math.pow(1,-6));
    }

    @Test
    public void testCanParseStringIntoDouble() throws Exception {
        String testValue = "1.2";
        ExecRow parsed = parseNonNullField(new SQLDouble(),Types.DOUBLE,testValue);
        double parsedValue = parsed.getColumn(1).getDouble();
        Assert.assertEquals("Incorrectly parsed value",Double.parseDouble(testValue),parsedValue,Math.pow(1,-10));
    }

    @Test
    public void testCanParseStringIntoDecimal() throws Exception {
        String testValue = "1.2";
        ExecRow parsed = parseNonNullField(new SQLDecimal(),Types.DECIMAL,testValue);
        BigDecimal parsedValue = (BigDecimal)parsed.getColumn(1).getObject();
        Assert.assertEquals("Incorrectly parsed value", new BigDecimal(testValue), parsedValue);
    }

    @Test
    public void testCanParseStringIntoBoolean() throws Exception {
        String testValue = "true";
        ExecRow parsed = parseNonNullField(new SQLBoolean(),Types.BOOLEAN,testValue);
        boolean parsedValue = parsed.getColumn(1).getBoolean();
        Assert.assertEquals("Incorrectly parsed value",Boolean.parseBoolean(testValue),parsedValue);
    }

    @Test
    public void testCanParseStringIntoDate() throws Exception {
        String testValue = "2010-01-01";
        ExecRow parsed = parseNonNullField(new SQLDate(),Types.DATE,testValue);
        Date parsedValue = parsed.getColumn(1).getDate(null);
        Assert.assertEquals("Incorrectly parsed value", new java.sql.Date(2010-1900,0,1),parsedValue);
    }

    @Test(expected = StandardException.class)
    public void testImproperDateFormatExplodes() throws Exception {
        String testValue = "201001-01";
        try{
            parseNonNullField(new SQLDate(),Types.DATE,testValue);
        }catch(StandardException se){
            Assert.assertEquals("22007",se.getSqlState());
            throw se;
        }
    }

    @Test
    public void testCanParseStringIntoTime() throws Exception {
        String testValue = "00:00:00";
        ExecRow parsed = parseNonNullField(new SQLTime(),Types.TIME,testValue);
        Time parsedValue = parsed.getColumn(1).getTime(null);
        Assert.assertEquals("Incorrectly parsed value", new java.sql.Time(0,0,0),parsedValue);
    }

    @Test(expected = StandardException.class)
    public void testImproperTimeFormatExplodes() throws Exception {
        String testValue = "0000:00";
        try{
             parseNonNullField(new SQLTime(),Types.TIME,testValue);
        }catch(StandardException se){
            Assert.assertEquals("22007",se.getSqlState());
            throw se;
        }
    }

    @Test
    public void testCanParseStringIntoTimestamp() throws Exception {
        String testValue = "2010-01-01 00:00:00.000";
        ExecRow parsed = parseNonNullField(new SQLTimestamp(),Types.TIME,testValue);
        Timestamp parsedValue = parsed.getColumn(1).getTimestamp(null);
        Assert.assertEquals("Incorrectly parsed value", new java.sql.Timestamp(2010-1900,0,1,0,0,0,0),parsedValue);
    }

    @Test(expected = StandardException.class)
    public void testImproperTimestampFormatExplodes() throws Exception {
        String testValue = "2010-01-01 0000:00.0";
        try{
            parseNonNullField(new SQLTimestamp(),Types.TIMESTAMP,testValue);
        }catch(StandardException se){
            Assert.assertEquals("22007",se.getSqlState());
            throw se;
        }
    }


    private void testParsesEmptyAsNull(DataValueDescriptor dvd,
                                       int jdbcType,
                                       boolean testEmptySpaces) throws StandardException {
        ExecRow row = new ValueRow(1);
        row.setColumn(1,dvd);

        ColumnContext ctx = new ColumnContext.Builder()
                .columnName("t")
                .columnNumber(0)
                .columnType(jdbcType)
                .nullable(true).build();

        RowParser rowParser = new RowParser(row,null,null,null);

        String testValue = null;
        ExecRow process = rowParser.process(new String[]{testValue}, new ColumnContext[]{ctx});

        Assert.assertTrue("Incorrectly parsed value",process.getColumn(1).isNull());

        testValue = "";
        process = rowParser.process(new String[]{testValue}, new ColumnContext[]{ctx});

        Assert.assertTrue("Incorrectly parsed value",process.getColumn(1).isNull());

        if(testEmptySpaces){
            testValue = " ";
            process = rowParser.process(new String[]{testValue}, new ColumnContext[]{ctx});

            Assert.assertTrue("Incorrectly parsed value",process.getColumn(1).isNull());
        }
    }

    private void testNullIntoNonNull(DataValueDescriptor dvd,int jdbcType) throws StandardException {
        ExecRow row = new ValueRow(1);
        row.setColumn(1,dvd);

        ColumnContext ctx = new ColumnContext.Builder()
                .columnName("t")
                .columnNumber(0)
                .columnType(jdbcType)
                .nullable(false).build();
        RowParser rowParser = new RowParser(row,null,null,null);

        String testValue = null;
        try{
            rowParser.process(new String[]{testValue}, new ColumnContext[]{ctx});
        }catch(StandardException se){
            Assert.assertEquals("Incorrect sql state returned",ErrorState.LANG_NULL_INTO_NON_NULL.getSqlState(),se.getSqlState());
            throw se;
        }
    }

    private ExecRow parseNonNullField(DataValueDescriptor dvd,
                                      int jdbcType,
                                      String testValue) throws StandardException {
        return parseNonNullField(dvd,jdbcType,testValue,testValue.length()+10);
    }

    private ExecRow parseNonNullField(DataValueDescriptor dvd,
                                      int jdbcType,
                                      String testValue,
                                      int length) throws StandardException{
        ExecRow row = new ValueRow(1);
        row.setColumn(1,dvd);

        ColumnContext ctx = new ColumnContext.Builder()
                .columnName("t")
                .length(length)
                .columnNumber(0)
                .columnType(jdbcType)
                .nullable(true).build();
        RowParser rowParser = new RowParser(row,null,null,null);

        return rowParser.process(new String[]{testValue}, new ColumnContext[]{ctx});

    }

}
