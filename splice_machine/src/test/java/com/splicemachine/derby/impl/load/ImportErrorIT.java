/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.nio.file.Files;
import java.sql.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Tests for checking proper error conditions hold for Importing.
 *
 * That is, if an import fails, it should fail with certain characteristics (depending on
 * the failure type). This test exists to check those characteristics
 *
 * @author Scott Fines
 * Created on: 9/20/13
 */
public class ImportErrorIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = ImportErrorIT.class.getSimpleName().toUpperCase();
    /*
     * a Single table used to test all the different errors:
     *
     * null into a not null X
     * String too long for field X
     *
     * Long into int overflow X
     * Float into int truncation error X
     * Double into long truncation error X
     * Double into Float overflow X
     *
     * incorrect datetime format X
     * incorrect time format X
     * incorrect date format X
     *
     * File not found X
     * No such table
     * cannot modify an auto-increment column
     */
    private static final String TABLE = "errorTable";

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceTableWatcher tableWatcher = new SpliceTableWatcher(TABLE,schema.schemaName,"(a int not null, b bigint, c real, d double, e varchar(5),f date not null,g time not null, h timestamp not null)");
    private static final SpliceTableWatcher decimalTable = new SpliceTableWatcher("DECIMALTABLE", schema.schemaName, "(d decimal(2))");
    private static final SpliceTableWatcher incrementTable = new SpliceTableWatcher("INCREMENT", schema.schemaName, "(a int generated always as identity, b int)");
    private static final SpliceTableWatcher timestampTable = new SpliceTableWatcher("TIMESTAMP", schema.schemaName, "(a1 bigint, b1 timestamp)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
            .around(tableWatcher)
            .around(decimalTable)
            .around(incrementTable)
            .around(timestampTable);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static File BADDIR;
    @BeforeClass
    public static void beforeClass() throws Exception {
        BADDIR = SpliceUnitTest.createBadLogDirectory(schema.schemaName);
    }

    @Test
    public void testNoSuchTable() throws Exception {
        runImportTest("NO_SUCH_TABLE","null_col.csv",BADDIR.getCanonicalPath(),new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!", "XIE0M",se.getSQLState());

                String correctErrorMessage = "Table '"+table+"' does not exist.";
                Assert.assertEquals("Incorrect error message!", correctErrorMessage, se.getMessage().trim());
            }
        });
    }

    @Test
    public void testCannotFindFile() throws Exception {
        final String fileName = "file_doesnt_exist.csv";
        runImportTest(fileName,new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","X0X14",se.getSQLState());
                String retval = se.getMessage();
                Assert.assertTrue("Incorrect error message! correct: Expected error to contain <"+
                                      fileName+">, Error: <"+retval+">",retval.contains(fileName));
            }
        });
    }

    @Test
    public void testCannotFindBadRecordsDir() throws Exception {
        final String fileName = "null_col.csv";
        final String badRecordsDir="/totallyMissingBadDir";
        runImportTest(fileName,badRecordsDir,new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                //make sure the error code is correct
                Assert.assertEquals("Incorrect sql state!","XIE04",se.getSQLState());
                String retval = se.getMessage();
                Assert.assertTrue("Incorrect error message! correct: Expected error to contain <"+
                        badRecordsDir+">, Error: <"+retval+">",retval.contains(badRecordsDir));
            }
        });
    }

    @Test
    public void testCanReimportFileAfterTableDrop() throws Exception{
        /*
         * Regression test for DB-1082. This is bizarrely complicated, but it should work:
         *
         * 1. create a new table
         * 2. move file "db1082bad.csv" to file "db1082.csv"
         * 3. import "db1082.csv" --test for expected error
         * 4. drop the table
         * 5. move file "db1082good.csv" to file "db1082.csv"
         * 6. import "db1082.csv" --test should succeed
         * 7. Select * from table --should contain rows
         */
        File badFile = new File(getResourceDirectory()+"db1082bad.csv");
        File destFile = new File(getResourceDirectory()+"test_data/bad_import/db1082.csv");
        if(destFile.exists())
            destFile.delete();
        Files.copy(badFile.toPath(), destFile.toPath());
        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setSchema(schema.schemaName);
        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists DB1082");
            s.execute("create table DB1082(a int, b int)");
        }
        runImportTest(destFile.getName(),new ErrorCheck(){
            @Override
            public void check(String table,String location,SQLException se) throws Exception{
                Assert.assertEquals("SE009",se.getSQLState());
            }
        });

        destFile.delete();
        File goodFile = new File(getResourceDirectory()+"db1082good.csv");
        Files.copy(goodFile.toPath() ,destFile.toPath());
        try(Statement s = conn.createStatement()){
            s.execute("drop table DB1082");
            s.execute("create table DB1082(a int, b int)");
            String inputFilePath = getResourceDirectory()+"test_data/bad_import/"+destFile.getName();
            s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
                            "'%s'," +  // schema name
                            "'%s'," +  // table name
                            "null," +  // insert column list
                            "'%s'," +  // file path
                            "','," +   // column delimiter
                            "null," +  // character delimiter
                            "null," +  // timestamp format
                            "null," +  // date format
                            "null," +  // time format
                            "%d," +    // max bad records
                            "'%s'," +  // bad record dir
                            "null," +  // has one line records
                            "null)",   // char set
                    schema.schemaName, "DB1082", inputFilePath,
                    0, BADDIR.getCanonicalPath()));

            destFile.delete();
            try(ResultSet rs = s.executeQuery("select * from DB1082")){
                Assert.assertTrue("no rows found!",rs.next());
            }
        }
    }

    @Test
    public void testCannotInsertNullFieldIntoNonNullColumn() throws Exception {
        final String importFileName = "null_col.csv";
        final String expectedErrorCode = "23502";
        final String expectedErrorMsg = "Column 'A' cannot accept a NULL value.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertStringOverCharacterLimits() throws Exception {
        final String importFileName = "long_string.csv";
        final String expectedErrorCode = "22001";
        final String expectedErrorMsg = "A truncation error was encountered trying to shrink VARCHAR " +
            "'thisstringhasmorethanfivecharacters' to length 5.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertALongIntoAnIntegerField() throws Exception {
        final String importFileName = "long_int.csv";
        final String expectedErrorCode = "22018";
        final String expectedErrorMsg = "Invalid character string format for type INTEGER.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertAFloatIntoAnIntegerField() throws Exception {
        final String importFileName = "float_int.csv";
        final String expectedErrorCode = "22018";
        final String expectedErrorMsg = "Invalid character string format for type INTEGER.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertADoubleIntoALongField() throws Exception {
        final String importFileName = "double_long.csv";
        final String expectedErrorCode = "22018";
        final String expectedErrorMsg = "Invalid character string format for type BIGINT.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertADoubleIntoAFloatField() throws Exception {
        final String importFileName = "double_float.csv";
        final String expectedErrorCode = "22003";
        final String expectedErrorMsg = "The resulting value is outside the range for the data type REAL.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertAPoorlyFormattedDate() throws Exception {
        final String importFileName = "bad_date.csv";
        final String expectedErrorCode = "22007";
        final String expectedErrorMsg = "22007.S.181 Error parsing datetime 201301-01 with pattern: yyyy-MM-dd. Try using an ISO8601 pattern such as, yyyy-MM-dd'T'HH:mm:ss.SSSZZ, yyyy-MM-dd'T'HH:mm:ssZ or yyyy-MM-dd [Columns in Table: 8] [Columns in File: 8] [Bad Column ID: 6] [Bad Column Value: 201301-01] row Data: [1, 123, 1.4, 1.5555642, Hello, 201301-01, 00:00:00, 2013-01-01 00:00:00]";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertAPoorlyFormattedTime() throws Exception {
        final String importFileName = "bad_time.csv";
        final String expectedErrorCode = "22007";
        final String expectedErrorMsg = "Error parsing datetime 0000:00 with pattern: HH:mm:ss";

        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertNullDateIntoDateField() throws Exception{
        final String importFileName = "null_date.csv";
        final String expectedErrorCode = "23502";
        final String expectedErrorMsg = "Column 'F' cannot accept a NULL value.";

        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertNullTimeIntoTimeField() throws Exception{
        final String importFileName = "null_time.csv";
        final String expectedErrorCode = "23502";
        final String expectedErrorMsg = "Column 'G' cannot accept a NULL value.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertNullTimestampIntoTimestampField() throws Exception{
        final String importFileName = "null_timestamp.csv";
        final String expectedErrorCode = "23502";
        final String expectedErrorMsg = "Column 'H' cannot accept a NULL value.";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    public void testCannotInsertAPoorlyFormatedTimestamp() throws Exception {
        final String importFileName = "bad_timestamp.csv";
        final String expectedErrorCode = "22007";
        final String expectedErrorMsg = "Error parsing datetime 2013-01-0100:00:00 with pattern: yyyy-MM-dd HH:mm:ss";
        helpTestImportError(importFileName, expectedErrorCode, expectedErrorMsg);
    }

    @Test
    @Ignore("SPLICE-230: Import of improper decimal gives overflow when selected")
    public void testDecimalTable() throws Exception {
        final String importFileName = "bad_decimal.csv";
        final String expectedErrorCode = "XIE0A";
        final String expectedErrorMsg = "The resulting value is outside the range for the data type DECIMAL/NUMERIC(2,0).";
        runImportTest("DECIMALTABLE",importFileName, new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                // "too many records" error
                Assert.assertEquals("Incorrect sql state!", "SE009", se.getSQLState());
                //make sure the error code is correct
                SpliceUnitTest.assertBadFileContainsError(BADDIR, importFileName, expectedErrorCode, expectedErrorMsg);
            }
        });
    }


    @Test @Ignore("This error, 42Z23, gets detected before import begins in statement bind.")
    public void testNonNullIntoIncrement() throws Exception {
        final String importFileName = "simple_column.txt";
        final String expectedErrorCode = "42Z23";
        final String expectedErrorMsg = "Attempt to modify an identity column 'A'.";
        runImportTest("INCREMENT",importFileName, new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                // "too many records" error
                Assert.assertEquals("Incorrect sql state!", "SE009", se.getSQLState());
                //make sure the error code is correct
                SpliceUnitTest.assertBadFileContainsError(BADDIR, importFileName, expectedErrorCode, expectedErrorMsg);
            }
        });
    }

    @Test
    public void testImportFromS3Error() throws Exception {
        try {
            String sqlText =
                    String.format("CALL SYSCS_UTIL.IMPORT_DATA(" +
                            "'%s', " +
                            "'%s', " +
                            "null, " +
                            "?, " +
                            "',', " +
                            "null, " +
                            "null, " +
                            "null, " +
                            "null, " +
                            "1, " +
                            "'/BAD', " +
                            "true, " +
                            "null)", schema, TABLE);

            PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
            ps.setString(1, "s3a://splice-qa/temp/impo/test.csv");
            ps.execute();
            fail();
        } catch (SQLException sqle) {
            String sqlState = sqle.getSQLState();
            Assert.assertTrue(sqlState, sqlState.compareToIgnoreCase("EXT34") == 0 ||
                    sqlState.compareToIgnoreCase("X0X14") == 0 ||
                    sqlState.compareToIgnoreCase("XIE04") == 0);
        }

    }

    private void helpTestImportError(final String importFileName, final String expectedErrorCode, final String
        expectedErrorMsg) throws Exception {
        runImportTest(importFileName, new ErrorCheck() {
            @Override
            public void check(String table, String location, SQLException se) throws Exception {
                // "too many records" error
                Assert.assertEquals("Incorrect sql state!", "SE009", se.getSQLState());
                //make sure the error code is correct
                SpliceUnitTest.assertBadFileContainsError(BADDIR, importFileName, expectedErrorCode, expectedErrorMsg);
            }
        });
    }

    private interface ErrorCheck{
        void check(String table, String location, SQLException se) throws Exception;
    }

    private void runImportTest(String file, ErrorCheck check) throws Exception {
        runImportTest(file,BADDIR.getCanonicalPath(),check);
    }


    private void runImportTest(String file,String badRecordsDir, ErrorCheck check) throws Exception {
        runImportTest(TABLE,file,badRecordsDir,check);
    }

    private void runImportTest(String table,String file,String badRecordsDir,ErrorCheck check) throws Exception {
        String inputFilePath = getResourceDirectory()+"test_data/bad_import/"+file;
        PreparedStatement ps = methodWatcher.prepareStatement(format("call SYSCS_UTIL.IMPORT_DATA(" +
                        "'%s'," +  // schema name
                        "'%s'," +  // table name
                        "null," +  // insert column list
                        "'%s'," +  // file path
                        "','," +   // column delimiter
                        "null," +  // character delimiter
                        "null," +  // timestamp format
                        "null," +  // date format
                        "null," +  // time format
                        "%d," +    // max bad records
                        "'%s'," +  // bad record dir
                        "null," +  // has one line records
                        "null)",   // char set
                schema.schemaName, table, inputFilePath,
                0, badRecordsDir));

        try{
            ps.execute();
            Assert.fail("No SQLException was thrown!");
        }catch(SQLException se){
            check.check(schema.schemaName+"."+table, inputFilePath, se);
        }
    }

    @Test
    public void testTimestampConversion() throws Exception{
        /*
         * Regression test for SPLICE-2151.
         * With derby.database.convertOutOfRangeTimeStamps set to true, out-of-range
         * timestamps should be converted to the max/min timestamp.
         * With derby.database.convertOutOfRangeTimeStamps set to false, out-of-range
         * timestamps should be considered bad data.
         */

        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setSchema(schema.schemaName);
        String inputFilePath = getResourceDirectory()+"ts.csv";

        try(Statement s = conn.createStatement()){
            s.execute("drop table if exists TS");
            s.execute("create table TS(a1 BIGINT, b1 timestamp, primary key(a1))");
            s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'true')");
            s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()");

            try{
                s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
                "'%s'," +  // schema name
                "'%s'," +  // table name
                "null," +  // insert column list
                "'%s'," +  // file path
                "','," +   // column delimiter
                "null," +  // character delimiter
                "'yyyy-MM-dd HH:mm:ss.S'," +  // timestamp format
                "null," +  // date format
                "null," +  // time format
                "%d," +    // max bad records
                "'%s'," +  // bad record dir
                "null," +  // has one line records
                "null)",   // char set
                schema.schemaName, "TS", inputFilePath,
                0, BADDIR.getCanonicalPath()));

                s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'false')");

            }
            // IMPORT must fail if conversion of bad timestamps is disabled.
            catch(SQLException se){
                Assert.fail("Out of range timestamp not converted!");
            }
        }

        try(Statement s = conn.createStatement()){
            s.execute("DELETE FROM TS");
            s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'true')");
            s.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_STATEMENT_CACHE()");

            boolean extendedTimestamps = true;
            try(ResultSet rs = s.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_GLOBAL_DATABASE_PROPERTY('derby.database.createTablesWithVersion2Serializer')")) {
                if (rs.next()) {
                    String aStr = rs.getString(2); //a value
                    if (!rs.wasNull()) {
                        if (aStr.equals("true"))
                            extendedTimestamps = false;
                    }
                }
            }

            s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
            "'%s'," +  // schema name
            "'%s'," +  // table name
            "null," +  // insert column list
            "'%s'," +  // file path
            "','," +   // column delimiter
            "null," +  // character delimiter
            "'yyyy-MM-dd HH:mm:ss.S'," +  // timestamp format
            "null," +  // date format
            "null," +  // time format
            "%d," +    // max bad records
            "'%s'," +  // bad record dir
            "null," +  // has one line records
            "null)",   // char set
            schema.schemaName, "TS", inputFilePath,
            0, BADDIR.getCanonicalPath()));

            try(ResultSet rs = s.executeQuery("select EXTRACT(YEAR from b1) from TS order by 1")){
                String expectedResult = extendedTimestamps ? "1  |\n" +
                "------\n" +
                "  1  |\n" +
                " 100 |\n" +
                "2017 |\n" +
                "3100 |\n" +
                "9999 |" :
                "1  |\n" +
                "------\n" +
                "1677 |\n" +
                "1677 |\n" +
                "2017 |\n" +
                "2262 |\n" +
                "2262 |";
                assertEquals(expectedResult, TestUtils.FormattedResult.ResultFactory.toString(rs));
            }
            s.execute("call syscs_util.syscs_set_global_database_property('derby.database.convertOutOfRangeTimeStamps', 'false')");
        }
    }

    @Test
    public void testTimestampInGap() throws Exception {
        /*
         * Regression test for SPLICE-2257.
         * Test importing of timestamps in the gap where the clock jumps
         * ahead for daylight savings.
         */

        Connection conn = methodWatcher.getOrCreateConnection();
        conn.setSchema(schema.schemaName);
        String inputFilePath = getResourceDirectory() + "ts7.csv";

        try (Statement s = conn.createStatement()) {
            s.execute("drop table if exists TS7");
            s.execute("create table TS7(a1 BIGINT, b1 timestamp, primary key(a1))");

            try {
                s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
                "'%s'," +  // schema name
                "'%s'," +  // table name
                "null," +  // insert column list
                "'%s'," +  // file path
                "','," +   // column delimiter
                "null," +  // character delimiter
                "'yyyy-MM-dd HH:mm:ss'," +  // timestamp format
                "null," +  // date format
                "null," +  // time format
                "%d," +    // max bad records
                "'%s'," +  // bad record dir
                "null," +  // has one line records
                "null)",   // char set
                schema.schemaName, "TS7", inputFilePath,
                0, BADDIR.getCanonicalPath()));
            }
            catch (SQLException se) {
                Assert.fail("Timestamp in daylight savings gap not parsed!");
            }

            inputFilePath = getResourceDirectory() + "ts8.csv";

            try {
                s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
                "'%s'," +  // schema name
                "'%s'," +  // table name
                "null," +  // insert column list
                "'%s'," +  // file path
                "','," +   // column delimiter
                "null," +  // character delimiter
                "'yyyy-MM-dd HH:mm:ss.SSSSSS'," +  // timestamp format
                "null," +  // date format
                "null," +  // time format
                "%d," +    // max bad records
                "'%s'," +  // bad record dir
                "null," +  // has one line records
                "null)",   // char set
                schema.schemaName, "TS7", inputFilePath,
                0, BADDIR.getCanonicalPath()));
            }
            catch (SQLException se) {
                Assert.fail("Timestamp in daylight savings gap not parsed!");
            }

            inputFilePath = getResourceDirectory() + "ts9.csv";

            try {
                s.execute(format("call SYSCS_UTIL.IMPORT_DATA(" +
                "'%s'," +  // schema name
                "'%s'," +  // table name
                "null," +  // insert column list
                "'%s'," +  // file path
                "','," +   // column delimiter
                "null," +  // character delimiter
                "'yyyy-MM-dd HH:mm:ss.S'," +  // timestamp format
                "null," +  // date format
                "null," +  // time format
                "%d," +    // max bad records
                "'%s'," +  // bad record dir
                "null," +  // has one line records
                "null)",   // char set
                schema.schemaName, "TS7", inputFilePath,
                0, BADDIR.getCanonicalPath()));
            }
            catch (SQLException se) {
                Assert.fail("Timestamp in daylight savings gap not parsed!");
            }

            try(ResultSet rs = s.executeQuery("select * from TS7 order by 1")){
                // Handle the case when we're in a time zone with daylight savings,
                // and also the case when our time zone has no daylight savings,
                String expectedResult1 =
                "A1 |            B1             |\n" +
                "--------------------------------\n" +
                " 1 |   2017-03-12 02:00:00.0   |\n" +
                " 2 |2017-03-12 02:00:00.123456 |\n" +
                " 3 |   2017-03-12 02:08:23.9   |";
                String expectedResult2 =
                "A1 |            B1             |\n" +
                "--------------------------------\n" +
                " 1 |   2017-03-12 03:00:00.0   |\n" +
                " 2 |2017-03-12 03:00:00.123456 |\n" +
                " 3 |   2017-03-12 03:08:23.9   |";
                String matchString = TestUtils.FormattedResult.ResultFactory.toString(rs);
                if (!matchString.equals(expectedResult1))
                    assertEquals(expectedResult2, matchString);
            }
            s.execute("drop table if exists TS7");
        }
    }
}
