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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.ImmutableList;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TriggerBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.io.FileFilter;
import java.io.FileInputStream;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.splicemachine.db.shared.common.reference.SQLState.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 * IT's for external table functionality
 *
 */
@SuppressFBWarnings(value={ "RV_RETURN_VALUE_IGNORED_BAD_PRACTICE", // for File.mkdir and File.delete
                            "VA_FORMAT_STRING_USES_NEWLINE", // should replace \n with %n when using String.format
                            "OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", // possible with a MultiAutoClose class, but long
                            "SQL_NONCONSTANT_STRING_PASSED_TO_EXECUTE", // intentional
                          },
                    justification = ".")
public class ExternalTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = ExternalTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private TriggerBuilder tb = new TriggerBuilder();
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    static File tempDir;

    @BeforeClass
    public static void createTempDirectory() throws Exception
    {
        tempDir = createTempDirectory(SCHEMA_NAME);
    }

    @AfterClass
    public static void deleteTempDirectory() throws Exception
    {
        deleteTempDirectory(tempDir);
    }

    /**
     * @return this will return the temp directory, that is created on demand once for each test
     */
    public String getExternalResourceDirectory() throws Exception
    {
        return tempDir.toString() + "/";
    }

    @Test
    public void testNativeSparkExtractFunction() throws Exception {
        try {
            String path = getExternalResourceDirectory() + "native-spark-table";

            methodWatcher.executeUpdate(String.format("create external table dates_ext (d date) " +
                    "STORED AS PARQUET LOCATION '%s'",path));
            methodWatcher.executeUpdate("create table dates (d date) ");

            String values = "insert into %s values '1/1/2022', '7/7/2022', '1/1/2020', '1/1/1920', '6/6/1920', '1/1/1680', '10/10/1680', null";

            for (String table : ImmutableList.of("dates", "dates_ext")) {
                methodWatcher.executeUpdate(String.format(values, table));
                try (ResultSet rs = methodWatcher.executeQuery(
                        "select d, extract(weekday from d), extract(weekdayname from d) " +
                                "from " + table + " order by 1")) {
                    String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
                    String expected =
                            "D     |  2  |    3     |\n" +
                            "-----------------------------\n" +
                            "1680-01-01 |  1  | Monday   |\n" +
                            "1680-10-10 |  4  |Thursday  |\n" +
                            "1920-01-01 |  4  |Thursday  |\n" +
                            "1920-06-06 |  7  | Sunday   |\n" +
                            "2020-01-01 |  3  |Wednesday |\n" +
                            "2022-01-01 |  6  |Saturday  |\n" +
                            "2022-07-07 |  4  |Thursday  |\n" +
                            "   NULL    |NULL |  NULL    |";
                    assertEquals("Failed with table=" + table, expected, actual);
                }
            }
        } finally {
            methodWatcher.executeUpdate("drop table dates");
            methodWatcher.executeUpdate("drop table dates_ext");
        }
    }

    @Test
    public void testNativeSparkNotFunction() throws Exception {
        try {
            String path = getExternalResourceDirectory() + "native-spark-model";

            methodWatcher.executeUpdate(String.format("create external table model (upc_number BIGINT, model_score numeric(20, 10)) " +
                    "STORED AS PARQUET LOCATION '%s'",path));

            String insert = "insert into model values (NULL, 10), (10, 20), (50, 60)";

            methodWatcher.executeUpdate(insert);
            String query =
                    "select CASE " +
                    "WHEN UPC_NUMBER IS NOT NULL THEN" +
                    "   (CASE" +
                    "   WHEN (MODEL_SCORE * 2) > 100 THEN 100" +
                    "   ELSE (MODEL_SCORE * 2)" +
                    "   END)" +
                    "ELSE MODEL_SCORE " +
                    "END AS MODEL_SCORE from model order by 1";
            try (ResultSet rs = methodWatcher.executeQuery(query)) {
                String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
                String expected =
                        "MODEL_SCORE  |\n" +
                                "----------------\n" +
                                " 10.0000000000 |\n" +
                                " 40.0000000000 |\n" +
                                "100.0000000000 |";
                assertEquals(expected, actual);
            }

            // test spark explain
            testQueryContains("sparkexplain " + query, "Project [CASE WHEN NOT isnull", methodWatcher, true);
        } finally {
            methodWatcher.executeUpdate("drop table model");
        }
    }

    @Test
    public void testInvalidSyntax() throws Exception {
        Map<String, String> errorFor = new HashMap<String, String>() { {
            put("PARQUET", "EXT01");
            put("ORC", "EXT02");
            put("AVRO", "EXT36");
        } };

        for( String fileFormat : fileFormats) {
            try {
                String tablePath = getExternalResourceDirectory() + "/foobar/foobar";
                // Row Format not supported for Parquet/ORC/AVRO
                methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                        "row format delimited fields terminated by ',' escaped by '\\' " +
                        "lines terminated by '\\n' STORED AS " + fileFormat + " LOCATION '%s'", tablePath));
                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception", errorFor.get(fileFormat), e.getSQLState());
            }
        }
    }

    void testCreateFails(String test, String tableStr, String exception) throws Exception {
        String tablePath = getExternalResourceDirectory() + "/HUMPTY_DUMPTY_MOLITOR";
        for( String fileFormat : fileFormats) {
            assureFails(String.format("create external table foo " + tableStr +
                    " STORED AS " + fileFormat + " LOCATION '%s'", tablePath), exception, test);
        }
    }

    @Test
    public void testCreateTableErrors() throws Exception {
        methodWatcher.executeUpdate("create table Cities (col1 int, col2 int, primary key (col1))");

        testCreateFails( "NoPrimaryKeysOnExternalTables",
                "(col1 int, col2 int, primary key (col1))", "EXT06");

        testCreateFails( "NoCheckConstraintsOnExternalTables",
                "(col1 int, col2 int, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000))", "EXT07");

        testCreateFails( "NoReferenceConstraintsOnExternalTables",
                "(col1 int, col2 int, CITY_ID INT CONSTRAINT city_foreign_key REFERENCES Cities)", "EXT08");

        testCreateFails( "NoUniqueConstraintsOnExternalTables",
                "(col1 int, col2 int unique)", "EXT09");

        testCreateFails( "NoGenerationClausesOnExternalTables",
                "(col1 int, col2 varchar(24), col3 GENERATED ALWAYS AS ( UPPER(col2) ))", "EXT10");

        testCreateFails( "cannotUsePartitionUndefined", "(col1 int, col2 varchar(24)) PARTITIONED BY (col3))", "EXT21");

    }

    @Test
    public void testCreateTableErrors2() throws Exception {
        String tablePath = getExternalResourceDirectory() + "createTable2";
        assureFails("create external table foo (col1 int, col2 int) LOCATION '" + tablePath + "'",
                "EXT03", "StoredAsRequired");

        for (String fileFormat : fileFormats) {
            assureFails("create external table foo (col1 int, col2 int) STORED AS " + fileFormat,
                    "EXT04", "locationMissing");

            assureFails(String.format("create table foo (col1 int, col2 varchar(24))" +
                    " STORED AS " + fileFormat + " LOCATION '%s'", tablePath), "EXT18", "missingExternal");
        }
    }

    @Test
    public void testFileExistingNotDeleted() throws  Exception{
        String tablePath = getResourceDirectory()+"parquet_sample_one";

        File newFile = new File(tablePath);
        String[] files = newFile.list();
        int count = files == null ? 0 : files.length;

        methodWatcher.executeUpdate(String.format("create external table table_to_existing_file (\"partition1\" varchar(24), \"column1\" varchar(24), \"column2\" varchar(24))" +
                " PARTITIONED BY (\"partition1\") STORED AS PARQUET  LOCATION '%s'",tablePath));

        files = newFile.list();
        int count2 = files == null ? 0 : files.length;
        Assert.assertEquals(String.format("File : %s have been modified and it shouldn't",tablePath),count, count2);

        ResultSet rs = methodWatcher.executeQuery("select * from table_to_existing_file order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "partition1 | column1 | column2 |\n" +
                        "--------------------------------\n" +
                        "    AAA    |   AAA   |   AAA   |\n" +
                        "    BBB    |   BBB   |   BBB   |\n" +
                        "    CCC    |   CCC   |   CCC   |";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void createParquetTableMergeSchema() throws  Exception{
        String tablePath = getTempCopyOfResourceDirectory(tempDir, "parquet_sample_one" );
        try {
            // check also that merging doesn't require writeable directory
            new File(tablePath).setWritable(false);

            methodWatcher.executeUpdate(String.format("create external table parquet_table_to_existing_file (\"partition1\" varchar(24), \"column1\" varchar(24), \"column2\" varchar(24))" +
                    " PARTITIONED BY (\"partition1\") STORED AS PARQUET  LOCATION '%s'", tablePath));

            ResultSet rs = methodWatcher.executeQuery("select * from parquet_table_to_existing_file order by 1");
            String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected =
                    "partition1 | column1 | column2 |\n" +
                            "--------------------------------\n" +
                            "    AAA    |   AAA   |   AAA   |\n" +
                            "    BBB    |   BBB   |   BBB   |\n" +
                            "    CCC    |   CCC   |   CCC   |";
            Assert.assertEquals(actual, expected, actual);
        }
        finally {
            // otherwise cleanup scripts can't delete this directory
            new File(tablePath).setWritable(true);
        }
    }

    @Test
    public void createAvroTableMergeSchema() throws  Exception{
        String tablePath = getResourceDirectory()+"avro_simple_file_test";

        methodWatcher.executeUpdate(String.format("create external table avro_table_to_existing_file (col1 int, col2 int, col3 int)" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        ResultSet rs = methodWatcher.executeQuery("select * from avro_table_to_existing_file order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";

        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testFileExistingNotDeletedAvro() throws  Exception{
        String tablePath = getResourceDirectory()+"avro_simple_file_test";
        File newFile = new File(tablePath);
        String[] files = newFile.list();
        int count = files == null ? 0 : files.length;

        methodWatcher.executeUpdate(String.format("create external table table_to_existing_file_avro (col1 int, col2 int, col3 int)" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        files = newFile.list();
        int count2 = files == null ? 0 : files.length;
        Assert.assertEquals(String.format("File : %s have been modified and it shouldn't",tablePath),count,count2);

        ResultSet rs = methodWatcher.executeQuery("select * from table_to_existing_file_avro order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected = "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  1  |  1  |\n" +
                "  2  |  2  |  2  |\n" +
                "  3  |  3  |  3  |";

        Assert.assertEquals(actual, expected, actual);
    }


    String[] fileFormats = { "PARQUET", "ORC", "AVRO" };

    void assureFails(String query, String exceptionType, String test) throws Exception {
        try {
            methodWatcher.executeUpdate(query);
            Assert.fail(test + ": Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals(test + ": Wrong Exception (" + e.getMessage() + ")", exceptionType, e.getSQLState());
        }
    }

    void assureQueryFails(String query, String exceptionType, String test) throws Exception {
        try {
            methodWatcher.executeQuery(query);
            Assert.fail(test + ": Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals(test + ": Wrong Exception (" + e.getMessage() + ")", exceptionType, e.getSQLState());
        }
    }

    // tests that we can't UPDATE or DELETE values in external tables
    // and correct exceptions are thrown
    @Test
    public void testCannotDeleteOrUpdateExternalTable() throws Exception {
        for( String fileFormat : fileFormats) {
            String name = "delete_foo" + fileFormat;
            String tablePath = getExternalResourceDirectory() + "/" + name;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24))" +
                    " STORED AS " + fileFormat + " LOCATION '%s'", tablePath));

            assureFails("delete from " + name + " where col1 = 4", "EXT05", "");
            assureFails("update " + name + " set col1 = 4", "EXT05", "");
        }
    }

    // tests we can create an external table in a empty directory
    // also test if we can create a schema automatically
    @Test
    public void testEmptyDirectoryAndSchemaCreate() throws  Exception{
        for( String fileFormat : fileFormats) {
            String tablePath = getExternalResourceDirectory() + "empty_directory" + fileFormat;
            String name = "AUTO_SCHEMA_XT.AAA";
            new File(tablePath).mkdir();

            try {
                methodWatcher.executeUpdate(String.format("create external table " + name +
                        " (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                        " STORED AS " + fileFormat + " LOCATION '%s'", tablePath));

                ResultSet rs = methodWatcher.executeQuery("SELECT SCHEMANAME FROM SYS.SYSSCHEMAS WHERE SCHEMANAME = 'AUTO_SCHEMA_XT'");
                String expected = "SCHEMANAME   |\n" +
                        "----------------\n" +
                        "AUTO_SCHEMA_XT |";
                assertEquals("list of schemas does not match", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
                rs.close();
            }
            finally {
                methodWatcher.execute("DROP TABLE " + name);
                methodWatcher.execute("DROP SCHEMA AUTO_SCHEMA_XT RESTRICT");
            }
        }
    }


    @Test
    public void testLocationCannotBeAFile() throws  Exception{
        File temp = File.createTempFile("temp-file", ".tmp", tempDir);
        for( String fileFormat : fileFormats) {
            assureFails(String.format("create external table table_to_existing_file " +
                            "(col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                            " STORED AS " + fileFormat + " LOCATION '%s'", temp.getAbsolutePath()),
                    "EXT33", "");
        }
        temp.delete();
    }

    @Test
    public void refreshRequireExternalTable() throws  Exception{
        for( String fileFormat : fileFormats) {
            String name = "XT_REFRESH_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + name;

            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24))" +
                    " STORED AS " + fileFormat + " LOCATION '%s'", tablePath));

            PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) ");
            ps.setString(1, "EXTERNALTABLEIT");
            ps.setString(2, name);
            ps.execute();
        }
    }

    @Test
    public void refreshRequireExternalTableNotFound() throws  Exception{

        try (PreparedStatement ps
                     = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) "))
        {
            ps.setString(1, "EXTERNALTABLEIT");
            ps.setString(2, "NOT_EXIST");
            ps.executeQuery().close();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X05",e.getSQLState());
        }
    }

    //SPLICE-1387
    @Test
    public void refreshRequireExternalTableWrongParameters() throws  Exception{

        try ( PreparedStatement ps
                      = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE('arg1','arg2','arg3') "))
        {
            ps.executeQuery().close();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42Y03",e.getSQLState());
        }

    }

    public void testWriteReadFromSimpleExternalTable(String fileFormat) throws Exception {
        String name = "write_all_datatypes_" + fileFormat;
        String file = getExternalResourceDirectory() + name;
        int[] values = {1, 2, 3, 4, 0 /* = NULL */ };
        int[] colTypes = CreateTableTypeHelper.getTypes(fileFormat);
        CreateTableTypeHelper types = new CreateTableTypeHelper(colTypes, values);

        String externalTableOptions = " COMPRESSED WITH SNAPPY STORED AS " + fileFormat + " LOCATION '"
                + file + "'";
        methodWatcher.executeUpdate("create external table " + name + " (" + types.getSchema() + ")" + externalTableOptions);
        int insertCount = methodWatcher.executeUpdate( "insert into " + name + " values " + types.getInsertValues() + "");
        Assert.assertEquals(fileFormat + ": insertCount is wrong", values.length, insertCount);

        // test select *, result matches expectations (= inserted values)
        ResultSet rs = methodWatcher.executeQuery("select * from " + name );
        types.checkResultSetSelectAll(rs);

        // test NULLs
        ResultSet rs2 = methodWatcher.executeQuery("select COL_VARCHAR from " + name + " where COL_VARCHAR is null");
        Assert.assertEquals(fileFormat, "COL_VARCHAR |\n" +
                "--------------\n" +
                "    NULL     |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        if( !fileFormat.equals("ORC") ) { // this doesn't work for ORC?!
            ResultSet rs3 = methodWatcher.executeQuery("select COL_VARCHAR from " + name + " where COL_VARCHAR is not null");
            Assert.assertEquals(fileFormat, "COL_VARCHAR |\n" +
                    "--------------\n" +
                    "   AAAA 1    |\n" +
                    "   AAAA 2    |\n" +
                    "   AAAA 3    |\n" +
                    "   AAAA 4    |", TestUtils.FormattedResult.ResultFactory.toString(rs3));
        }

        // test schema suggestion when we're using a wrong schema
        try {
            methodWatcher.executeUpdate("create external table all_datatypes_wrong ( i int )" + externalTableOptions);
            Assert.fail( fileFormat + ": Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals(fileFormat + ": Wrong Exception", INCONSISTENT_NUMBER_OF_ATTRIBUTE, e.getSQLState());
            Assert.assertEquals(fileFormat + ": wrong exception message",
                    "1 attribute(s) defined but " + colTypes.length + " present in the external file : '" + file + "'. " +
                            "Suggested Schema is 'CREATE EXTERNAL TABLE T (" + types.getSuggestedTypes() + ");'.",
                    e.getMessage());
        }
    }

    // tests writing all columns types, null values, suggesting schema.
    @Test
    @Ignore("DB-10033")
    public void testWriteReadFromSimpleExternalTable() throws Exception {
        for( String fileFormat : fileFormats )
            testWriteReadFromSimpleExternalTable(fileFormat);

    }

    @Test
    public void testWriteReadCharVarcharTruncation() throws Exception {
        for( String fileFormat : fileFormats ){
            // we're using our internal ORC reader which doesn't support CHAR padding or CHAR/VARCHAR truncation
            // see DB-9911 for reevalutation of this
            if( fileFormat.equals("ORC")) {
                continue;
            }
            String name = "char_varchar_" + fileFormat;

            String file = getExternalResourceDirectory() + name;
            methodWatcher.executeUpdate("create external table " + name +
                    " ( col1 varchar(100), col2 varchar(100), col3 varchar(100) ) " +
                    "STORED AS " + fileFormat + " LOCATION '" + file + "'");

            int insertCount = methodWatcher.executeUpdate("insert into " + name + " values " +
                    "( '123456789', '123456789', '12345')");
            Assert.assertEquals("insertCount is wrong", 1, insertCount);

            ResultSet rs1 = methodWatcher.executeQuery("select * from " + name);
            List<String> res1 = CreateTableTypeHelper.getListResult(rs1);
            Assert.assertEquals(1, res1.size());
            Assert.assertEquals("'123456789', '123456789', '12345'", res1.get(0));

            methodWatcher.execute("drop table " + name );

            // create table in same location, but with shorter strings
            methodWatcher.executeUpdate("create external table " + name + " ( col1 VARCHAR(5), col2 CHAR(5), col3 CHAR(10) ) " +
                    "STORED AS " + fileFormat + " LOCATION '" + file + "'");

            ResultSet rs2 = methodWatcher.executeQuery("select * from " + name);
            List<String> res2 = CreateTableTypeHelper.getListResult(rs2);
            Assert.assertEquals(1, res2.size());
            Assert.assertEquals("'12345', '12345', '12345     '", res2.get(0));

            methodWatcher.execute("drop table " + name );
        }
    }

    @Test
    public void testCreateExternalTableWithEmptyORCDataFile() throws Exception {
        String tablePath = getExternalResourceDirectory()+"EXT_ORC";
        methodWatcher.executeUpdate(String.format("create external table EXT_ORC (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.execute("drop table EXT_ORC");

        int isCreated = methodWatcher.executeUpdate(String.format("create external table EXT_ORC_2 (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'",tablePath));

        Assert.assertEquals(isCreated, 0);
    }

    @Test
    public void testReadFromEmptyParquetExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"empty_parquet";
        methodWatcher.executeUpdate(String.format("create external table empty_parquet (col1 int, col2 varchar(24)) PARTITIONED BY (col2)" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        ResultSet rs = methodWatcher.executeQuery("select * from empty_parquet");
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    @Test
    public void testReadFromPartitionedEmptyAvroExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"empty_avro";
        methodWatcher.executeUpdate(String.format("create external table empty_avro (col1 int, col2 varchar(24)) PARTITIONED BY (col1)" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        ResultSet rs = methodWatcher.executeQuery("select * from empty_avro");
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    // todo: move to testWriteReadFromSimpleExternalTablefor( String storedAs : fileFormats )
    @Test
    public void testWriteReadFromSimpleCsvExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"dt_txt";
        methodWatcher.executeUpdate(String.format("create external table dt_txt(a date) stored as textfile location '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into dt_txt values ('2017-01-25')"));
        Assert.assertEquals("insertCount is wrong",1,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select a from dt_txt");
        Assert.assertEquals("A     |\n" +
                "------------\n" +
                "2017-01-25 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testReadSmallIntFromCsv() throws Exception {

        String tablePath = getExternalResourceDirectory()+"small_int_txt";
        methodWatcher.executeUpdate(String.format("create external table small_int_txt(a smallint) stored as textfile location '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into small_int_txt values (12)"));
        Assert.assertEquals("insertCount is wrong",1,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select a from small_int_txt");
        Assert.assertEquals("A |\n" +
                "----\n" +
                "12 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testWriteReadWithPartitionedByFloatTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"simple_parquet_with_partition";
        methodWatcher.executeUpdate(String.format("create external table simple_parquet_with_partition (col1 int, col2 varchar(24), col3 float(10) )" +
                "partitioned  by (col3) STORED AS PARQUET LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_parquet_with_partition values (1,'XXXX',3.4567)," +
                "(2,'YYYY',540.3434)," +
                "(3,'ZZZZ',590.3434654)"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_parquet_with_partition");
        Assert.assertEquals("COL1 |COL2 |  COL3    |\n" +
                "-----------------------\n" +
                "  1  |XXXX | 3.4567   |\n" +
                "  2  |YYYY |540.3434  |\n" +
                "  3  |ZZZZ |590.34344 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadWithPartitionedByFloatTableAvro() throws Exception {
        String tablePath = getExternalResourceDirectory()+"simple_avro_with_partition";
        methodWatcher.executeUpdate(String.format("create external table simple_avro_with_partition (col1 int, col2 varchar(24), col3 float(10) )" +
                "partitioned  by (col3) STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_avro_with_partition values (1,'XXXX',3.4567)," +
                "(2,'YYYY',540.3434)," +
                "(3,'ZZZZ',590.3434654)"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_avro_with_partition");
        Assert.assertEquals("COL1 |COL2 |  COL3    |\n" +
                "-----------------------\n" +
                "  1  |XXXX | 3.4567   |\n" +
                "  2  |YYYY |540.3434  |\n" +
                "  3  |ZZZZ |590.34344 |",TestUtils.FormattedResult.ResultFactory.toString(rs));


    }


    @Test
    // SPLICE-1219
    @Ignore("SPLICE-1514")
    public void testLocalBroadcastColumnar() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table left_side_bcast (col1 int, col2 int)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"left_side_bcast"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into left_side_bcast values (1,5)," +
                "(2,2)," +
                "(3,3)"));
        methodWatcher.executeUpdate(String.format("create external table right_side_bcast (col1 int, col2 int)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"right_side_bcast"));
        int insertCount2 = methodWatcher.executeUpdate(String.format("insert into right_side_bcast values (1,1)," +
                "(2,2)," +
                "(3,3)"));

        Assert.assertEquals("insertCount is wrong",3,insertCount);
        Assert.assertEquals("insertCount is wrong",3,insertCount2);

        ResultSet rs = methodWatcher.executeQuery("select * from --splice-properties joinOrder=fixed\n " +
                "left_side_bcast l inner join right_side_bcast r --splice-properties joinStrategy=BROADCAST\n" +
                " on l.col1 = r.col1 and l.col2 > r.col2+1 ");
        Assert.assertEquals("COL1 |COL2 |COL1 |COL2 |\n" +
                "------------------------\n" +
                "  1  |  5  |  1  |  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    // SPLICE-1219
    @Ignore("SPLICE-1514")
    public void testLocalBroadcastColumnarAvro() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table left_side_bcast (col1 int, col2 int)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"left_side_bcast"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into left_side_bcast values (1,5)," +
                "(2,2)," +
                "(3,3)"));
        methodWatcher.executeUpdate(String.format("create external table right_side_bcast (col1 int, col2 int)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"right_side_bcast"));
        int insertCount2 = methodWatcher.executeUpdate(String.format("insert into right_side_bcast values (1,1)," +
                "(2,2)," +
                "(3,3)"));

        Assert.assertEquals("insertCount is wrong",3,insertCount);
        Assert.assertEquals("insertCount is wrong",3,insertCount2);

        ResultSet rs = methodWatcher.executeQuery("select * from --splice-properties joinOrder=fixed\n " +
                "left_side_bcast l inner join right_side_bcast r --splice-properties joinStrategy=BROADCAST\n" +
                " on l.col1 = r.col1 and l.col2 > r.col2+1 ");
        Assert.assertEquals("COL1 |COL2 |COL1 |COL2 |\n" +
                "------------------------\n" +
                "  1  |  5  |  1  |  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testWriteReadFromPartitionedExternalTable() throws Exception {
        for( String fileFormat : fileFormats ) {
            String name = "partitioned_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + name;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24))" +
                    "partitioned by (col2) STORED AS " + fileFormat + " LOCATION '%s'", tablePath));
            int insertCount = methodWatcher.executeUpdate(String.format("insert into " + name + " values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));
            Assert.assertEquals("insertCount is wrong", 3, insertCount);
            ResultSet rs = methodWatcher.executeQuery("select * from " + name );
            Assert.assertEquals("COL1 |COL2 |\n" +
                    "------------\n" +
                    "  1  |XXXX |\n" +
                    "  2  |YYYY |\n" +
                    "  3  |ZZZZ |", TestUtils.FormattedResult.ResultFactory.toString(rs));

            //Make sure empty file is created
            Assert.assertTrue(String.format("Table %s hasn't been created", tablePath), new File(tablePath).exists());
        }
    }

    @Test
    public void testWriteReadWithPreExistingParquet() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table parquet_simple_file_table (\"partition1\" varchar(24), \"column1\" varchar(24), \"column2\" varchar(24))\n" +
                "partitioned by (\"partition1\") STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select \"column1\" from parquet_simple_file_table where \"partition1\"='AAA'");
        Assert.assertEquals("column1 |\n" +
                "----------\n" +
                "   AAA   |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testParquetErrorSuggestSchemaGiven() throws Exception {
        // test schema suggestion on a given file

        String file = getResourceDirectory() + "parquet_sample_one";
        String suggested = " Suggested Schema is 'CREATE EXTERNAL TABLE T (column1 CHAR/VARCHAR(x), column2 CHAR/VARCHAR(x), partition1 CHAR/VARCHAR(x));'.";
        try{
            methodWatcher.executeUpdate("create external table testParquetErrorSuggestSchemaGiven" +
                                         " (col1 INTEGER) STORED AS PARQUET LOCATION '" + file + "'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception", INCONSISTENT_NUMBER_OF_ATTRIBUTE, e.getSQLState());
            Assert.assertEquals( "wrong exception message",
                    "1 attribute(s) defined but 3 present in the external file : '" + file + "'." + suggested,
                    e.getMessage() );
        }

        try{
            methodWatcher.executeUpdate("create external table testParquetErrorSuggestSchemaGiven2" +
                    " (decimal_col1 DECIMAL(7, 2), col2_int INTEGER, d DOUBLE) STORED AS PARQUET LOCATION '" + file + "'");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            //Assert.assertEquals("Wrong Exception", INCONSISTENT_DATATYPE_ATTRIBUTES, e.getSQLState());
            Assert.assertEquals( "wrong exception message",
                    "The field 'DECIMAL_COL1':'DECIMAL(7,2)' defined in the table is not compatible with " +
                    "the field 'column1':'CHAR/VARCHAR(x)' defined in the external file '" + file + "'." + suggested,
                    e.getMessage() );
        }
    }

    // this test checks that when using a wrong schema, the suggested CREATE TABLE statement is
    // exactly as the actually used CREATE TABLE Statement
    @Test
    public void testWriteAllDataTypesCheckSuggest() throws Exception {
        String file_types[] = {"ORC", "PARQUET"};
        for( String file_type : file_types ) {
            String name = "all_datatypes_" + file_type;

            String file = getExternalResourceDirectory() + name;
            // parquet currently can't use TIME, will be written (or infered) as TIMESTAMP
            // see DB-9710

            // we currently can't infere the length of CHAR/VARCHAR, so suggestion is CHAR/VARCHAR(x)
            String nonCharTypes = "COL_DATE DATE, COL_TIMESTAMP TIMESTAMP, " +
                    "COL_TINYINT TINYINT, COL_SMALLINT SMALLINT, COL_INTEGER INT, COL_BIGINT BIGINT, " +
                    "COL_DECIMAL DECIMAL(7,2), COL_DOUBLE DOUBLE, COL_REAL REAL, COL_BOOLEAN BOOLEAN";
            String schema = "COL_CHAR char(10), COL_VARCH varchar(24), " + nonCharTypes;
            String suggestedTypes = "COL_CHAR CHAR/VARCHAR(x), COL_VARCH CHAR/VARCHAR(x), " + nonCharTypes;
            String values = "'AAAA', 'AAAA', " +
                    "'2017-7-27', '2013-03-23 09:45:00', " +
                    "1, 1, 1, 1, " +
                    "12.34, 1.5, 1.5, TRUE";

            String externalTableOptions = " COMPRESSED WITH SNAPPY STORED AS PARQUET LOCATION '" + file + "'";
            methodWatcher.executeUpdate("create external table " + name + " (" + schema + ")" + externalTableOptions);

            int insertCount = methodWatcher.executeUpdate("insert into " + name + " values (" + values + ")");
            Assert.assertEquals("insertCount is wrong", 1, insertCount);

            try {
                methodWatcher.executeUpdate("create external table all_datatypes_wrong ( i int )" + externalTableOptions);
                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception", INCONSISTENT_NUMBER_OF_ATTRIBUTE, e.getSQLState());
                Assert.assertEquals("wrong exception message",
                        "1 attribute(s) defined but 12 present in the external file : '" + file + "'. " +
                                "Suggested Schema is 'CREATE EXTERNAL TABLE T (" + suggestedTypes + ");'.",
                        e.getMessage());
            }
        }
    }

    @Test
    public void testWriteReadWithPreExistingAvro() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table avro_simple_file_table (col1 int, col2 int, col3 int)" +
                "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from avro_simple_file_table where col1=1");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                "  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadWithPreExistingParquetAndOr() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table parquet_simple_file_table_and_or (\"partition1\" varchar(24), \"column1\" varchar(24), \"column2\" varchar(24))\n" +
                "partitioned by (\"partition1\") STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select \"column1\" from parquet_simple_file_table_and_or where \"partition1\"='BBB' OR ( \"partition1\"='AAA' AND \"column1\"='AAA')");
        Assert.assertEquals("column1 |\n" +
                "----------\n" +
                "   AAA   |\n" +
                "   BBB   |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadWithPreExistingAvroAndOr() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table avro_simple_file_table_and_or (col1 int, col2 int, col3 int)" +
                "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from avro_simple_file_table_and_or where col1=1 OR ( col1=2 AND col2=2)");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                "  1  |\n" +
                "  2  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testWriteReadFromExternalTable() throws Exception {

        String[] compressionTypes = { "", // no compression
                "COMPRESSED WITH SNAPPY",
                "COMPRESSED WITH ZLIB"};
        for( String compression : compressionTypes )
        {
            for( String fileFormat : fileFormats )
            {
                String path = getExternalResourceDirectory() + "compressed_test";
                String createSql = "create external table compressed_test (col1 int, col2 varchar(24)) "
                        + compression + " STORED AS " + fileFormat + " LOCATION '" + path + "'";
                try {
                    FileUtils.deleteDirectory(new File(path));
                    methodWatcher.executeUpdate(createSql);
                    int insertCount = methodWatcher.executeUpdate(String.format("insert into compressed_test values (1,'XXXX')," +
                            "(2,'YYYY')"));
                    Assert.assertEquals("insertCount is wrong for " + createSql, 2, insertCount);
                    ResultSet rs = methodWatcher.executeQuery("select * from compressed_test");
                    Assert.assertEquals("different output for " + createSql, "COL1 |COL2 |\n" +
                            "------------\n" +
                            "  1  |XXXX |\n" +
                            "  2  |YYYY |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                    rs.close();
                    methodWatcher.execute("drop table compressed_test");
                    FileUtils.deleteDirectory(new File(path));
                } catch( Exception e )
                {
                    Assert.fail( "failed at " + createSql + " with exception:  \n" + e.toString()  );
                }
            }
        }
    }

    @Test
    public void testReadFailingNumberAttributeConstraint() throws Exception {
        try{
                methodWatcher.executeUpdate(String.format("create external table failing_number_attribute(col1 varchar(24), col2 varchar(24), col3 int, col4 int)" +
                        "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_sample_one"));
                ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_number_attribute where col1='AAA'");
                Assert.assertEquals("COL2 |\n" +
                        "------\n" +
                        " AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs));
                 Assert.fail("Exception not thrown");
            } catch (SQLException e) {

                Assert.assertEquals("Wrong Exception","EXT23",e.getSQLState());
            }
    }

    @Test
    public void testReadFailingNumberAttributeConstraintAvro() throws Exception {
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_number_attribute(col1 int, col2 int, col3 int, col4 int)" +
                    "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_number_attribute where col1='1'");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {

            Assert.assertEquals("Wrong Exception","EXT23",e.getSQLState());
        }
    }

    @Test
    public void testReadFailingNumberAttributeConstraintAvro_2() throws Exception {
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_number_attribute_2(col1 int, col2 int)" +
                    "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_number_attribute_2 where col1='1'");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {

            Assert.assertEquals("Wrong Exception","EXT23",e.getSQLState());
        }
    }

    @Test
    public void testReadFailingAttributeDataTypeConstraint() throws Exception {
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_data_type_attribute(col1 int, col2 varchar(24), col4 varchar(24))" +
                    "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_sample_one"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_data_type_attribute where col1='AAA'");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    " AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {

            Assert.assertEquals("Wrong Exception","EXT24",e.getSQLState());
        }
    }

    @Test
    public void testReadFailingAttributeDataTypeConstraintAvro() throws Exception {
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_data_type_attribute(col1 int, col2 varchar(24), col4 varchar(24))" +
                    "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_data_type_attribute where col1=1");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  1  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {

            Assert.assertEquals("Wrong Exception","EXT24",e.getSQLState());
        }
    }

    @Test
    public void testReadInCompatibleAttributeDataTypeAvro() throws Exception {
        String filename = getResourceDirectory() + "avro_simple_file_test";
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_data_type_attribute(col1 int, col2 varchar(24), col4 varchar(24))" +
                    "STORED AS AVRO LOCATION '%s'", filename));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception",
                                    "The field 'COL2':'CHAR/VARCHAR(x)' defined in the table is not compatible with " +
                                    "the field 'c1':'INT' defined in the external file '" + filename + "'. " +
                                    "Suggested Schema is 'CREATE EXTERNAL TABLE T (c0 INT, c1 INT, c2 INT);'.",
                    e.getMessage());
        }
    }

    @Test
    public void testReadPassedConstraint() throws Exception {
            methodWatcher.executeUpdate(String.format("create external table failing_correct_attribute(\"partition1\" varchar(24), \"column1\" varchar(24), \"column2\" varchar(24))" +
                    "partitioned by (\"partition1\") STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_sample_one"));
            ResultSet rs = methodWatcher.executeQuery("select \"column1\" from failing_correct_attribute where \"column1\"='AAA'");
            Assert.assertEquals("column1 |\n" +
                    "----------\n" +
                    "   AAA   |",TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    @Test
    public void testReadPassedConstraintAvro() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table failing_correct_attribute_avro(col1 int, col2 int, col4 int)" +
                "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_correct_attribute_avro where col1=2");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                "  2  |",TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    @Test //DB-6809
    public void testReadTextExternalTableDelimiter() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table external_t1 (col1 int, col2 varchar(20), col3 date)" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'" +
                " STORED AS TEXTFILE LOCATION '%s'", getResourceDirectory()+"test_external_text"));
        ResultSet rs = methodWatcher.executeQuery("select * from external_t1");
        Assert.assertEquals("COL1 | COL2   |   COL3    |\n" +
                "---------------------------\n" +
                "  1  |SPLI\"CE |2014-05-20 |\n" +
                "  2  |MACHINE |2015-09-02 |" ,TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test // DB-9682
    public void testReadTextMismatchSchema() throws Exception {
        String file = getResourceDirectory() + "test_external_text";
        String formats[] = {
                "(col1 int, col2 varchar(20), col3 date, col4 date)", // too much
                "(col1 int, col2 varchar(20) )"};                     // to little
        String numCols[] = {"4", "2"};
        for (int i = 0; i < 2; i++)
        {
            try {
                methodWatcher.executeUpdate(String.format("create external table external_t2 " + formats[i] +
                        "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'" +
                        " STORED AS TEXTFILE LOCATION '%s'", file));
                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception (" + e.getMessage() + ")", INCONSISTENT_NUMBER_OF_ATTRIBUTE, e.getSQLState());
                Assert.assertEquals("wrong exception message",
                        numCols[i] + " attribute(s) defined but 3 present " +
                                "in the external file : '" + file + "'. Suggested Schema is 'CREATE EXTERNAL TABLE T " +
                                "(_c0 CHAR/VARCHAR(x), _c1 CHAR/VARCHAR(x), _c2 CHAR/VARCHAR(x));'.", e.getMessage());
            }
        }
    }

    @Test
    public void testWriteReadFromSimpleTextExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table simple_text (col1 int, col2 varchar(24), col3 boolean)" +
                " STORED AS TEXTFILE LOCATION '%s'", getExternalResourceDirectory()+"simple_text"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_text values (1,'XXXX',true)," +
                "(2,'YYYY',false)," +
                "(3,'ZZZZ', true)"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_text");
        Assert.assertEquals("COL1 |COL2 |COL3  |\n" +
                "-------------------\n" +
                "  1  |XXXX |true  |\n" +
                "  2  |YYYY |false |\n" +
                "  3  |ZZZZ |true  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadFromCompressedErrorTextExternalTable() throws Exception {
        assureFails(String.format("create external table compressed_ignored_text (col1 int, col2 varchar(24))" +
                        "COMPRESSED WITH SNAPPY STORED AS TEXTFILE LOCATION '%s'", getExternalResourceDirectory()+"compressed_ignored_text"),
                "EXT17", "");
    }

    @Test
    public void testExternalTableDescriptorCompression() throws Exception {
        //with no compression token
        methodWatcher.executeUpdate(String.format("create external table simple_table_none_orc (col1 int, col2 varchar(24))" +
                "partitioned by (col1) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"simple_table_none_orc"));
        ResultSet rs = methodWatcher.executeQuery("select COMPRESSION from SYS.SYSTABLES where tablename='SIMPLE_TABLE_NONE_ORC'");
        Assert.assertEquals("COMPRESSION |\n" +
                "--------------\n" +
                "    none     |",TestUtils.FormattedResult.ResultFactory.toString(rs));


        //with compression snappy
        methodWatcher.executeUpdate(String.format("create external table simple_table_snappy_orc (col1 int, col2 varchar(24))" +
                "compressed with snappy partitioned by (col2) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"simple_table_snappy_orc"));
        rs = methodWatcher.executeQuery("select COMPRESSION from SYS.SYSTABLES where tablename='SIMPLE_TABLE_SNAPPY_ORC'");
        Assert.assertEquals("COMPRESSION |\n" +
                "--------------\n" +
                "   snappy    |",TestUtils.FormattedResult.ResultFactory.toString(rs));

        //with compression zlib
        methodWatcher.executeUpdate(String.format("create external table simple_table_ZLIB_orc (col1 int, col2 varchar(24))" +
                "compressed with zlib partitioned by (col2) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"simple_table_ZLIB_orc"));
        rs = methodWatcher.executeQuery("select COMPRESSION from SYS.SYSTABLES where tablename='SIMPLE_TABLE_ZLIB_ORC'");
        Assert.assertEquals("COMPRESSION |\n" +
                "--------------\n" +
                "    zlib     |",TestUtils.FormattedResult.ResultFactory.toString(rs));

    }

    @Test
    public void testReadEmptyFile() throws Exception {
        for( String fileFormat : new String[]{"ORC", "PARQUET", "AVRO", "TEXTFILE"}) {
            String tablePath = getExternalResourceDirectory() + "test_empty_" + fileFormat;
            String name = "TEST_EMPTY_" + fileFormat;

            // this will create an empty file in the path
            methodWatcher.executeUpdate("CREATE EXTERNAL TABLE " + name + " (t1 varchar(30), t2 varchar(30)) \n" +
                    "STORED AS TEXTFILE location '" + tablePath + "'");

            ResultSet rs = methodWatcher.executeQuery("select * from " + name );
            Assert.assertTrue(new File(tablePath).exists());
            Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs));

            methodWatcher.execute("drop table " + name );
            // make sure we can open that table again and not get a "0 attributes in file" error for CSV
            methodWatcher.executeUpdate("CREATE EXTERNAL TABLE " + name + " (t1 varchar(30), t2 varchar(30)) \n" +
                    "STORED AS TEXTFILE location '" + tablePath + "'");
        }
    }

    @Test
    public void testPinExternalOrcTable() throws Exception {
        String path = getExternalResourceDirectory()+"orc_pin";
        methodWatcher.executeUpdate(String.format("create external table orc_pin (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'", path));
        methodWatcher.executeUpdate("insert into orc_pin values (1,'test')");

        methodWatcher.executeUpdate("pin table orc_pin");
        ResultSet rs = methodWatcher.executeQuery("select * from orc_pin --splice-properties pin=true");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |test |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }



    @Test
    public void testPinExternalParquetTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table parquet_pin (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"parquet_pin"));
        methodWatcher.executeUpdate("insert into parquet_pin values (1,'test')");

        methodWatcher.executeUpdate("pin table parquet_pin");
        ResultSet rs = methodWatcher.executeQuery("select * from parquet_pin --splice-properties pin=true");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |test |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testPinExternalAvroTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table avro_pin (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"avro_pin"));
        methodWatcher.executeUpdate("insert into avro_pin values (1,'test')");

        methodWatcher.executeUpdate("pin table avro_pin");
        ResultSet rs = methodWatcher.executeQuery("select * from avro_pin --splice-properties pin=true");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |test |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testPinExternalTextTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table textfile_pin (col1 int, col2 varchar(24))" +
                " STORED AS TEXTFILE LOCATION '%s'", getExternalResourceDirectory()+"textfile_pin"));
        methodWatcher.executeUpdate("insert into textfile_pin values (1,'test')");

        methodWatcher.executeUpdate("pin table textfile_pin");
        ResultSet rs = methodWatcher.executeQuery("select * from textfile_pin --splice-properties pin=true");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |test |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    // SPLICE-1180
    public void testReadTimeFromFile() throws Exception {

        methodWatcher.executeUpdate(String.format("create external table timestamp_parquet (a time)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"timestamp_parquet"));
        methodWatcher.executeUpdate("insert into timestamp_parquet values ('22:22:22')");
        ResultSet rs = methodWatcher.executeQuery("select * from timestamp_parquet");
        Assert.assertEquals("A    |\n" +
                "----------\n" +
                "22:22:22 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    // SPLICE-1180
    public void testReadClobFromFile() throws Exception {

        methodWatcher.executeUpdate(String.format("create external table clob_parquet (largecol clob(65535))" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"clob_parquet"));
        methodWatcher.executeUpdate("insert into clob_parquet values ('asdfasfd234234')");
        ResultSet rs = methodWatcher.executeQuery("select * from clob_parquet");
        Assert.assertEquals("LARGECOL    |\n" +
                "----------------\n" +
                "asdfasfd234234 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    // SPLICE-1180
    public void testReadClobFromFileAvro() throws Exception {

        methodWatcher.executeUpdate(String.format("create external table clob_avro (largecol clob(65535))" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"clob_avro"));
        methodWatcher.executeUpdate("insert into clob_avro values ('asdfasfd234234')");
        ResultSet rs = methodWatcher.executeQuery("select * from clob_avro");
        Assert.assertEquals("LARGECOL    |\n" +
                "----------------\n" +
                "asdfasfd234234 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    // SPLICE-1180
    public void testReadSmallIntFromFile() throws Exception {

        methodWatcher.executeUpdate(String.format("create external table short_parquet (col1 smallint)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"short_parquet"));
        methodWatcher.executeUpdate("insert into short_parquet values (12)");
        ResultSet rs = methodWatcher.executeQuery("select * from short_parquet");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                " 12  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testDateOrcReader() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/DateOrc";
        methodWatcher.executeUpdate(String.format("create external table t_date (c1 date) " +
                "STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeUpdate("insert into t_date values('2017-7-27'), ('1970-01-01')");
        ResultSet rs = methodWatcher.executeQuery("select * from t_date order by 1");

        String expected = "C1     |\n" +
                "------------\n" +
                "1970-01-01 |\n" +
                "2017-07-27 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        // test query with predicate on date
        rs = methodWatcher.executeQuery("select * from t_date where c1='2017-07-27' order by 1");

        expected = "C1     |\n" +
                "------------\n" +
                "2017-07-27 |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        rs = methodWatcher.executeQuery("select * from t_date where c1>='2017-07-27' order by 1");

        expected = "C1     |\n" +
                "------------\n" +
                "2017-07-27 |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        rs = methodWatcher.executeQuery("select * from t_date where c1>=date('2017-07-27') order by 1");

        expected = "C1     |\n" +
                "------------\n" +
                "2017-07-27 |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testPartitionByDateOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionByDateOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_date (a1 date, b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.executeUpdate("insert into t_partition_by_date values('2017-7-27', 1, 'AAA'), ('1970-01-01', 2, 'BBB'), ('2017-7-27', 3, 'CCC')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_date group by a1 order by 1");

        String expected = "A1     | 2 |\n" +
                "----------------\n" +
                "1970-01-01 | 1 |\n" +
                "2017-07-27 | 2 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        // test query with predicate on date
        rs = methodWatcher.executeQuery("select * from t_partition_by_date where a1='2017-07-27' order by 1,2");

        expected = "A1     |B1 |C1  |\n" +
                "---------------------\n" +
                "2017-07-27 | 1 |AAA |\n" +
                "2017-07-27 | 3 |CCC |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        rs = methodWatcher.executeQuery("select * from t_partition_by_date where a1>='2017-07-27' order by 1,2");

        expected = "A1     |B1 |C1  |\n" +
                "---------------------\n" +
                "2017-07-27 | 1 |AAA |\n" +
                "2017-07-27 | 3 |CCC |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        //test query with null date

        methodWatcher.executeUpdate("insert into t_partition_by_date values(NULL, 1, 'DDD')");
        rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_date group by a1 order by 1");

        expected = "A1     | 2 |\n" +
                "----------------\n" +
                "1970-01-01 | 1 |\n" +
                "2017-07-27 | 2 |\n" +
                "   NULL    | 1 |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testPartitionByCharOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionByCharOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_char (a1 CHAR(10), b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.executeUpdate("insert into t_partition_by_char values(NULL, 1, 'DDD')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_char group by a1 order by 1");

        String expected = "A1  | 2 |\n" +
                "----------\n" +
                "NULL | 1 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testPartitionByNumericOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionByNumericOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_numeric (a1 NUMERIC, b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.executeUpdate("insert into t_partition_by_numeric values(NULL, 1, 'DDD')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_numeric group by a1 order by 1");

        String expected = "A1  | 2 |\n" +
                "----------\n" +
                "NULL | 1 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testPartitionByBoolOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionByBoolOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_bool (a1 Boolean, b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.executeUpdate("insert into t_partition_by_bool values(NULL, 1, 'DDD')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_bool group by a1 order by 1");

        String expected = "A1  | 2 |\n" +
                "----------\n" +
                "NULL | 1 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testPartitionByDoubleOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionByDoubleOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_double (a1 double, b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));

        methodWatcher.executeUpdate("insert into t_partition_by_double values(NULL, 1, 'DDD')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_double group by a1 order by 1");

        String expected = "A1  | 2 |\n" +
                "----------\n" +
                "NULL | 1 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testReadIntFromFileAvro() throws Exception {

        methodWatcher.executeUpdate(String.format("create external table short_avro (col1 int)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"short_avro"));
        methodWatcher.executeUpdate("insert into short_avro values (12)");
        ResultSet rs = methodWatcher.executeQuery("select * from short_avro");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                " 12  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testModifyExtTableFails() throws Exception {
        for( String fileFormat : fileFormats) {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            String name = "ALTER_TABLE_" + fileFormat;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            assureFails("alter table " + name + " add column col3 int", "EXT12",
                    fileFormat + ": cannotAlterExternalTable");
            assureFails("create index add_index_foo_ix on " + name + " (col2)", "EXT13",
                    fileFormat + ": cannotAddIndexToExternalTable");

            verifyTriggerCreateFails(tb.on(name).named("trig").before().delete().row().then("select * from sys.systables"),
                    "Cannot add triggers to external table '" + name + "'.");
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void createTrigger(TriggerBuilder tb) throws Exception {
        methodWatcher.executeUpdate(tb.build());
    }

    private void verifyTriggerCreateFails(TriggerBuilder tb, String expectedError) throws Exception {
        try {
            createTrigger(tb);
            fail("expected trigger creation to fail for=" + tb.build());
        } catch (Exception e) {
            assertEquals(expectedError, e.getMessage());
        }

    }

    @Test
    public void testWriteToWrongPartitionedParquetExternalTable() throws Exception {
        for( String fileFormat : fileFormats) {
                String name = "w_partitioned_" + fileFormat;
                String tablePath = getExternalResourceDirectory() + name;
                methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24))" +
                        "partitioned by (col1) STORED AS " + fileFormat + " LOCATION '%s'", tablePath));
                methodWatcher.executeUpdate(String.format("insert into " + name + " values (1,'XXXX')," +
                        "(2,'YYYY')," +
                        "(3,'ZZZZ')"));
                assureFails(String.format("create external table " + name + "2 (col1 int, col2 varchar(24))" +
                        "partitioned by (col2) STORED AS " + fileFormat + " LOCATION '%s'", tablePath), "EXT24", "");
        }
    }

    @Test
    public void testWriteToNotPermittedLocation() throws Exception{
        for( String fileFormat : fileFormats) {
            String name = "NO_PERMISSION_" + fileFormat;
            String filename = getExternalResourceDirectory() + name;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 varchar(24), col3 boolean)" +
                    " STORED AS " + fileFormat + " LOCATION '%s'", filename));

            File file = new File(filename);

            // this should be fine
            methodWatcher.executeUpdate(String.format("insert into " + name + " values (1,'XXXX',true), " +
                    "(2,'YYYY',false), (3,'ZZZZ', true)"));
            try {
                file.setWritable(false);
                methodWatcher.executeUpdate(String.format("insert into " + name + " values (1,'XXXX',true), " +
                        "(2,'YYYY',false), (3,'ZZZZ', true)"));

                // we don't want to have a unwritable file in the folder, clean it up
                file.setWritable(true);
                file.delete();
                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                // we don't want to have a unwritable file in the folder, clean it up
                file.setWritable(true);
                file.delete();
                Assert.assertEquals("Wrong Exception", "SE010", e.getSQLState());
            }
        }
    }


    @Test @Ignore // failing on mapr5.2.0. Temporary ignoring
    public void testReadToNotPermittedLocation() throws Exception{


        methodWatcher.executeUpdate(String.format("create external table PARQUET_NO_PERMISSION_READ (col1 int, col2 varchar(24), col3 boolean)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"PARQUET_NO_PERMISSION_READ"));

        File file = new File(String.valueOf(getExternalResourceDirectory()+"PARQUET_NO_PERMISSION_READ"));

        try{

            methodWatcher.executeUpdate(String.format("insert into PARQUET_NO_PERMISSION_READ values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));
            file.setReadable(false);
            methodWatcher.executeQuery(String.format("select * from PARQUET_NO_PERMISSION_READ"));

            // we don't want to have a unreadable file in the folder, clean it up
            file.setReadable(true);
            file.delete();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            // we don't want to have a unreadable file in the folder, clean it up
            file.setReadable(true);
            file.delete();
            Assert.assertEquals("Wrong Exception","EXT11",e.getSQLState());
        }
    }

    @Test @Ignore // failing on mapr5.2.0. Temporary ignoring
    public void testReadToNotPermittedLocationAvro() throws Exception{


        methodWatcher.executeUpdate(String.format("create external table AVRO_NO_PERMISSION_READ (col1 int, col2 varchar(24), col3 boolean)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"AVRO_NO_PERMISSION_READ"));

        File file = new File(String.valueOf(getExternalResourceDirectory()+"AVRO_NO_PERMISSION_READ"));

        try{

            methodWatcher.executeUpdate(String.format("insert into AVRO_NO_PERMISSION_READ values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));
            file.setReadable(false);
            methodWatcher.executeQuery(String.format("select * from AVRO_NO_PERMISSION_READ"));

            // we don't want to have a unreadable file in the folder, clean it up
            file.setReadable(true);
            file.delete();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            // we don't want to have a unreadable file in the folder, clean it up
            file.setReadable(true);
            file.delete();
            Assert.assertEquals("Wrong Exception","EXT11",e.getSQLState());
        }
    }

    // rather slow test (20s). maybe combine with testWriteReadArrays?
    @Test
    public void testCollectStats() throws Exception {
        for( String fileFormat : new String[]{"ORC", "PARQUET", "AVRO", "TEXTFILE"}) {
            String name = "TEST_COLLECT_STATS_" + fileFormat;
            String filename = getExternalResourceDirectory() + name;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 char(24))" +
                    " STORED AS ORC LOCATION '%s'", filename));
            int insertCount = methodWatcher.executeUpdate(String.format("insert into " + name + " values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));
            Assert.assertEquals("insertCount is wrong",3,insertCount);
            ResultSet rs = methodWatcher.executeQuery("select * from " + name);
            Assert.assertEquals("COL1 |COL2 |\n" +
                    "------------\n" +
                    "  1  |XXXX |\n" +
                    "  2  |YYYY |\n" +
                    "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            // collect table level stats
            PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
            ps.setString(1, "EXTERNALTABLEIT");
            ps.setString(2, name);
            ps.setBoolean(3, true);
            rs = ps.executeQuery();
            rs.next();
            Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
            rs.close();

            ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where " +
                    "schemaname = 'EXTERNALTABLEIT' and tablename = '" + name + "'");
            String expected = "TOTAL_ROW_COUNT |\n" +
                    "------------------\n" +
                    "        3        |";
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
            rs2.close();

            // drop stats using table
            spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_TABLE_STATISTICS ('EXTERNALTABLEIT', '" + name + "')");

            // make sure it is clean
            rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where " +
                    "schemaname = 'EXTERNALTABLEIT' and tablename = '" + name + "' ");
            Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs2));
            rs2.close();

            // Now, collect schema level stats
            ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS(?,?) ");
            ps.setString(1, "EXTERNALTABLEIT");
            ps.setBoolean(2, false);
            rs = ps.executeQuery();
            rs.next();
            Assert.assertEquals("Error with COLLECT_SCHEMA_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
            rs.close();

            // check the stats again
            rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where " +
                    "schemaname = 'EXTERNALTABLEIT' and tablename = '" + name + "' ");
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
            rs2.close();

            // drop stats using schema
            spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_SCHEMA_STATISTICS ('EXTERNALTABLEIT')");

            // make sure it is clean
            rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
            Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs2));
            rs2.close();
        }
    }

    // rather slow test (20s)
    @Test
    public void testWriteReadArrays() throws Exception {
        for( String fileFormat : fileFormats) {
            String name = "TEST_ARRAY_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + name;
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int array, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'", tablePath));
            int insertCount = methodWatcher.executeUpdate(String.format("insert into " + name + " values ([1,1,1],'XXXX')," +
                    "([2,2,2],'YYYY')," +
                    "([3,3,3],'ZZZZ')"));
            Assert.assertEquals("insertCount is wrong", 3, insertCount);

            // execute the following once without and once with analyze table
            for( int i=0; i<2; i++) {
                if( i == 1 )
                    methodWatcher.executeQuery("analyze table " + name);

                ResultSet rs = methodWatcher.executeQuery("select * from " + name);
                Assert.assertEquals("COL1    |COL2 |\n" +
                        "-----------------\n" +
                        "[1, 1, 1] |XXXX |\n" +
                        "[2, 2, 2] |YYYY |\n" +
                        "[3, 3, 3] |ZZZZ |", TestUtils.FormattedResult.ResultFactory.toString(rs));
                ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from " + name);
                Assert.assertEquals("COL1    |\n" +
                        "-----------\n" +
                        "[1, 1, 1] |\n" +
                        "[2, 2, 2] |\n" +
                        "[3, 3, 3] |", TestUtils.FormattedResult.ResultFactory.toString(rs2));

                //Make sure empty file is created
                Assert.assertTrue(String.format("Table %s hasn't been created", tablePath), new File(tablePath).exists());
            }
        }
    }

    private String concatAllCsvFiles(File path) throws Exception {
        FileFilter fileFilter = new WildcardFileFilter("*.csv");
        File[] files = path.listFiles(fileFilter);
        if( files == null )
            return "<FILE NOT FOUND>";

        StringBuilder sb = new StringBuilder();
        for ( File file : files ) {
            FileInputStream stream = new FileInputStream( file );
            sb.append( IOUtils.toString(stream, "UTF-8") );
        }
        return sb.toString();
    }

    @Test
    public void testCsvOptions() throws Exception {

        String tablePath = getExternalResourceDirectory() + "test_csv_options";
        // see https://splicemachine.atlassian.net/browse/DB-10339
        String csvOptions[] = {
                // default
                "",
                "\"\\\"Hallo; #\\\"World\\\"!\\\"\",\";Ha,\"\n",
                // TERMINATED BY
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ';'",
                "\"\\\"Hallo; #\\\"World\\\"!\\\"\";\";Ha,\"\n",
                // ESCAPED BY
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '#'",
                "\"#\"Hallo; ###\"World#\"!#\"\",\";Ha,\"\n",
        };
        for( int i = 0; i < csvOptions.length; i+=2 ) {
            // Create an external table stored as text
            methodWatcher.executeUpdate( "CREATE EXTERNAL TABLE TEST_CSV_OPTIONS (t1 varchar(30), t2 varchar(30)) \n" +
                    csvOptions[i] + " STORED AS TEXTFILE\n" +
                    "location '" + tablePath + "'");
            Assert.assertEquals( methodWatcher.executeUpdate(
                    "insert into TEST_CSV_OPTIONS values ('\"Hallo; #\"World\"!\"', ';Ha,')"), 1);


            ResultSet rs = methodWatcher.executeQuery("select * from TEST_CSV_OPTIONS");
            Assert.assertEquals(csvOptions[i],"T1         | T2  |\n" +
                    "--------------------------\n" +
                    "\"Hallo; #\"World\"!\" |;Ha, |",TestUtils.FormattedResult.ResultFactory.toString(rs));

            File path = new File(tablePath);
            Assert.assertEquals( csvOptions[i+1], concatAllCsvFiles(path) );
            methodWatcher.execute("drop table TEST_CSV_OPTIONS" );
            FileUtils.deleteDirectory( path );
        }
    }

    @Test
    public void testUsingExsitingCsvFile() throws Exception {

        // Create an external table stored as text
        methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE EXT_TEXT (id INT, c_text varchar(30)) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS TEXTFILE\n" +
                "location '%s'", getExternalResourceDirectory() + "testUsingExsitingCsvFile"));

        // insert into the table
        methodWatcher.execute("insert into EXT_TEXT values (1, 'text1'), (2, 'text2'), (3, 'text3'), (4, 'text4')");
        ResultSet rs = methodWatcher.executeQuery("select * from ext_text order by 1");
        String before = TestUtils.FormattedResult.ResultFactory.toString(rs);

        // drop and recreate another external table using previous data
        methodWatcher.execute("drop table ext_text");

        methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE EXT_TEXT2 (id INT, c_text varchar(30)) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS TEXTFILE\n" +
                "location '%s'", getExternalResourceDirectory() + "testUsingExsitingCsvFile"));

        rs = methodWatcher.executeQuery("select * from ext_text2 order by 1");
        String after = TestUtils.FormattedResult.ResultFactory.toString(rs);

        Assert.assertEquals(after, before, after);

    }

    @Test
    public void testBuildInFunctionText()  throws Exception {
        //
        String tablePath = getExternalResourceDirectory()+ "EXT_FUNCTION_TEXT";
        methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE EXT_FUNCTION_TEXT (id INT, c_vchar varchar(30), c_date DATE,  c_num NUMERIC, c_bool BOOLEAN) \n" +
                "ROW FORMAT DELIMITED \n" +
                "FIELDS TERMINATED BY ','\n" +
                "STORED AS TEXTFILE\n" +
                "location '%s'", tablePath));



        methodWatcher.execute("insert into EXT_FUNCTION_TEXT (id, c_vchar, c_date, c_num, c_bool) values (1, 'nR-trkDr#,`9DSUbCw C+U8QctPUBy', '7958-05-18', 13691,  true)," +
                "(2, '$c\">0n`w6b-$O7F`Q6#QWNnivV=6v?', '3450-03-06', 35317, false)," +
                "(3, 'Q=-DoLR#Bd|(M/![FaN6q Jn>\"CEIW', '4736-03-12', 2877, true)," +
                "(4, 'eo}+Eyd~%MwIbheQ>aHz;h~Wb{T%5y', '2871-11-07', 71800, true), " +
                "(5, '@SEulog}9|{]46m~cYDYspt%Z4tZ_4', '2833-03-03', 67859, false)");

        ResultSet rs = methodWatcher.executeQuery("select  DAY(c_date) from EXT_FUNCTION_TEXT  order by 1");
        Assert.assertEquals("1 |\n" +
                "----\n" +
                "12 |\n" +
                "18 |\n" +
                " 3 |\n" +
                " 6 |\n" +
                " 7 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testTinyIntOrcReader() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/tinyIntOrc";
        methodWatcher.executeUpdate(String.format("create external table t (c1 tinyint, c2 int array) " +
                "STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeUpdate("insert into t values(1, [1,1,1])");
        ResultSet rs = methodWatcher.executeQuery("select * from t where c1=1");
        rs.next();
        int i = rs.getInt(1);
        Assert.assertTrue(i == 1);
    }

    @Test
    public void testSmallIntOrcReader() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/ShortIntOrc";
        methodWatcher.executeUpdate(String.format("create external table t_small (c1 smallint, c2 int array) " +
                "STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeUpdate("insert into t_small values(1, [1,1,1])");
        ResultSet rs = methodWatcher.executeQuery("select * from t_small where c1=1");
        rs.next();
        int i = rs.getInt(1);
        Assert.assertTrue(i == 1);
    }

    @Test
    public void testPartitionBySmallIntOrcTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartitionBySmallIntOrc";
        methodWatcher.executeUpdate(String.format("create external table t_partition_by_smallint (a1 smallint, b1 int, c1 varchar(10))" +
                "partitioned by (a1)" +
                "STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeUpdate("insert into t_partition_by_smallint values(1, 1, 'AAA'), (2, 2, 'BBB'), (1, 3, 'CCC')");
        ResultSet rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_smallint group by a1 order by 1");

        String expected = "A1 | 2 |\n" +
                "--------\n" +
                " 1 | 2 |\n" +
                " 2 | 1 |";

        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        // test query with predicate on date
        rs = methodWatcher.executeQuery("select * from t_partition_by_smallint where a1=1 order by 1, 2");

        expected = "A1 |B1 |C1  |\n" +
                "-------------\n" +
                " 1 | 1 |AAA |\n" +
                " 1 | 3 |CCC |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        rs = methodWatcher.executeQuery("select * from t_partition_by_smallint where a1>=2 order by 1,2");

        expected = "A1 |B1 |C1  |\n" +
                "-------------\n" +
                " 2 | 2 |BBB |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();

        methodWatcher.executeUpdate("insert into t_partition_by_smallint values(NULL, 1, 'DDD')");
        rs = methodWatcher.executeQuery("select a1, count(*) from t_partition_by_smallint group by a1 order by 1");

        expected = "A1  | 2 |\n" +
                "----------\n" +
                "  1  | 2 |\n" +
                "  2  | 1 |\n" +
                "NULL | 1 |";

        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(expected, resultString);
        rs.close();
    }

    @Test
    public void testQueryOnOrcTableWithInequalityPredicate() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/OrcInequalityPredicate";
        methodWatcher.executeUpdate(String.format("create external table orc_inequality (c1 smallint, c2 int, c3 char(10)) " +
                "STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA ('%s', 'ORC_INEQUALITY', null, '%s', ',', null, null, null, null, 0, null, true, null)",
                getSchemaName(), getResourceDirectory()+"orc_inequality_predicate.csv"));
        /* the constant value is within the (min,max) range of the referenced column */
        ResultSet rs = methodWatcher.executeQuery("select * from orc_inequality where c1>1 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c2>=200 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c1<3 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c3<='BBB' order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 2 |200 |BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* the constant value is below the (min,max) range of the referenced column */
        rs = methodWatcher.executeQuery("select * from orc_inequality where c2>0 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c2<=0 order by 1,2");
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* the constant value is above the (min,max) range of the referenced column */
        rs = methodWatcher.executeQuery("select * from orc_inequality where c3>= 'FFF' order by 1,2");
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c3 <= 'FFF' order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from orc_inequality where c2 <> 300 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testQueryOnPartitionedOrcTableWithInequalityPredicate() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/PartOrcInequalityPredicate";
        methodWatcher.executeUpdate(String.format("create external table part_orc_inequality (c1 smallint, c2 int, c3 char(10)) " +
                "partitioned by (c1) STORED AS ORC LOCATION '%s'",tablePath));
        methodWatcher.executeQuery(format("call SYSCS_UTIL.IMPORT_DATA ('%s', 'PART_ORC_INEQUALITY', null, '%s', ',', null, null, null, null, 0, null, true, null)",
                getSchemaName(), getResourceDirectory()+"orc_inequality_predicate.csv"));
        /* the constant value is within the (min,max) range of the referenced column */
        ResultSet rs = methodWatcher.executeQuery("select * from part_orc_inequality where c1>1 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c2>=200 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c1<3 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c3<='BBB' order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 2 |200 |BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* the constant value is below the (min,max) range of the referenced column */
        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c2>0 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c2<=0 order by 1,2");
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* the constant value is above the (min,max) range of the referenced column */
        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c3>= 'FFF' order by 1,2");
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c3 <= 'FFF' order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |300 |CCC |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 3 |300 |CCC |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("select * from part_orc_inequality where c2 <> 300 order by 1,2");
        Assert.assertEquals("C1 |C2  |C3  |\n" +
                "--------------\n" +
                " 1 |100 |AAA |\n" +
                " 1 |200 |BBB |\n" +
                " 1 |400 |DDD |\n" +
                " 2 |200 |BBB |\n" +
                " 4 |400 |DDD |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
    /**
     *
     *
     *
     Elapsedtime on;
     --select user_id, data_date, clicks
     select *
     from RFUEL.impressions
     where data_date between 20160915 and 20160925
     and ad_info[2] =‘80831’
     {limit 10};

     [centos@ip-10-250-0-12 rfuel]$ more /tmp/t4_b.sql
     Elapsedtime on;

     --select user_id, server_timestamp
     select *
     from rfuel.impressions
     where data_date = 20160915
     and clicks >0
     {limit 10};

     Elapsedtime on;
     select user_id, data_date, clicks
     from RFUEL.impressions
     where data_date between 20160915 and 20160925
     and ad_info[2] =‘80831’
     {limit 10};

     select

     [centos@ip-10-250-0-12 rfuel]$ more t4.sql
     Elapsedtime on;
     select user_id, server_timestamp
     from rfuel.impressions
     where data_date = 20160915
     and clicks >0
     {limit 10};


     user_id, server_timestamp, data_date, clicks, ad_info[2]

     user_id BIGINT,
     server_timestamp BIGINT,
     clicks INT,
     user_agent                            VARCHAR(1000),
     user_accept_language                  VARCHAR(1000),
     url                                   VARCHAR(1000),
     referrer_url                          VARCHAR(1000),
     page_id                               VARCHAR(1000),
     page_category_csv                     VARCHAR(1000),
     page_tags_csv                         VARCHAR(1000),
     ad_info                               VARCHAR(1000) ARRAY,
     data_date                             INTEGER)
     ,
     PARTITIONED BY (
     data_date
     )



     */
    @Test
    public void testPartitionPruning() throws Exception {
        String tablePath =  getExternalResourceDirectory()+"partition_prune_orc";
        methodWatcher.executeUpdate(String.format("create external table partition_prune_orc ( user_id BIGINT,\n" +
                "     server_timestamp BIGINT,\n" +
                "     clicks INT,\n" +
                "     user_agent                            VARCHAR(1000),\n" +
                "     user_accept_language                  VARCHAR(1000),\n" +
                "     url                                   VARCHAR(1000),\n" +
                "     referrer_url                          VARCHAR(1000),\n" +
                "     page_id                               VARCHAR(1000),\n" +
                "     page_category_csv                     VARCHAR(1000),\n" +
                "     page_tags_csv                         VARCHAR(1000),\n" +
                "     ad_info                               VARCHAR(1000) ARRAY,\n" +
                "     data_date                             INTEGER)"+
                "partitioned by (data_date) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"partition_prune_orc"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into partition_prune_orc values "+
                "(100,100, 100, 'x','x','x','x','x','x','x',['100','100'], 20160915)," +
                "(200,200, 200, 'x','x','x','x','x','x','x',['100','100'], 20160916)," +
                "(300,300, 300, 'x','x','x','x','x','x','x',['100','100'], 20160917)"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from partition_prune_orc");
        Assert.assertEquals("USER_ID |SERVER_TIMESTAMP |CLICKS |USER_AGENT |USER_ACCEPT_LANGUAGE | URL |REFERRER_URL | PAGE_ID | PAGE_CATEGORY_CSV | PAGE_TAGS_CSV |  AD_INFO  | DATA_DATE |\n" +
                "----------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "   100   |       100       |  100  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160915  |\n" +
                "   200   |       200       |  200  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160916  |\n" +
                "   300   |       300       |  300  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160917  |",TestUtils.FormattedResult.ResultFactory.toString(rs));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

        // collect stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "PARTITION_PRUNE_ORC");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'PARTITION_PRUNE_ORC'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        // make sure query runs ok after collect stats
        ResultSet rs3 = methodWatcher.executeQuery("select * from partition_prune_orc");
        Assert.assertEquals("USER_ID |SERVER_TIMESTAMP |CLICKS |USER_AGENT |USER_ACCEPT_LANGUAGE | URL |REFERRER_URL | PAGE_ID | PAGE_CATEGORY_CSV | PAGE_TAGS_CSV |  AD_INFO  | DATA_DATE |\n" +
                "----------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                "   100   |       100       |  100  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160915  |\n" +
                "   200   |       200       |  200  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160916  |\n" +
                "   300   |       300       |  300  |     x     |          x          |  x  |      x      |    x    |         x         |       x       |[100, 100] | 20160917  |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        rs3.close();


    }


    @Test
    public void testFaultyCompressionAvro() throws Exception {
        assureFails(String.format("CREATE EXTERNAL TABLE bad_compression_avro (col1 int) " +
                        "COMPRESSED WITH TURD STORED AS AVRO LOCATION '%s'",
                getExternalResourceDirectory()+"/compression"), "42X01", "");
    }

    // tests for avro support of date type:

    @Test
    public void testWriteReadDateAvroExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"simple_avro_date";
        methodWatcher.executeUpdate(String.format("create external table simple_avro_date (col1 date)" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_avro_date values ('2000-01-01')," +
                "('2000-02-02')," +
                "('2000-03-03')," +
                "(null)"));
        Assert.assertEquals("insertCount is wrong",4,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_avro_date");
        Assert.assertEquals("COL1    |\n" +
                "------------\n" +
                "2000-01-01 |\n" +
                "2000-02-02 |\n" +
                "2000-03-03 |\n" +
                "   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from simple_avro_date");
        Assert.assertEquals("COL1    |\n" +
                "------------\n" +
                "2000-01-01 |\n" +
                "2000-02-02 |\n" +
                "2000-03-03 |\n" +
                "   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

        methodWatcher.executeUpdate(String.format("create external table simple_avro_date_copy (col1 date) stored as avro " +
                "location '%s'",tablePath));
        ResultSet rs3 = methodWatcher.executeQuery("select * from simple_avro_date_copy");
        Assert.assertEquals("COL1    |\n" +
                "------------\n" +
                "2000-01-01 |\n" +
                "2000-02-02 |\n" +
                "2000-03-03 |\n" +
                "   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
    }


    @Test
    public void testPartitionedAvroDateTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"partitionAvroDate";
        methodWatcher.executeUpdate(String.format("create external table partitionAvroDate (col1 date, col2 date) partitioned by (col2) stored as avro " +
                "location '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into partitionAvroDate values ('1999-01-01','1999-01-01')," +
                "(null,null)," +
                "('2000-02-02','2000-02-02')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from partitionAvroDate");
        Assert.assertEquals("COL1    |   COL2    |\n" +
                "------------------------\n" +
                "1999-01-01 |1999-01-01 |\n" +
                "2000-02-02 |2000-02-02 |\n" +
                "   NULL    |   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        methodWatcher.executeUpdate(String.format("create external table partitionAvroDateCopy (col1 date, col2 date) partitioned by (col2) stored as avro " +
                "location '%s'",tablePath));
        ResultSet rs2 = methodWatcher.executeQuery("select * from partitionAvroDateCopy");
        Assert.assertEquals("COL1    |   COL2    |\n" +
                "------------------------\n" +
                "1999-01-01 |1999-01-01 |\n" +
                "2000-02-02 |2000-02-02 |\n" +
                "   NULL    |   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
    }


    @Test
    public void testCollectAvroDateStats() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table avro_date_stats (col1 date)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"avro_date_stats"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into avro_date_stats values ('2000-01-01')," +
                "('2000-02-02')," +
                "('2000-03-03')," +
                "(null)"));
        Assert.assertEquals("insertCount is wrong",4,insertCount);

        ResultSet rs;
        // collect table level stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "AVRO_DATE_STATS");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'AVRO_DATE_STATS'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        4        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        ResultSet rs3 = methodWatcher.executeQuery("select * from avro_date_stats");
        Assert.assertEquals("COL1    |\n" +
                "------------\n" +
                "2000-01-01 |\n" +
                "2000-02-02 |\n" +
                "2000-03-03 |\n" +
                "   NULL    |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        rs3.close();
    }

    @Test
    public void testBroadcastJoinOrcTablesOnSpark() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table l (col1 int)" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"left"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into l values 1, 2, 3" ));
        methodWatcher.executeUpdate(String.format("create external table r (col1 int)" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"right"));
        int insertCount2 = methodWatcher.executeUpdate(String.format("insert into r values 1,2,3"));

        Assert.assertEquals("insertCount is wrong",3,insertCount);
        Assert.assertEquals("insertCount is wrong",3,insertCount2);

        ResultSet rs = methodWatcher.executeQuery("select * from \n" +
                "l --splice-properties useSpark=true\n" +
                ", r --splice-properties joinStrategy=broadcast\n" +
                "where l.col1=r.col1");
        Assert.assertEquals("COL1 |COL1 |\n" +
                "------------\n" +
                "  1  |  1  |\n" +
                "  2  |  2  |\n" +
                "  3  |  3  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Ignore
    @Test
    public void testParquetMergeSchema() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table parquet_schema_evolution (c1 int, c2 varchar(2), c3 double)" +
                    " STORED AS PARQUET LOCATION '%s'", getResourceDirectory() + "parquet_schema_evolution"));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT23",e.getSQLState());
        }

        methodWatcher.executeUpdate(String.format("create external table parquet_schema_evolution (c1 int, c2 varchar(2), c3 double)" +
                " STORED AS PARQUET LOCATION '%s' merge schema", getResourceDirectory() + "parquet_schema_evolution"));


        ResultSet rs = methodWatcher.executeQuery("select * from parquet_schema_evolution order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "C1 |C2 |  C3   |\n" +
                "----------------\n" +
                " 1 |CA | NULL  |\n" +
                " 2 |NY | NULL  |\n" +
                " 3 |TX | NULL  |\n" +
                " 4 |NJ |100.01 |";
        Assert.assertEquals(actual, expected, actual);

        methodWatcher.execute("insert into parquet_schema_evolution values (5, 'PA', 200)");
        rs = methodWatcher.executeQuery("select * from parquet_schema_evolution order by 1");
        actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        expected =
                "C1 |C2 |  C3   |\n" +
                "----------------\n" +
                " 1 |CA | NULL  |\n" +
                " 2 |NY | NULL  |\n" +
                " 3 |TX | NULL  |\n" +
                " 4 |NJ |100.01 |\n" +
                " 5 |PA | 200.0 |";
        Assert.assertEquals(actual, expected, actual);
    }


    @Test
    public void testReadWriteAvroFromHive() throws Exception {
        String path = getTempCopyOfResourceDirectory(tempDir, "t_avro" );
        methodWatcher.execute(String.format("create external table t_avro (col1 varchar(30), col2 int)" +
                " STORED AS AVRO LOCATION '%s'", path ));

        methodWatcher.execute("insert into t_avro values ('John', 18)");
        ResultSet rs = methodWatcher.executeQuery("select * from t_avro order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "COL1  |COL2 |\n" +
                "-------------\n" +
                "John  | 18  |\n" +
                " Sam  | 22  |\n" +
                "Scott | 25  |\n" +
                " Tom  | 20  |";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testNumericType() throws Exception {
        String path = getExternalResourceDirectory() + "t_num"; // = "empty" table
        methodWatcher.execute(String.format("create external table t_num (col1 NUMERIC(23,2), col2 bigint)" +
                " STORED AS PARQUET LOCATION '%s'", path));
        methodWatcher.execute("insert into t_num values (100.23456, 1)");
        ResultSet rs = methodWatcher.executeQuery("select * from t_num order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "COL1  |COL2 |\n" +
                "--------------\n" +
                "100.23 |  1  |";

        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testNotExistAndEmptyDirectory() throws Exception {
        for( String fileFormat : new String[]{"PARQUET", "ORC",  "AVRO", "TEXTFILE"})
        {
            String name = "empty_directory_not_exist_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + name;
            File path =  new File(tablePath);
            path.mkdir();
            methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                    " STORED AS " + fileFormat + " LOCATION '%s'",tablePath));
            FileUtils.deleteDirectory(path);
            assureQueryFails("select * from " + name, "EXT11", fileFormat + ": testNotExistDir");

            path.mkdir();
            ResultSet rs = methodWatcher.executeQuery("select count(*) from " + name);
            String expected = "1 |\n----\n 0 |";
            String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            assertEquals(expected, resultString);
            rs.close();
        }
    }

    @Test
    public void testParquetColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory()+"parquet_colname";
        methodWatcher.execute(String.format("create external table t_parquet (col1 int, col2 varchar(5))" +
                " STORED AS PARQUET LOCATION '%s'", tablePath));
        methodWatcher.execute("insert into t_parquet values (1, 'A')");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();

        Dataset dataset = spark
                .read()
                .parquet(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);
    }


    @Test
    public void testParquetPartitionColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory()+"parquet_partition_colname";
        methodWatcher.execute(String.format("create external table t_parquet_partition (col1 int, col2 varchar(5)) partitioned by (col2)" +
                " STORED AS PARQUET LOCATION '%s'", tablePath));
        methodWatcher.execute("insert into t_parquet_partition values (2, 'B')");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();
        Dataset dataset = spark
                .read()
                .parquet(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testOrcColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory()+"orc_colname";
        methodWatcher.execute(String.format("create external table t_orc (col1 int, col2 varchar(5))" +
                " STORED AS ORC LOCATION '%s'", tablePath));
        methodWatcher.execute("insert into t_orc values (3, 'C')");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();
        Dataset dataset = spark
                .read()
                .orc(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testOrcPartitionColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory()+"orc_partition_colname";
        methodWatcher.execute(String.format("create external table t_orc_partition (col1 int, col2 varchar(5)) partitioned by (col2)" +
                " STORED AS ORC LOCATION '%s'", tablePath));
        methodWatcher.execute("insert into t_orc_partition values (4, 'D')");
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();
        Dataset dataset = spark
                .read()
                .orc(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test @Ignore("Need to import spark-avro.jar in hbase_sql to test in a separate Spark")
    public void testAvroColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory() + "avro_colname";
        methodWatcher.executeUpdate(String.format("create external table avro_colname (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'", tablePath));
        methodWatcher.executeUpdate(String.format("insert into avro_colname values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();
        Dataset dataset = spark
                .read()
                .format("com.databricks.spark.avro")
                .load(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);

    }
    @Test @Ignore("Need to import spark-avro.jar in hbase_sql to test in a separate Spark")
    public void testAvroPartitionColumnName() throws Exception {
        String tablePath = getExternalResourceDirectory() + "avro_partition_colname";
        methodWatcher.executeUpdate(String.format("create external table avro_patition_colname (col1 int, col2 varchar(24))" +
                "partitioned by (col2) STORED AS AVRO LOCATION '%s'", tablePath));
        methodWatcher.executeUpdate(String.format("insert into avro_patition_colname values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("ExternaltableIT")
                .getOrCreate();
        Dataset dataset = spark
                .read()
                .format("com.databricks.spark.avro")
                .load(tablePath);
        String actual = dataset.schema().toString();
        String expected = "StructType(StructField(COL1,IntegerType,true), StructField(COL2,StringType,true))";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testAnalyzeOrcExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"orc_ext";
        methodWatcher.execute(String.format("create external table orc_ext (col1 decimal(5,2), col2 varchar(5))" +
                " STORED AS ORC LOCATION '%s'", tablePath));
        methodWatcher.execute("insert into orc_ext values (0.11, 'C')");
        ResultSet rs = methodWatcher.executeQuery("analyze table orc_ext");
        int count = 0;
        while (rs.next()) {
            ++count;
            String colName = rs.getString(2);
            Assert.assertTrue(colName.compareToIgnoreCase("orc_ext") == 0);
        }
        Assert.assertEquals(1, count);
    }

    @Test
    public void testShowCreateTableTextExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"show_text";
        methodWatcher.execute(String.format("CREATE EXTERNAL TABLE myTextTable\n" +
                "                    (col1 INT, col2 VARCHAR(24))\n" +
                "                    PARTITIONED BY (col1)\n" +
                "                    ROW FORMAT DELIMITED FIELDS\n" +
                "                    TERMINATED BY ','\n" +
                "                    ESCAPED BY '\\\\' \n" +
                "                    LINES TERMINATED BY '\\\\n'\n" +
                "                    STORED AS TEXTFILE\n" +
                "                    LOCATION '%s'", tablePath));
        ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','MYTEXTTABLE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"MYTEXTTABLE\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "PARTITIONED BY (\"COL1\")\n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' LINES TERMINATED BY '\\\\n'\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '" + tablePath + "';", rs.getString(1));

    }

    @Test
    public void testShowCreateTableExternalTable() throws Exception {
        for( String fileFormat : new String[]{"ORC", "PARQUET", "AVRO", "TEXTFILE"}) {
            String name = "TEST_SHOW_TABLE_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + name;

            methodWatcher.execute(String.format("CREATE EXTERNAL TABLE " + name + "\n" +
                    "                    (col1 INT, col2 VARCHAR(24))\n" +
                    "                    PARTITIONED BY (col1)\n" +
                    "                    STORED AS " + fileFormat + "\n" +
                    "                    LOCATION '%s'", tablePath));
            ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','" + name + "')");
            rs.next();
            Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"" + name + "\" (\n" +
                    "\"COL1\" INTEGER\n" +
                    ",\"COL2\" VARCHAR(24)\n" +
                    ") \n" +
                    "PARTITIONED BY (\"COL1\")\n" +
                    "STORED AS " + fileFormat  + "\n" +
                    "LOCATION '" + tablePath + "';", rs.getString(1));
        }

    }

    @Test
    public void testShowCreateTableSnappyParquetExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"show_snappyparquet";
        methodWatcher.execute(String.format("CREATE EXTERNAL TABLE mySnappyParquetTable\n" +
                "                    (col1 INT, col2 VARCHAR(24), col3 INT)\n" +
                "                    COMPRESSED WITH snappy\n" +
                "                    PARTITIONED BY (col2,col3)\n" +
                "                    STORED AS PARQUET\n" +
                "                    LOCATION '%s'", tablePath));
        ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','MYSNAPPYPARQUETTABLE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"MYSNAPPYPARQUETTABLE\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ",\"COL3\" INTEGER\n" +
                ") \n" +
                "COMPRESSED WITH snappy\n" +
                "PARTITIONED BY (\"COL2\",\"COL3\")\n" +
                "STORED AS PARQUET\n" +
                "LOCATION '" + tablePath + "';", rs.getString(1));

    }

    @Test
    public void testConcurrentRead() throws Exception {
        String tablePath = getExternalResourceDirectory()+"concurrent_test";
        methodWatcher.execute(String.format("create external table concurrent_test ( a int)" +
                " stored as parquet location '%s'", tablePath));
        methodWatcher.execute("insert into concurrent_test values 1,2,3,4");
        int n = 100;
        ExtThread[] threads = new ExtThread[n];
        for (int i = 0; i < n; ++i) {
            threads[i] = new ExtThread(methodWatcher.createConnection());
            threads[i].start();
        }

        for (int i = 0; i < n; ++i) {
            threads[i].join();
        }

        for (int i = 0; i < n; ++i) {
            Assert.assertEquals(true, threads[i].success);
        }
    }

    @Test
    public void testCompactionIgnoresExternalTables() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA('%s')",SCHEMA_NAME));
        } catch (SQLException | StandardException e) {
            throw new RuntimeException(e);
        }
    }

    public static class ExtThread extends Thread{
        private TestConnection conn;
        public volatile boolean success = false;
        public ExtThread(TestConnection conn) {
            this.conn = conn;
        }
        @Override
        public void run() {

            try (Statement s = conn.createStatement()) {
                s.execute("select count(*) from concurrent_test");
                success = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
