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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TriggerBuilder;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 * IT's for external table functionality
 *
 */
public class ExternalTableIT extends SpliceUnitTest{

    private static final String SCHEMA_NAME = ExternalTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private TriggerBuilder tb = new TriggerBuilder();
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void cleanoutDirectory() {
        try {
            File file = new File(getExternalResourceDirectory());
            if (file.exists())
                FileUtils.deleteDirectory(new File(getExternalResourceDirectory()));
            file.mkdir();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    @Test
    public void testInvalidSyntaxParquet() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/foobar/foobar";
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                    "row format delimited fields terminated by ',' escaped by '\\' " +
                    "lines terminated by '\\n' STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT01",e.getSQLState());
        }
    }

    @Test
    public void testInvalidSyntaxAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/foobar/foobar";
            // Row Format not supported for Avro
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                    "row format delimited fields terminated by ',' escaped by '\\' " +
                    "lines terminated by '\\n' STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT36",e.getSQLState());
        }
    }

    @Test
    public void testInvalidSyntaxORC() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/foobar/foobar";
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int) partitioned by (col1) " +
                    "row format delimited fields terminated by ',' escaped by '\\' " +
                    "lines terminated by '\\n' STORED AS ORC LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT02",e.getSQLState());
        }
    }


    @Test
    public void testStoredAsRequired() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/foobar/foobar";
            // Location Required For Parquet
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int) LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT03",e.getSQLState());
        }
    }

    @Test
    public void testLocationRequired() throws Exception {
        try {
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) STORED AS PARQUET");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT04",e.getSQLState());
        }
    }

    @Test
    public void testLocationRequiredAvro() throws Exception {
        try {
            methodWatcher.executeUpdate("create external table foo (col1 int, col2 int) STORED AS AVRO");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT04",e.getSQLState());
        }
    }

    @Test
    public void testCannotUsePartitionUndefined() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external  table table_without_defined_partition (col1 int, col2 varchar(24))" +
                    " PARTITIONED BY (col3) STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT21",e.getSQLState());
        }
    }

    @Test
    public void testCannotUsePartitionUndefinedAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external  table table_without_defined_partition (col1 int, col2 varchar(24))" +
                    " PARTITIONED BY (col3) STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT21",e.getSQLState());
        }
    }

    @Test
    public void testNoPrimaryKeysOnExternalTables() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int, primary key (col1)) STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT06",e.getSQLState());
        }
    }

    @Test
    public void testNoPrimaryKeysOnExternalTablesAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int, primary key (col1)) STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT06",e.getSQLState());
        }
    }

    @Test
    public void testNoCheckConstraintsOnExternalTables() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000)) STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT07",e.getSQLState());
        }
    }

    @Test
    public void testNoCheckConstraintsOnExternalTablesAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int, SALARY DECIMAL(9,2) CONSTRAINT SAL_CK CHECK (SALARY >= 10000)) STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT07",e.getSQLState());
        }
    }

    @Test
    public void testNoReferenceConstraintsOnExternalTables() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate("create table Cities (col1 int, col2 int, primary key (col1))");
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int, CITY_ID INT CONSTRAINT city_foreign_key\n" +
                    " REFERENCES Cities) STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT08",e.getSQLState());
        }
    }

    @Test
    public void testNoReferenceConstraintsOnExternalTablesAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO_TEST";
            methodWatcher.executeUpdate("create table Cities_avro (col1 int, col2 int, primary key (col1))");
            methodWatcher.executeUpdate(String.format("create external table foo_avro (col1 int, col2 int, CITY_ID INT CONSTRAINT city_foreign_key\n" +
                    " REFERENCES Cities_avro) STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT08",e.getSQLState());
        }
    }

    @Test
    public void testNoUniqueConstraintsOnExternalTables() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int unique)" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT09",e.getSQLState());
        }
    }

    @Test
    public void testNoUniqueConstraintsOnExternalTablesAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 int unique)" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT09",e.getSQLState());
        }
    }

    @Test
    public void testNoGenerationClausesOnExternalTables() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 varchar(24), col3 GENERATED ALWAYS AS ( UPPER(col2) ))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT10",e.getSQLState());
        }
    }

    @Test
    public void testNoGenerationClausesOnExternalTablesAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table foo (col1 int, col2 varchar(24), col3 GENERATED ALWAYS AS ( UPPER(col2) ))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT10",e.getSQLState());
        }
    }

    @Test
    public void testMissingExternal() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create table foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT18",e.getSQLState());
        }
    }

    @Test
    public void testMissingExternalAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create table foo (col1 int, col2 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT18",e.getSQLState());
        }
    }


    @Test
    public void testCannotUpdateExternalTable() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table update_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("update update_foo set col1 = 4");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testCannotUpdateExternalTableAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table update_foo_avro (col1 int, col2 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("update update_foo_avro set col1 = 4");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testCannotDeleteExternalTable() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table delete_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("delete from delete_foo where col1 = 4");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testCannotDeleteExternalTableAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table delete_foo_avro (col1 int, col2 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("delete from delete_foo_avro where col1 = 4");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT05",e.getSQLState());
        }
    }

    @Test
    public void testFileExistingNotDeleted() throws  Exception{
        String tablePath = getResourceDirectory()+"parquet_sample_one";

        File newFile = new File(tablePath);
        String[] files = newFile.list();
        int count = files.length;

        methodWatcher.executeUpdate(String.format("create external table table_to_existing_file (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        files = newFile.list();
        int count2 = files.length;
        Assert.assertEquals(String.format("File : %s have been modified and it shouldn't",tablePath),count, count2);

        ResultSet rs = methodWatcher.executeQuery("select * from table_to_existing_file order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "COL1 |COL2 |COL3 |\n" +
                        "------------------\n" +
                        " AAA | AAA | AAA |\n" +
                        " BBB | BBB | BBB |\n" +
                        " CCC | CCC | CCC |";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void createParquetTableMergeSchema() throws  Exception{
        String tablePath = getResourceDirectory()+"parquet_sample_one";

        methodWatcher.executeUpdate(String.format("create external table parquet_table_to_existing_file (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s' merge schema",tablePath));

        ResultSet rs = methodWatcher.executeQuery("select * from parquet_table_to_existing_file order by 1");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected =
                "COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                " AAA | AAA | AAA |\n" +
                " BBB | BBB | BBB |\n" +
                " CCC | CCC | CCC |";
        Assert.assertEquals(actual, expected, actual);
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
        int count = files.length;

        methodWatcher.executeUpdate(String.format("create external table table_to_existing_file_avro (col1 int, col2 int, col3 int)" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        files = newFile.list();
        int count2 = files.length;
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

    @Test
    public void testEmptyDirectory() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"empty_directory";
        new File(tablePath).mkdir();

        methodWatcher.executeUpdate(String.format("create external table empty_directory (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
    }

    @Test
    public void testEmptyDirectoryAvro() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"empty_directory_avro";
        new File(tablePath).mkdir();

        methodWatcher.executeUpdate(String.format("create external table empty_directory_avro (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
    }

    @Test
    public void testLocationCannotBeAFileAvro() throws  Exception{
        File temp = File.createTempFile("temp-file-avro", ".tmp");
        try {
            methodWatcher.executeUpdate(String.format("create external table table_to_existing_file_avro_temp (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'", temp.getAbsolutePath()));
            Assert.fail("Exception not thrown");
        } catch (SQLException ee) {
            Assert.assertEquals("Wrong Exception","EXT33" ,ee.getSQLState());
        }
    }

    @Test
    public void testLocationCannotBeAFile() throws  Exception{
        File temp = File.createTempFile("temp-file", ".tmp");

        try {
            methodWatcher.executeUpdate(String.format("create external table table_to_existing_file (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",temp.getAbsolutePath()));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT33",e.getSQLState());
        }
    }

    @Test
    public void refreshRequireExternalTable() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"external_table_refresh";

        methodWatcher.executeUpdate(String.format("create external table external_table_refresh (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "EXTERNAL_TABLE_REFRESH");
        ps.execute();

    }

    @Test
    public void refreshRequireExternalTableAvro() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"external_table_refresh_avro";

        methodWatcher.executeUpdate(String.format("create external table external_table_refresh_avro (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "EXTERNAL_TABLE_REFRESH_AVRO");
        ps.execute();

    }

    @Test
    public void refreshRequireExternalTableNotFound() throws  Exception{

        try {
            PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) ");
            ps.setString(1, "EXTERNALTABLEIT");
            ps.setString(2, "NOT_EXIST");
            ResultSet rs = ps.executeQuery();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X05",e.getSQLState());
        }

    }

    //SPLICE-1387
    @Test
    public void refreshRequireExternalTableWrongParameters() throws  Exception{

        try {
            PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE('arg1','arg2','arg3') ");

            ResultSet rs = ps.executeQuery();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42Y03",e.getSQLState());
        }

    }


    @Test
    public void testWriteReadNullValues() throws Exception {

        String tablePath = getExternalResourceDirectory()+"null_test_location";
        methodWatcher.executeUpdate(String.format("create external table null_test (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into null_test values (1,null)," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from null_test where col2 is null");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |NULL |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select * from null_test where col2 is not null");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

    }

    @Test
    public void testWriteReadNullValuesAvro() throws Exception {

        String tablePath = getExternalResourceDirectory()+"null_test_location_avro";
        methodWatcher.executeUpdate(String.format("create external table null_test_avro (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into null_test_avro values (1,null)," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from null_test_avro where col2 is null");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |NULL |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select * from null_test_avro where col2 is not null");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

    }


    @Test
    public void testWriteReadFromSimpleParquetExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"simple_parquet";
        methodWatcher.executeUpdate(String.format("create external table simple_parquet (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_parquet values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_parquet");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from simple_parquet");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "  2  |\n" +
                "  3  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

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

    @Test
    public void testWriteReadFromSimpleAvroExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"simple_avro";
        methodWatcher.executeUpdate(String.format("create external table simple_avro (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_avro values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_avro");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from simple_avro");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "  2  |\n" +
                "  3  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

    }


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
    public void testWriteReadFromPartitionedParquetExternalTable() throws Exception {
        String tablePath =  getExternalResourceDirectory()+"partitioned_parquet";
                methodWatcher.executeUpdate(String.format("create external table partitioned_parquet (col1 int, col2 varchar(24))" +
                "partitioned by (col2) STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"partitioned_parquet"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into partitioned_parquet values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from partitioned_parquet");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }

    @Test
    public void testWriteReadFromPartitionedAvroExternalTable() throws Exception {
        String tablePath =  getExternalResourceDirectory()+"partitioned_avro";
        methodWatcher.executeUpdate(String.format("create external table partitioned_avro (col1 int, col2 varchar(24))" +
                "partitioned by (col2) STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"partitioned_avro"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into partitioned_avro values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from partitioned_avro");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }

    @Test
    public void testWriteReadFromSimpleORCExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table simple_orc (col1 int, col2 varchar(24), col3 NUMERIC)" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"simple_orc"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_orc values (1,'XXXX',13691)," +
                "(2,'YYYY',2345)," +
                "(3,'ZZZZ',12345)"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_orc");
        Assert.assertEquals("COL1 |COL2 |COL3  |\n" +
                "-------------------\n" +
                "  1  |XXXX |13691 |\n" +
                "  2  |YYYY |2345  |\n" +
                "  3  |ZZZZ |12345 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadFromPartitionedORCExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table partitioned_orc (col1 int, col2 varchar(24))" +
                "partitioned by (col2) STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"partitioned_orc"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into partitioned_orc values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from partitioned_orc");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet r2 = methodWatcher.executeQuery("select count(*) from partitioned_orc");
        Assert.assertEquals("1 |\n" +
                "----\n" +
                " 3 |",TestUtils.FormattedResult.ResultFactory.toString(r2));
    }

    @Test
    public void testWriteReadWithPreExistingParquet() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table parquet_simple_file_table (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from parquet_simple_file_table where col1='AAA'");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                " AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs));
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
        methodWatcher.executeUpdate(String.format("create external table parquet_simple_file_table_and_or (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from parquet_simple_file_table_and_or where col1='BBB' OR ( col1='AAA' AND col2='AAA')");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                " AAA |\n" +
                " BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
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

    @Test @Ignore
    public void testWriteReadFromCompressedParquetExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table compressed_parquet_test (col1 int, col2 varchar(24))" +
                " COMPRESSED WITH SNAPPY STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"compressed_parquet_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into compressed_parquet_test values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from compressed_parquet_test");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                "AAAA |\n" +
                "BBBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testWriteReadFromCompressedAvroExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table compressed_avro_test (col1 varchar(24))" +
                " COMPRESSED WITH SNAPPY STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"compressed_avro_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into compressed_avro_test values ('XXXX')," +
                "('YYYY')"));
        Assert.assertEquals("insertCount is wrong",2,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from compressed_avro_test");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "XXXX |\n" +
                "YYYY |",TestUtils.FormattedResult.ResultFactory.toString(rs));
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
        try{
            methodWatcher.executeUpdate(String.format("create external table failing_data_type_attribute(col1 int, col2 varchar(24), col4 varchar(24))" +
                    "STORED AS AVRO LOCATION '%s'", getResourceDirectory()+"avro_simple_file_test"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_data_type_attribute where col1=1");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception",
                    "The field 'c1':'StringType' defined in the table is not compatible with the field 'c1':'IntegerType' defined in the external file '"+getResourceDirectory()+"avro_simple_file_test'",
                    e.getMessage());
        }
    }

    @Test
    public void testReadPassedConstraint() throws Exception {
            methodWatcher.executeUpdate(String.format("create external table failing_correct_attribute(col1 varchar(24), col2 varchar(24), col4 varchar(24))" +
                    "partitioned by (col1) STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_sample_one"));
            ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_correct_attribute where col1='AAA'");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    " AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs));

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
        try{

                methodWatcher.executeUpdate(String.format("create external table compressed_ignored_text (col1 int, col2 varchar(24))" +
                        "COMPRESSED WITH SNAPPY STORED AS TEXTFILE LOCATION '%s'", getExternalResourceDirectory()+"compressed_ignored_text"));

                Assert.fail("Exception not thrown");
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception","EXT17",e.getSQLState());
            }
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
    public void validateReadParquetFromEmptyDirectory() throws Exception {
        String path = getExternalResourceDirectory()+"parquet_empty";
        methodWatcher.executeUpdate(String.format("create external table parquet_empty (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"parquet_empty"));
        ResultSet rs = methodWatcher.executeQuery("select * from parquet_empty");
        Assert.assertTrue(new File(path).exists());
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void validateReadAvroFromEmptyDirectory() throws Exception {
        String path = getExternalResourceDirectory()+"avro_empty";
        methodWatcher.executeUpdate(String.format("create external table avro_empty (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"avro_empty"));
        ResultSet rs = methodWatcher.executeQuery("select * from avro_empty");
        Assert.assertTrue(new File(path).exists());
        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testPinExternalOrcTable() throws Exception {
        String path = getExternalResourceDirectory()+"orc_pin";
        methodWatcher.executeUpdate(String.format("create external table orc_pin (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"orc_pin"));
        methodWatcher.executeUpdate("insert into orc_pin values (1,'test')");

        methodWatcher.executeUpdate("pin table orc_pin");
        ResultSet rs = methodWatcher.executeQuery("select * from orc_pin --splice-properties pin=true");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |test |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }



    @Test
    public void testPinExternalParquetTable() throws Exception {
        String path = getExternalResourceDirectory()+"parquet_pin";
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
        String path = getExternalResourceDirectory()+"avro_pin";
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
        String path = getExternalResourceDirectory()+"parquet_pin";
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
    public void validateReadORCFromEmptyDirectory() throws Exception {
        String path = getExternalResourceDirectory()+"orc_empty";
        methodWatcher.executeUpdate(String.format("create external table orc_empty (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'", path));
        Assert.assertTrue(new File(path).exists());
        ResultSet rs = methodWatcher.executeQuery("select * from orc_empty");

        Assert.assertEquals("",TestUtils.FormattedResult.ResultFactory.toString(rs));
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
    public void testCannotAlterExternalTable() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table alter_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("alter table alter_foo add column col3 int");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT12",e.getSQLState());
        }
    }

    @Test
    public void testCannotAlterExternalTableAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table alter_foo_avro (col1 int, col2 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("alter table alter_foo_avro add column col3 int");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT12",e.getSQLState());
        }
    }


    @Test
    public void testCannotAddIndexToExternalTable() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
            methodWatcher.executeUpdate(String.format("create external table add_index_foo (col1 int, col2 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("create index add_index_foo_ix on add_index_foo (col2)");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT13",e.getSQLState());
        }
    }

    @Test
    public void testCannotAddIndexToExternalTableAvro() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
            methodWatcher.executeUpdate(String.format("create external table add_index_foo_avro (col1 int, col2 varchar(24))" +
                    " STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("create index add_index_foo_ix on add_index_foo_avro (col2)");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT13",e.getSQLState());
        }
    }

    @Test
    public void testCannotAddTriggerToExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
        methodWatcher.executeUpdate(String.format("create external table add_trigger_foo (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        verifyTriggerCreateFails(tb.on("add_trigger_foo").named("trig").before().delete().row().then("select * from sys.systables"),
                "Cannot add triggers to external table 'ADD_TRIGGER_FOO'.");
    }

    @Test
    public void testCannotAddTriggerToExternalTableAvro() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_AVRO";
        methodWatcher.executeUpdate(String.format("create external table add_trigger_foo_avro (col1 int, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));

        verifyTriggerCreateFails(tb.on("add_trigger_foo_avro").named("trig").before().delete().row().then("select * from sys.systables"),
                "Cannot add triggers to external table 'ADD_TRIGGER_FOO_AVRO'.");
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

    public static String getExternalResourceDirectory() {
        return getHBaseDirectory()+"/target/external/";

    }


    @Test
    public void testWriteToWrongPartitionedParquetExternalTable() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table w_partitioned_parquet (col1 int, col2 varchar(24))" +
                    "partitioned by (col1) STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory() + "w_partitioned_parquet"));
            methodWatcher.executeUpdate(String.format("insert into w_partitioned_parquet values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));
            methodWatcher.executeUpdate(String.format("create external table w_partitioned_parquet_2 (col1 int, col2 varchar(24))" +
                    "partitioned by (col2) STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory() + "w_partitioned_parquet"));
            methodWatcher.executeUpdate(String.format("insert into w_partitioned_parquet_2 values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteToWrongPartitionedAvroExternalTable() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table w_partitioned_avro (col1 int, col2 varchar(24))" +
                    "partitioned by (col1) STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory() + "w_partitioned_avro"));
            methodWatcher.executeUpdate(String.format("insert into w_partitioned_avro values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));
            methodWatcher.executeUpdate(String.format("create external table w_partitioned_avro_2 (col1 int, col2 varchar(24))" +
                    "partitioned by (col2) STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory() + "w_partitioned_avro"));
            methodWatcher.executeUpdate(String.format("insert into w_partitioned_avro_2 values (1,'XXXX')," +
                    "(2,'YYYY')," +
                    "(3,'ZZZZ')"));

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testWriteToNotPermittedLocation() throws Exception{


        methodWatcher.executeUpdate(String.format("create external table PARQUET_NO_PERMISSION (col1 int, col2 varchar(24), col3 boolean)" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"PARQUET_NO_PERMISSION"));

        File file = new File(String.valueOf(getExternalResourceDirectory()+"PARQUET_NO_PERMISSION"));

        try{

            methodWatcher.executeUpdate(String.format("insert into PARQUET_NO_PERMISSION values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));
            file.setWritable(false);
            methodWatcher.executeUpdate(String.format("insert into  PARQUET_NO_PERMISSION values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));

            // we don't want to have a unwritable file in the folder, clean it up
            file.setWritable(true);
            file.delete();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            // we don't want to have a unwritable file in the folder, clean it up
            file.setWritable(true);
            file.delete();
            Assert.assertEquals("Wrong Exception","SE010",e.getSQLState());
        }
    }

    @Test
    public void testWriteToNotPermittedLocationAvro() throws Exception{


        methodWatcher.executeUpdate(String.format("create external table AVRO_NO_PERMISSION (col1 int, col2 varchar(24), col3 boolean)" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"AVRO_NO_PERMISSION"));

        File file = new File(String.valueOf(getExternalResourceDirectory()+"AVRO_NO_PERMISSION"));

        try{

            methodWatcher.executeUpdate(String.format("insert into AVRO_NO_PERMISSION values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));
            file.setWritable(false);
            methodWatcher.executeUpdate(String.format("insert into  AVRO_NO_PERMISSION values (1,'XXXX',true), (2,'YYYY',false), (3,'ZZZZ', true)"));

            // we don't want to have a unwritable file in the folder, clean it up
            file.setWritable(true);
            file.delete();
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            // we don't want to have a unwritable file in the folder, clean it up
            file.setWritable(true);
            file.delete();
            Assert.assertEquals("Wrong Exception","SE010",e.getSQLState());
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

    @Test
    public void testCollectStats() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table t1_orc (col1 int, col2 char(24))" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"t1_orc_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into t1_orc values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from t1_orc");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // collect table level stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "T1_ORC");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_ORC'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        // drop stats using table
        spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_TABLE_STATISTICS ('EXTERNALTABLEIT', 'T1_ORC')");

        // make sure it is clean
        rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_ORC' ");
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
        rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_ORC' ");
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        // drop stats using schema
        spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_SCHEMA_STATISTICS ('EXTERNALTABLEIT')");

        // make sure it is clean
        rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();
    }

    @Test
     public void testCollectStatsText() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table t1_csv (col1 int, col2 char(24))" +
                " STORED AS TEXTFILE LOCATION '%s'", getExternalResourceDirectory()+"t1_csv_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into t1_csv values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);

        ResultSet rs;
        // collect table level stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "T1_CSV");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_CSV'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();
    }

    @Test
    public void testCollectStatsParquet() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table t1_parq (col1 int, col2 char(24))" +
                " STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"t1_parq_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into t1_parq values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);

        ResultSet rs;
        // collect table level stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "T1_PARQ");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_PARQ'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();
    }

    @Test
    public void testCollectStatsAvro() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table t1_avro (col1 int, col2 char(24))" +
                " STORED AS AVRO LOCATION '%s'", getExternalResourceDirectory()+"t1_avro_test"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into t1_avro values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);

        ResultSet rs;
        // collect table level stats
        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.COLLECT_TABLE_STATISTICS(?,?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "T1_AVRO");
        ps.setBoolean(3, true);
        rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals("Error with COLLECT_TABLE_STATISTICS for external table","EXTERNALTABLEIT",  rs.getString(1));
        rs.close();

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sysvw.systablestatistics where schemaname = 'EXTERNALTABLEIT' and tablename = 'T1_AVRO'");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        ResultSet rs3 = methodWatcher.executeQuery("select * from t1_avro");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        rs3.close();
    }

    @Test
    public void testWriteReadArraysParquet() throws Exception {

        String tablePath = getExternalResourceDirectory()+"parquet_array";
        methodWatcher.executeUpdate(String.format("create external table parquet_array (col1 int array, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into parquet_array values ([1,1,1],'XXXX')," +
                "([2,2,2],'YYYY')," +
                "([3,3,3],'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from parquet_array");
        Assert.assertEquals("COL1    |COL2 |\n" +
                "-----------------\n" +
                "[1, 1, 1] |XXXX |\n" +
                "[2, 2, 2] |YYYY |\n" +
                "[3, 3, 3] |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from parquet_array");
        Assert.assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                "[2, 2, 2] |\n" +
                "[3, 3, 3] |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }

    @Test
    public void testWriteReadArraysAvro() throws Exception {

        String tablePath = getExternalResourceDirectory()+"avro_array";
        methodWatcher.executeUpdate(String.format("create external table avro_array (col1 int array, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into avro_array values ([1,1,1],'XXXX')," +
                "([2,2,2],'YYYY')," +
                "([3,3,3],'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from avro_array");
        Assert.assertEquals("COL1    |COL2 |\n" +
                "-----------------\n" +
                "[1, 1, 1] |XXXX |\n" +
                "[2, 2, 2] |YYYY |\n" +
                "[3, 3, 3] |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from avro_array");
        Assert.assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                "[2, 2, 2] |\n" +
                "[3, 3, 3] |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }

    @Test
    public void testWriteReadArraysWithStatsParquet() throws Exception {

        String tablePath = getExternalResourceDirectory()+"parquet_array_stats";
        methodWatcher.executeUpdate(String.format("create external table parquet_array_stats (col1 int array, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into parquet_array_stats values ([1,1,1],'XXXX')," +
                "([2,2,2],'YYYY')," +
                "([3,3,3],'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        methodWatcher.executeQuery("analyze table parquet_array_stats");

        ResultSet rs = methodWatcher.executeQuery("select * from parquet_array_stats");
        Assert.assertEquals("COL1    |COL2 |\n" +
                "-----------------\n" +
                "[1, 1, 1] |XXXX |\n" +
                "[2, 2, 2] |YYYY |\n" +
                "[3, 3, 3] |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from parquet_array_stats");
        Assert.assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                "[2, 2, 2] |\n" +
                "[3, 3, 3] |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }

    @Test
    public void testWriteReadArraysWithStatsAvro() throws Exception {

        String tablePath = getExternalResourceDirectory()+"avro_array_stats";
        methodWatcher.executeUpdate(String.format("create external table avro_array_stats (col1 int array, col2 varchar(24))" +
                " STORED AS AVRO LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into avro_array_stats values ([1,1,1],'XXXX')," +
                "([2,2,2],'YYYY')," +
                "([3,3,3],'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        methodWatcher.executeQuery("analyze table avro_array_stats");

        ResultSet rs = methodWatcher.executeQuery("select * from avro_array_stats");
        Assert.assertEquals("COL1    |COL2 |\n" +
                "-----------------\n" +
                "[1, 1, 1] |XXXX |\n" +
                "[2, 2, 2] |YYYY |\n" +
                "[3, 3, 3] |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from avro_array_stats");
        Assert.assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                "[2, 2, 2] |\n" +
                "[3, 3, 3] |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());
    }


    @Test
    public void testWriteReadArraysORC() throws Exception {

        String tablePath = getExternalResourceDirectory()+"orc_array";
        methodWatcher.executeUpdate(String.format("create external table orc_array (col1 int array, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'",tablePath));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into orc_array values ([1,1,1],'XXXX')," +
                "([2,2,2],'YYYY')," +
                "([3,3,3],'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from orc_array");
        Assert.assertEquals("COL1    |COL2 |\n" +
                "-----------------\n" +
                "[1, 1, 1] |XXXX |\n" +
                "[2, 2, 2] |YYYY |\n" +
                "[3, 3, 3] |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs2 = methodWatcher.executeQuery("select distinct col1 from orc_array");
        Assert.assertEquals("COL1    |\n" +
                "-----------\n" +
                "[1, 1, 1] |\n" +
                "[2, 2, 2] |\n" +
                "[3, 3, 3] |",TestUtils.FormattedResult.ResultFactory.toString(rs2));

        //Make sure empty file is created
        Assert.assertTrue(String.format("Table %s hasn't been created",tablePath), new File(tablePath).exists());

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
        try {
            String tablePath = getExternalResourceDirectory()+"/compression";
            methodWatcher.executeUpdate(String.format("CREATE EXTERNAL TABLE bad_compression_avro (col1 int) " +
                    "COMPRESSED WITH TURD " +
                    "STORED AS AVRO LOCATION '%s'",tablePath));
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X01",e.getSQLState());
        }
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

        methodWatcher.execute(String.format("create external table t_avro (col1 varchar(30), col2 int)" +
                " STORED AS AVRO LOCATION '%s'", getResourceDirectory() + "t_avro"));

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
        methodWatcher.execute(String.format("create external table t_num (col1 NUMERIC(23,2), col2 bigint)" +
                " STORED AS PARQUET LOCATION '%s'", getResourceDirectory() + "t_num"));
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
    public void testPureEmptyDirectory() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"pure_empty_directory";
        File path =  new File(tablePath);
        path.mkdir();
        methodWatcher.executeUpdate(String.format("create external table pure_empty_directory (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
        FileUtils.cleanDirectory(path);

        ResultSet rs = methodWatcher.executeQuery("select * from pure_empty_directory");
        String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
        String expected = "";
        Assert.assertEquals(actual, expected, actual);
    }

    @Test
    public void testNotExistDirectory() throws  Exception{
        try {
            String tablePath = getExternalResourceDirectory()+"empty_directory_not_exist";
            File path =  new File(tablePath);
            path.mkdir();
            methodWatcher.executeUpdate(String.format("create external table empty_directory_not_exist (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                    " STORED AS PARQUET LOCATION '%s'",tablePath));
            FileUtils.deleteDirectory(path);

            methodWatcher.executeQuery("select * from empty_directory_not_exist");
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT11",e.getSQLState());
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
                "PARTITIONED BY (COL1)\n" +
                "ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' LINES TERMINATED BY '\\\\n'\n" +
                "STORED AS TEXTFILE\n" +
                "LOCATION '" + tablePath + "';", rs.getString(1));

    }

    @Test
    public void testShowCreateTableOrcExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"show_orc";
        methodWatcher.execute(String.format("CREATE EXTERNAL TABLE myOrcTable\n" +
                "                    (col1 INT, col2 VARCHAR(24))\n" +
                "                    PARTITIONED BY (col1)\n" +
                "                    STORED AS ORC\n" +
                "                    LOCATION '%s'", tablePath));
        ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','MYORCTABLE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"MYORCTABLE\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "PARTITIONED BY (COL1)\n" +
                "STORED AS ORC\n" +
                "LOCATION '" + tablePath + "';" , rs.getString(1));

    }

    @Test
    public void testShowCreateTableParquetExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"show_parquet";
        methodWatcher.execute(String.format("CREATE EXTERNAL TABLE myParquetTable\n" +
                "                    (col1 INT, col2 VARCHAR(24))\n" +
                "                    PARTITIONED BY (col2)\n" +
                "                    STORED AS PARQUET\n" +
                "                    LOCATION '%s'" , tablePath));
        ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','MYPARQUETTABLE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"MYPARQUETTABLE\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "PARTITIONED BY (COL2)\n" +
                "STORED AS PARQUET\n" +
                "LOCATION '" + tablePath + "';", rs.getString(1));
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
                "PARTITIONED BY (COL2,COL3)\n" +
                "STORED AS PARQUET\n" +
                "LOCATION '" + tablePath + "';", rs.getString(1));

    }

    @Test
    public void testShowCreateTableAvroExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"show_avro";
        methodWatcher.execute(String.format("CREATE EXTERNAL TABLE myAvroTable\n" +
                "                    (col1 INT, col2 VARCHAR(24))\n" +
                "                    PARTITIONED BY (col1)\n" +
                "                    STORED AS AVRO\n" +
                "                    LOCATION '%s'", tablePath));
        ResultSet rs = methodWatcher.executeQuery("CALL SYSCS_UTIL.SHOW_CREATE_TABLE('EXTERNALTABLEIT','MYAVROTABLE')");
        rs.next();
        Assert.assertEquals("CREATE EXTERNAL TABLE \"EXTERNALTABLEIT\".\"MYAVROTABLE\" (\n" +
                "\"COL1\" INTEGER\n" +
                ",\"COL2\" VARCHAR(24)\n" +
                ") \n" +
                "PARTITIONED BY (COL1)\n" +
                "STORED AS AVRO\n" +
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

            try {
                conn.createStatement().execute("select count(*) from concurrent_test");
                success = true;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
