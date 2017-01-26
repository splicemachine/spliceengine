/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_dao.TriggerBuilder;
import org.apache.commons.io.FileUtils;
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
    public void testFileExistingNotDeleted() throws  Exception{
        String tablePath = getResourceDirectory()+"parquet_sample_one";

        File newFile = new File(tablePath);
        newFile.createNewFile();
        long lastModified = newFile.lastModified();


        methodWatcher.executeUpdate(String.format("create external table table_to_existing_file (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));
        Assert.assertEquals(String.format("File : %s have been modified and it shouldn't",tablePath),lastModified,newFile.lastModified());
    }

    @Test
    public void refreshRequireExternalTable() throws  Exception{
        String tablePath = getExternalResourceDirectory()+"external_table_refresh";

        methodWatcher.executeUpdate(String.format("create external table external_table_refresh (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        PreparedStatement ps = spliceClassWatcher.prepareCall("CALL  SYSCS_UTIL.SYSCS_REFRESH_EXTERNAL_TABLE(?,?) ");
        ps.setString(1, "EXTERNALTABLEIT");
        ps.setString(2, "EXTERNAL_TABLE_REFRESH");
        ResultSet rs = ps.executeQuery();
        rs.next();

        Assert.assertEquals("Error with refresh external table","EXTERNALTABLEIT.EXTERNAL_TABLE_REFRESH schema table refreshed",  rs.getString(1));


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
    // SPLICE-1219
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
    public void testWriteReadFromPartitionedParquetExternalTable() throws Exception {
        String tablePath =  getExternalResourceDirectory()+"partitioned_parquet";
        methodWatcher.executeUpdate(String.format("create external table partitioned_parquet (col1 int, col2 varchar(24))" +
                "partitioned by (col1) STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"partitioned_parquet"));
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

    @Test @Ignore
    public void testWriteReadFromSimpleORCExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table simple_orc (col1 int, col2 varchar(24))" +
                " STORED AS ORC LOCATION '%s'", getExternalResourceDirectory()+"simple_orc"));
        int insertCount = methodWatcher.executeUpdate(String.format("insert into simple_orc values (1,'XXXX')," +
                "(2,'YYYY')," +
                "(3,'ZZZZ')"));
        Assert.assertEquals("insertCount is wrong",3,insertCount);
        ResultSet rs = methodWatcher.executeQuery("select * from simple_orc");
        Assert.assertEquals("COL1 |COL2 |\n" +
                "------------\n" +
                "  1  |XXXX |\n" +
                "  2  |YYYY |\n" +
                "  3  |ZZZZ |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test @Ignore
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
    public void testWriteReadWithPreExistingParquetAndOr() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table parquet_simple_file_table_and_or (col1 varchar(24), col2 varchar(24), col3 varchar(24))" +
                "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_simple_file_test"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from parquet_simple_file_table_and_or where col1='BBB' OR ( col1='AAA' AND col2='AAA')");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                " AAA |\n" +
                " BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test @Ignore
    public void testWriteReadFromCompressedParquetExternalTable() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table compressed_parquet_test (col1 int, col2 varchar(24))" +
                " COMPRESSED WITH SNAPPY  STORED AS PARQUET LOCATION '%s'", getExternalResourceDirectory()+"compressed_parquet_test"));
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
    public void testReadPassedConstraint() throws Exception {
        methodWatcher.executeUpdate(String.format("create external table failing_correct_attribute(col1 varchar(24), col2 varchar(24), col4 varchar(24))" +
                "STORED AS PARQUET LOCATION '%s'", getResourceDirectory()+"parquet_sample_one"));
        ResultSet rs = methodWatcher.executeQuery("select COL2 from failing_correct_attribute where col1='AAA'");
        Assert.assertEquals("COL2 |\n" +
                "------\n" +
                " AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs));

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

    // look like it will be resolve in the next Spark version
    // https://issues.apache.org/jira/browse/SPARK-15474
    // for now ignoring
    @Test @Ignore
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
    public void testCannotAddTriggerToExternalTable() throws Exception {
        String tablePath = getExternalResourceDirectory()+"/HUMPTY_DUMPTY_MOLITOR";
        methodWatcher.executeUpdate(String.format("create external table add_trigger_foo (col1 int, col2 varchar(24))" +
                " STORED AS PARQUET LOCATION '%s'",tablePath));

        verifyTriggerCreateFails(tb.on("add_trigger_foo").named("trig").before().delete().row().then("select * from sys.systables"),
                "Cannot add triggers to external table 'ADD_TRIGGER_FOO'.");
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
    @Ignore
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
    public void testWriteToNotPermitedLocation() throws Exception{


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


    @Test @Ignore // failing on mapr5.2.0. Temporary ignoring
    public void testReadToNotPermitedLocation() throws Exception{


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

        ResultSet rs2 = methodWatcher.executeQuery("select total_row_count from sys.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
        String expected = "TOTAL_ROW_COUNT |\n" +
                "------------------\n" +
                "        3        |";
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        // drop stats using table
        spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_TABLE_STATISTICS ('EXTERNALTABLEIT', 'T1_ORC')");

        // make sure it is clean
        rs2 = methodWatcher.executeQuery("select total_row_count from sys.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
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
        rs2 = methodWatcher.executeQuery("select total_row_count from sys.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();

        // drop stats using schema
        spliceClassWatcher.executeUpdate("CALL  SYSCS_UTIL.DROP_SCHEMA_STATISTICS ('EXTERNALTABLEIT')");

        // make sure it is clean
        rs2 = methodWatcher.executeQuery("select total_row_count from sys.systablestatistics where schemaname = 'EXTERNALTABLEIT' ");
        Assert.assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        rs2.close();
    }

}