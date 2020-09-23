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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tgildersleeve on 6/30/17.
 * SPLICE-1621
 */
@Category(LongerThanTwoMinutes.class)
public class ExternalTablePartitionIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = ExternalTablePartitionIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

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

    /// this will return the temp directory, that is created on demand once for each test
    public String getExternalResourceDirectory() throws Exception
    {
        return tempDir.toString() + "/";
    }

    @Test
    public void testSparkGeneratesFewParquetFiles() throws Exception {
        testSparkGeneratesFewFiles("parquet");
    }
    @Test
    public void testSparkGeneratesFewOrcFiles() throws Exception {
        testSparkGeneratesFewFiles("orc");
    }
    @Test
    public void testSparkGeneratesFewAvroFiles() throws Exception {
        testSparkGeneratesFewFiles("avro");
    }

    public void testSparkGeneratesFewFiles(String format) throws Exception {
        String tablePath = getExternalResourceDirectory()+"/"+format+"_number_files";
        methodWatcher.executeUpdate(String.format("create table %s_number_files_orig (col1 int, col2 int, col3 varchar(10))",format));
        methodWatcher.executeUpdate(String.format("create external table %s_number_files (col1 int, col2 int, col3 varchar(10)) " +
                "partitioned by (col1) STORED AS %1$s LOCATION '%s'",format,tablePath));
        methodWatcher.executeUpdate(String.format("insert into %s_number_files_orig values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')",format));
        methodWatcher.executeUpdate(String.format("insert into %s_number_files_orig values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')",format));


        methodWatcher.executeUpdate(String.format("insert into %1$s_number_files " +
                "select * from %1$s_number_files_orig --splice-properties useSpark=true \n" +
                "union all select * from %1$s_number_files_orig " +
                "union all select * from %1$s_number_files_orig " +
                "union all select * from %1$s_number_files_orig " +
                "union all select * from %1$s_number_files_orig " +
                "union all select * from %1$s_number_files_orig ",format));


        ResultSet rs = methodWatcher.executeQuery(String.format("select count(*) from %s_number_files",format));

        assertTrue(rs.next());
        assertEquals(36, rs.getInt(1));

        // There should be 11 entries:
        // 3 data files (+3 crc files)
        // 1 _SUCCESS file (+1 crc file)
        // 3 subdirectories
        assertEquals(11, getNumberOfFiles(tablePath));
    }

    String[] fileFormatsText = { "PARQUET", "ORC", "AVRO", "TEXTFILE" };

    private void checkPartitionInsertSelect(String testName, String fileFormat, String partitionedBy) throws Exception {
        String name = testName + "_" + fileFormat;
        String tablePath = getExternalResourceDirectory() + "/" + name;
        methodWatcher.executeUpdate(String.format("create external table " + name + " (col1 int, col2 int, col3 varchar(10)) " +
                "partitioned by (" + partitionedBy + ") STORED AS " + fileFormat + " LOCATION '%s'", tablePath));

        methodWatcher.executeUpdate("insert into " + name  + " values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");

        ResultSet rs = methodWatcher.executeQuery("select * from " + name );
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  2  | AAA |\n" +
                "  3  |  4  | BBB |\n" +
                "  5  |  6  | CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs));
        ResultSet rs1 = methodWatcher.executeQuery("select col1 from " + name );
        assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |\n" +
                "  3  |\n" +
                "  5  |", TestUtils.FormattedResult.ResultFactory.toString(rs1));
        ResultSet rs2 = methodWatcher.executeQuery("select col2 from " + name );
        assertEquals("COL2 |\n" +
                "------\n" +
                "  2  |\n" +
                "  4  |\n" +
                "  6  |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        ResultSet rs3 = methodWatcher.executeQuery("select col3 from " + name );
        assertEquals("COL3 |\n" +
                "------\n" +
                " AAA |\n" +
                " BBB |\n" +
                " CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs3));
        ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from " + name );
        assertEquals("COL2 |COL3 |\n" +
                "------------\n" +
                "  2  | AAA |\n" +
                "  4  | BBB |\n" +
                "  6  | CCC |", TestUtils.FormattedResult.ResultFactory.toString(rs4));

        /* test query with predicates */
        ResultSet rs5 = methodWatcher.executeQuery("select * from " + name  + " where col1=3");
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));

        /* test query with predicate on partitioning column and non-partitioning column */
        ResultSet rs51 = methodWatcher.executeQuery("select * from " + name  + " where col1>=3 and col1<4 and col3='BBB'");
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs51));


        ResultSet rs6 = methodWatcher.executeQuery("select * from " + name  + " where col2=4");
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));

        ResultSet rs7 = methodWatcher.executeQuery("select * from " + name  + " where col3='CCC'");
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs7));


        ResultSet rs8 = methodWatcher.executeQuery("select * from " + name + " where col3='AAA'");
        assertEquals("COL1 |COL2 |COL3 |\n" +
                "------------------\n" +
                "  1  |  2  | AAA |", TestUtils.FormattedResult.ResultFactory.toString(rs8));
    }

    @Test
    public void testPartitionFirst() throws Exception {
        for( String fileFormat : fileFormatsText) {
            checkPartitionInsertSelect( "partition_first", fileFormat, "col1");
        }
    }

    @Test
    public void testPartitionFirstSecond() throws Exception {
        for( String fileFormat : fileFormatsText) {
            checkPartitionInsertSelect( "partition_first_second", fileFormat, "col1, col2");
        }
    }

    @Test
    public void testPartitionSecond() throws Exception {
        for( String fileFormat : fileFormatsText) {
            checkPartitionInsertSelect( "partition_second", fileFormat, "col2");
        }
    }

    @Test
    public void testPartitionLast() throws Exception {
        for (String fileFormat : fileFormatsText) {
            checkPartitionInsertSelect( "partition_last", fileFormat, "col3");
        }
    }

    @Test
    public void testPartitionThirdSecond() throws Exception {
        for (String fileFormat : fileFormatsText) {
            checkPartitionInsertSelect( "partition_third_second", fileFormat, "col3, col2");
        }
    }

    @Test
    public void testAggressive() throws Exception {
        for (String fileFormat : fileFormatsText) {
            String name = "orc_aggressive_" + fileFormat;
            String tablePath = getExternalResourceDirectory() + "/" + name;
            methodWatcher.executeUpdate(String.format("create external table " + name +
                    " (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char) " +
                    "partitioned by (col4, col6, col2) STORED AS " + fileFormat + " LOCATION '%s'", tablePath));
            methodWatcher.executeUpdate("insert into " + name + " values " +
                    "(111,'AAA',true,111, 1.1, 'a'),(222,'BBB',false,222, 2.2, 'b'),(333,'CCC',true,333, 3.3, 'c')");
            ResultSet rs = methodWatcher.executeQuery("select * from " + name );
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from " + name);
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        }
    }

    @Test
    public void testAggressive2() throws Exception{
        for (String format : fileFormatsText) {
            String tablePath0 = String.format(getExternalResourceDirectory() + "/%s_aggressive_2_0", format);
            String tablePath1 = String.format(getExternalResourceDirectory() + "/%s_aggressive_2_1", format);
            String tablePath2 = String.format(getExternalResourceDirectory() + "/%s_aggressive_2_2", format);

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_0 " +
                    "(col1 varchar(10), col2 double, col3 int, col4 varchar(10), col5 int) " +
                    "partitioned by (col1, col2, col4, col5) stored as %s location '%s'", format, format, tablePath0));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_0 values " +
                    "('whoa',77.7,444,'crazy',21),('hey',11.11,222,'crazier',10)", format));
            ResultSet rs0 = methodWatcher.executeQuery(String.format("select col4, col2, col3 from %s_aggressive_2_0 where col5 = 21", format));
            assertEquals("COL4  |COL2 |COL3 |\n" +
                    "-------------------\n" +
                    "crazy |77.7 | 444 |", TestUtils.FormattedResult.ResultFactory.toString(rs0));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_1 " +
                    "(col1 int, col2 double, col3 char, col4 varchar(10), col5 int) " +
                    "partitioned by (col3, col1, col2) stored as %s location '%s'", format, format, tablePath1));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_1 values " +
                    "(666,66.666,'z','zing',20),(555,55.555,'y','yowza',40),(1,1.1,'g','garish',7)", format));
            ResultSet rs1 = methodWatcher.executeQuery(String.format("select col2,col4 from %s_aggressive_2_1", format));
            assertEquals("COL2  | COL4  |\n" +
                    "----------------\n" +
                    "  1.1  |garish |\n" +
                    "55.555 | yowza |\n" +
                    "66.666 | zing  |", TestUtils.FormattedResult.ResultFactory.toString(rs1));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_2 " +
                    "(col1 varchar(10), col2 double, col3 varchar(10), col4 varchar(10), col5 int) " +
                    "partitioned by (col5, col4, col3) stored as %s location '%s'", format, format, tablePath2));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_2 values " +
                    "('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1)", format));
            ResultSet rs2 = methodWatcher.executeQuery(String.format("select col2 from %s_aggressive_2_2", format));
            assertEquals("COL2 |\n" +
                    "------\n" +
                    " 1.1 |\n" +
                    " 1.1 |\n" +
                    " 1.1 |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        }
    }

    // tests for creating an external table from an existing file:

    @Test
    public void testPartitionExisting() throws Exception {
        for (String fileFormat : fileFormatsText)
        {
            String name = "partition_existing_" + fileFormat;
            // parquet_partition_existing, orc_partition_existing, avro_partition_existing, textfile_partition_existing
            String tablePath = getResourceDirectory() + fileFormat.toLowerCase() + "_partition_existing";
            methodWatcher.executeUpdate(String.format("create external table " + name +
                    " (\"c0\" int, \"c1\" varchar(10), \"c2\" boolean, \"c3\" int, \"c4\" double, \"c5\" char)" +
                    "partitioned by (\"c3\", \"c1\") STORED AS " + fileFormat + " LOCATION '%s'", tablePath));
            ResultSet rs = methodWatcher.executeQuery("select * from " + name + " order by 1");
            assertEquals("c0  |c1  | c2   |c3  |c4  |c5 |\n" +
                    "-------------------------------\n" +
                    "111 |AAA |true  |111 |1.1 | a |\n" +
                    "222 |BBB |false |222 |2.2 | b |\n" +
                    "333 |CCC |true  |333 |3.3 | c |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select \"c4\", \"c1\", \"c5\" from " + name);
            assertEquals("c4  |c1  |c5 |\n" +
                    "--------------\n" +
                    "1.1 |AAA | a |\n" +
                    "2.2 |BBB | b |\n" +
                    "3.3 |CCC | c |", TestUtils.FormattedResult.ResultFactory.toString(rs2));

            methodWatcher.execute("drop table " + name);

            methodWatcher.executeUpdate(String.format("create external table " + name +
                    " (\"c0\" int, \"c1\" varchar(10), \"c2\" boolean, \"c3\" int, \"c4\" double, \"c5\" char)" +
                    "partitioned by (\"c3\", \"c1\") STORED AS " + fileFormat + " LOCATION '%s' merge schema", tablePath));
            rs = methodWatcher.executeQuery("select * from " + name + " order by 1");
            assertEquals("c0  |c1  | c2   |c3  |c4  |c5 |\n" +
                    "-------------------------------\n" +
                    "111 |AAA |true  |111 |1.1 | a |\n" +
                    "222 |BBB |false |222 |2.2 | b |\n" +
                    "333 |CCC |true  |333 |3.3 | c |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs2 = methodWatcher.executeQuery("select \"c4\", \"c1\", \"c5\" from " + name );
            assertEquals("c4  |c1  |c5 |\n" +
                    "--------------\n" +
                    "1.1 |AAA | a |\n" +
                    "2.2 |BBB | b |\n" +
                    "3.3 |CCC | c |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        }

    }

    @Test
    public void testInsertionToHiveData() throws Exception {
        for (String fileFormat : new String[] {"PARQUET", "AVRO"} ) {
            String name = "insertion_to_hive_" + fileFormat;
            String tmpPath = getTempCopyOfResourceDirectory(tempDir, "pt_" + fileFormat.toLowerCase() );
            methodWatcher.executeUpdate(String.format("create external table " + name +
                            "(\"name\" varchar(10), \"age\" int, \"state\" char(2)) " +
                            "partitioned by (\"state\") stored as " + fileFormat + " location '%s'", tmpPath));
            methodWatcher.execute("insert into " + name + " values ('Kate', 19, 'HI')");
            ResultSet rs = methodWatcher.executeQuery("select * from " + name + " order by 1");
            String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected = "name | age | state |\n" +
                    "--------------------\n" +
                    "Kate | 19  |  HI   |\n" +
                    " Sam | 20  |  CA   |\n" +
                    " Tom | 21  |  NY   |";
            assertEquals(actual, expected, actual);
        }
    }

    @Test
    public void testOrcRowGroupStatsWherePartitionColumnIsNotTheLast() throws Exception {
        String tablePath = SpliceUnitTest.getResourceDirectory() +"orc_test_rowgroupstats";
        methodWatcher.executeUpdate(String.format("create external table orc_test_rowgroupstats (a1 int, b1 date, c1 int) " +
                "partitioned by (b1) STORED AS ORC LOCATION '%s'",tablePath));

        /* Q1 predicate on partition column */
        ResultSet rs = methodWatcher.executeQuery("select c1 from orc_test_rowgroupstats where b1=date('2018-11-26') order by c1 {limit 2}");
        assertEquals("C1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q2 predicte on non-partition column */
        rs = methodWatcher.executeQuery("select c1 from orc_test_rowgroupstats where c1<10");
        assertEquals("C1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 6 |\n" +
                " 7 |\n" +
                " 8 |\n" +
                " 9 |",TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
