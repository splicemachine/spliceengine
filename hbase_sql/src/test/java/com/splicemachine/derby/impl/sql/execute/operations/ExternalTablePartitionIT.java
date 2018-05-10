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
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by tgildersleeve on 6/30/17.
 * SPLICE-1621
 */
public class ExternalTablePartitionIT {

    private static final String SCHEMA_NAME = ExternalTablePartitionIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);

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
    public void testParquetPartitionFirst() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_first";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_1st (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_1st values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_1st");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_1st");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_1st");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_1st");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from parquet_part_1st");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
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
        try {
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
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testAvroPartitionFirst() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_first";
            methodWatcher.executeUpdate(String.format("create external table avro_part_1st (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_1st values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_1st");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_1st");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_1st");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_1st");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from avro_part_1st");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionFirst() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_first";
            methodWatcher.executeUpdate(String.format("create external table orc_part_1st (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_1st values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_1st");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_1st");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_1st");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_1st");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from orc_part_1st");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));

            /* test query with predicates */
            ResultSet rs5 = methodWatcher.executeQuery("select * from orc_part_1st where col1=3");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));

            ResultSet rs6 = methodWatcher.executeQuery("select * from orc_part_1st where col2=4");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));

            ResultSet rs7 = methodWatcher.executeQuery("select * from orc_part_1st where col3='CCC'");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs7));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfilePartitionFirst() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_first";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_1st (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_1st values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_1st");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_1st");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_1st");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_1st");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from textfile_part_1st");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionFirstSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_first_second";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_1st_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1,col2) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_1st_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_1st_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_1st_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_1st_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_1st_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from parquet_part_1st_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testAvroPartitionFirstSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_first_second";
            methodWatcher.executeUpdate(String.format("create external table avro_part_1st_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1,col2) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_1st_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_1st_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_1st_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_1st_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_1st_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from avro_part_1st_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionFirstSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_first_second";
            methodWatcher.executeUpdate(String.format("create external table orc_part_1st_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1,col2) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_1st_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_1st_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_1st_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_1st_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_1st_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from orc_part_1st_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));

            // test query with predicate
            ResultSet rs5 = methodWatcher.executeQuery("select * from orc_part_1st_2nd where col1=3");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));

            ResultSet rs6 = methodWatcher.executeQuery("select * from orc_part_1st_2nd where col2=4");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));

            ResultSet rs7 = methodWatcher.executeQuery("select * from orc_part_1st_2nd where col3='AAA'");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |",TestUtils.FormattedResult.ResultFactory.toString(rs7));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfilePartitionFirstSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_first_second";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_1st_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1,col2) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_1st_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_1st_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_1st_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_1st_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_1st_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from textfile_part_1st_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_second";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from parquet_part_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testAvroPartitionSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_second";
            methodWatcher.executeUpdate(String.format("create external table avro_part_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from avro_part_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testOrcPartitionSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_second";
            methodWatcher.executeUpdate(String.format("create external table orc_part_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from orc_part_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));

            // test query with predicates
            ResultSet rs5 = methodWatcher.executeQuery("select * from orc_part_2nd where col1=3");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));
            ResultSet rs6 = methodWatcher.executeQuery("select * from orc_part_2nd where col2=4");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));
            ResultSet rs7 = methodWatcher.executeQuery("select * from orc_part_2nd where col3='CCC'");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs7));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }


    @Test
    public void testTextfilePartitionSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_second_";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from textfile_part_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionLast() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_last";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_last (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_last values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_last");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_last");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_last");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_last");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from parquet_part_last");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testAvroPartitionLast() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_last";
            methodWatcher.executeUpdate(String.format("create external table avro_part_last (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_last values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_last");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_last");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_last");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_last");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from avro_part_last");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionLast() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_last";
            methodWatcher.executeUpdate(String.format("create external table orc_part_last (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_last values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_last");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_last");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_last");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_last");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from orc_part_last");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));

            // test query with predicate
            ResultSet rs5 = methodWatcher.executeQuery("select * from orc_part_last where col1=3");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));
            ResultSet rs6 = methodWatcher.executeQuery("select * from orc_part_last where col2=4");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));
            ResultSet rs7 = methodWatcher.executeQuery("select * from orc_part_last where col3='CCC'");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs7));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfilePartitionLast() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_last";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_last (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_last values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_last");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_last");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_last");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_last");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from textfile_part_last");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionThirdSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_third_second";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_3rd_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3,col2) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_3rd_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_3rd_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_3rd_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_3rd_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_3rd_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from parquet_part_3rd_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testAvroPartitionThirdSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_third_second";
            methodWatcher.executeUpdate(String.format("create external table avro_part_3rd_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3,col2) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_3rd_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_3rd_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_3rd_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_3rd_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_3rd_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from avro_part_3rd_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionThirdSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_third_second";
            methodWatcher.executeUpdate(String.format("create external table orc_part_3rd_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3,col2) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_3rd_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_3rd_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_3rd_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_3rd_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_3rd_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from orc_part_3rd_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));

            //test query with prediate
            ResultSet rs5 = methodWatcher.executeQuery("select * from orc_part_3rd_2nd where col1=3");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs5));
            ResultSet rs6 = methodWatcher.executeQuery("select * from orc_part_3rd_2nd where col2=4");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  3  |  4  | BBB |",TestUtils.FormattedResult.ResultFactory.toString(rs6));
            ResultSet rs7 = methodWatcher.executeQuery("select * from orc_part_3rd_2nd where col3='CCC'");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs7));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfilePartitionThirdSecond() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_third_second";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_3rd_2nd (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3,col2) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_3rd_2nd values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_3rd_2nd");
            assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_3rd_2nd");
            assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_3rd_2nd");
            assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_3rd_2nd");
            assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
            ResultSet rs4 = methodWatcher.executeQuery("select col2, col3 from textfile_part_3rd_2nd");
            assertEquals("COL2 |COL3 |\n" +
                    "------------\n" +
                    "  2  | AAA |\n" +
                    "  4  | BBB |\n" +
                    "  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs4));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcAggressive() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_aggressive";
            methodWatcher.executeUpdate(String.format("create external table orc_aggressive (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char) " +
                    "partitioned by (col4, col6, col2) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_aggressive values " +
                    "(111,'AAA',true,111, 1.1, 'a'),(222,'BBB',false,222, 2.2, 'b'),(333,'CCC',true,333, 3.3, 'c')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_aggressive");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from orc_aggressive");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testParquetAggressive() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_aggressive";
            methodWatcher.executeUpdate(String.format("create external table parquet_aggressive (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char) " +
                    "partitioned by (col4, col6, col2) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_aggressive values " +
                    "(111,'AAA',true,111, 1.1, 'a'),(222,'BBB',false,222, 2.2, 'b'),(333,'CCC',true,333, 3.3, 'c')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_aggressive");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from parquet_aggressive");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testAvroAggressive() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_aggressive";
            methodWatcher.executeUpdate(String.format("create external table avro_aggressive (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char) " +
                    "partitioned by (col4, col6, col2) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_aggressive values " +
                    "(111,'AAA',true,111, 1.1, 'a'),(222,'BBB',false,222, 2.2, 'b'),(333,'CCC',true,333, 3.3, 'c')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_aggressive");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from avro_aggressive");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfileAggressive() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_aggressive";
            methodWatcher.executeUpdate(String.format("create external table textfile_aggressive (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char) " +
                    "partitioned by (col4, col6, col2) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_aggressive values " +
                    "(111,'AAA',true,111, 1.1, 'a'),(222,'BBB',false,222, 2.2, 'b'),(333,'CCC',true,333, 3.3, 'c')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_aggressive");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from textfile_aggressive");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testParquetAggressive2() throws Exception{
        try{
            String format = "parquet";
            String tablePath0 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_0",format);
            String tablePath1 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_1",format);
            String tablePath2 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_2",format);

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_0 (col1 varchar(10), col2 double, col3 int, col4 varchar(10), col5 int) " +
                    "partitioned by (col1, col2, col4, col5) stored as %s location '%s'",format,format,tablePath0));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_0 values " +
                    "('whoa',77.7,444,'crazy',21),('hey',11.11,222,'crazier',10)",format));
            ResultSet rs0 = methodWatcher.executeQuery(String.format("select col4, col2, col3 from %s_aggressive_2_0 where col5 = 21",format));
            assertEquals("COL4  |COL2 |COL3 |\n" +
                    "-------------------\n" +
                    "crazy |77.7 | 444 |",TestUtils.FormattedResult.ResultFactory.toString(rs0));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_1 (col1 int, col2 double, col3 char, col4 varchar(10), col5 int) " +
                    "partitioned by (col3, col1, col2) stored as %s location '%s'",format,format,tablePath1));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_1 values " +
                    "(666,66.666,'z','zing',20),(555,55.555,'y','yowza',40),(1,1.1,'g','garish',7)",format));
            ResultSet rs1 = methodWatcher.executeQuery(String.format("select col2,col4 from %s_aggressive_2_1",format));
            assertEquals("COL2  | COL4  |\n" +
                    "----------------\n" +
                    "  1.1  |garish |\n" +
                    "55.555 | yowza |\n" +
                    "66.666 | zing  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_2 (col1 varchar(10), col2 double, col3 varchar(10), col4 varchar(10), col5 int) " +
                    "partitioned by (col5, col4, col3) stored as %s location '%s'",format,format,tablePath2));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_2 values " +
                    "('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1)",format));
            ResultSet rs2 = methodWatcher.executeQuery(String.format("select col2 from %s_aggressive_2_2",format));
            assertEquals("COL2 |\n" +
                    "------\n" +
                    " 1.1 |\n" +
                    " 1.1 |\n" +
                    " 1.1 |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testAvroAggressive2() throws Exception{
        try{
            String format = "avro";
            String tablePath0 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_0",format);
            String tablePath1 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_1",format);
            String tablePath2 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_2",format);

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_0 (col1 varchar(10), col2 double, col3 int, col4 varchar(10), col5 int) " +
                    "partitioned by (col1, col2, col4, col5) stored as %s location '%s'",format,format,tablePath0));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_0 values " +
                    "('whoa',77.7,444,'crazy',21),('hey',11.11,222,'crazier',10)",format));
            ResultSet rs0 = methodWatcher.executeQuery(String.format("select col4, col2, col3 from %s_aggressive_2_0 where col5 = 21",format));
            assertEquals("COL4  |COL2 |COL3 |\n" +
                    "-------------------\n" +
                    "crazy |77.7 | 444 |",TestUtils.FormattedResult.ResultFactory.toString(rs0));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_1 (col1 int, col2 double, col3 char, col4 varchar(10), col5 int) " +
                    "partitioned by (col3, col1, col2) stored as %s location '%s'",format,format,tablePath1));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_1 values " +
                    "(666,66.666,'z','zing',20),(555,55.555,'y','yowza',40),(1,1.1,'g','garish',7)",format));
            ResultSet rs1 = methodWatcher.executeQuery(String.format("select col2,col4 from %s_aggressive_2_1",format));
            assertEquals("COL2  | COL4  |\n" +
                    "----------------\n" +
                    "  1.1  |garish |\n" +
                    "55.555 | yowza |\n" +
                    "66.666 | zing  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_2 (col1 varchar(10), col2 double, col3 varchar(10), col4 varchar(10), col5 int) " +
                    "partitioned by (col5, col4, col3) stored as %s location '%s'",format,format,tablePath2));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_2 values " +
                    "('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1)",format));
            ResultSet rs2 = methodWatcher.executeQuery(String.format("select col2 from %s_aggressive_2_2",format));
            assertEquals("COL2 |\n" +
                    "------\n" +
                    " 1.1 |\n" +
                    " 1.1 |\n" +
                    " 1.1 |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testOrcAggressive2() throws Exception{
        try{
            String format = "orc";
            String tablePath0 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_0",format);
            String tablePath1 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_1",format);
            String tablePath2 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_2",format);

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_0 (col1 varchar(10), col2 double, col3 int, col4 varchar(10), col5 int) " +
                    "partitioned by (col1, col2, col4, col5) stored as %s location '%s'",format,format,tablePath0));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_0 values " +
                    "('whoa',77.7,444,'crazy',21),('hey',11.11,222,'crazier',10)",format));
            ResultSet rs0 = methodWatcher.executeQuery(String.format("select col4, col2, col3 from %s_aggressive_2_0 where col5 = 21",format));
            assertEquals("COL4  |COL2 |COL3 |\n" +
                    "-------------------\n" +
                    "crazy |77.7 | 444 |",TestUtils.FormattedResult.ResultFactory.toString(rs0));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_1 (col1 int, col2 double, col3 char, col4 varchar(10), col5 int) " +
                    "partitioned by (col3, col1, col2) stored as %s location '%s'",format,format,tablePath1));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_1 values " +
                    "(666,66.666,'z','zing',20),(555,55.555,'y','yowza',40),(1,1.1,'g','garish',7)",format));
            ResultSet rs1 = methodWatcher.executeQuery(String.format("select col2,col4 from %s_aggressive_2_1",format));
            assertEquals("COL2  | COL4  |\n" +
                    "----------------\n" +
                    "  1.1  |garish |\n" +
                    "55.555 | yowza |\n" +
                    "66.666 | zing  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_2 (col1 varchar(10), col2 double, col3 varchar(10), col4 varchar(10), col5 int) " +
                    "partitioned by (col5, col4, col3) stored as %s location '%s'",format,format,tablePath2));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_2 values " +
                    "('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1)",format));
            ResultSet rs2 = methodWatcher.executeQuery(String.format("select col2 from %s_aggressive_2_2",format));
            assertEquals("COL2 |\n" +
                    "------\n" +
                    " 1.1 |\n" +
                    " 1.1 |\n" +
                    " 1.1 |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfileAggressive2() throws Exception{
        try{
            String format = "textfile";
            String tablePath0 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_0",format);
            String tablePath1 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_1",format);
            String tablePath2 = String.format(getExternalResourceDirectory()+"/%s_aggressive_2_2",format);

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_0 (col1 varchar(10), col2 double, col3 int, col4 varchar(10), col5 int) " +
                    "partitioned by (col1, col2, col4, col5) stored as %s location '%s'",format,format,tablePath0));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_0 values " +
                    "('whoa',77.7,444,'crazy',21),('hey',11.11,222,'crazier',10)",format));
            ResultSet rs0 = methodWatcher.executeQuery(String.format("select col4, col2, col3 from %s_aggressive_2_0 where col5 = 21",format));
            assertEquals("COL4  |COL2 |COL3 |\n" +
                    "-------------------\n" +
                    "crazy |77.7 | 444 |",TestUtils.FormattedResult.ResultFactory.toString(rs0));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_1 (col1 int, col2 double, col3 char, col4 varchar(10), col5 int) " +
                    "partitioned by (col3, col1, col2) stored as %s location '%s'",format,format,tablePath1));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_1 values " +
                    "(666,66.666,'z','zing',20),(555,55.555,'y','yowza',40),(1,1.1,'g','garish',7)",format));
            ResultSet rs1 = methodWatcher.executeQuery(String.format("select col2,col4 from %s_aggressive_2_1",format));
            assertEquals("COL2  | COL4  |\n" +
                    "----------------\n" +
                    "  1.1  |garish |\n" +
                    "55.555 | yowza |\n" +
                    "66.666 | zing  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));

            methodWatcher.executeUpdate(String.format("create external table %s_aggressive_2_2 (col1 varchar(10), col2 double, col3 varchar(10), col4 varchar(10), col5 int) " +
                    "partitioned by (col5, col4, col3) stored as %s location '%s'",format,format,tablePath2));
            methodWatcher.executeUpdate(String.format("insert into %s_aggressive_2_2 values " +
                    "('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1),('hello',1.1,'goodbye','farewell',1)",format));
            ResultSet rs2 = methodWatcher.executeQuery(String.format("select col2 from %s_aggressive_2_2",format));
            assertEquals("COL2 |\n" +
                    "------\n" +
                    " 1.1 |\n" +
                    " 1.1 |\n" +
                    " 1.1 |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    // tests for creating an external table from an existing file:

    @Test @Ignore // SPLICE-1807
    public void testParquetPartitionExisting() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table parquet_partition_existing (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col char)" +
                    "partitioned by (col4, col2,col1) STORED AS PARQUET LOCATION '%s'", SpliceUnitTest.getResourceDirectory()+"parquet_partition_existing"));
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_partition_existing");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from parquet_partition_existing");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test @Ignore // SPLICE-1807
    public void testAvroPartitionExisting() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table avro_partition_existing (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char)" +
                    "partitioned by (col4, col2, col1) STORED AS AVRO LOCATION '%s'", SpliceUnitTest.getResourceDirectory()+"avro_partition_existing"));
            ResultSet rs = methodWatcher.executeQuery("select * from avro_partition_existing");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from avro_partition_existing");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test @Ignore // SPLICE-1807
    public void testOrcPartitionExisting() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("create external table orc_partition_existing (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char)" +
                    "partitioned by (col4, col2, col1) STORED AS ORC LOCATION '%s'", SpliceUnitTest.getResourceDirectory()+"orc_partition_existing"));
            ResultSet rs = methodWatcher.executeQuery("select * from orc_partition_existing");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from orc_partition_existing");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }

    @Test
    public void testTextfilePartitionExisting() throws Exception {
        try {

            methodWatcher.executeUpdate(String.format("create external table textfile_partition_existing (col1 int, col2 varchar(10), col3 boolean, col4 int, col5 double, col6 char)" +
                    "partitioned by (col4, col2, col1) STORED AS TEXTFILE LOCATION '%s'", SpliceUnitTest.getResourceDirectory() + "textfile_partition_existing"));
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_partition_existing");
            assertEquals("COL1 |COL2 |COL3  |COL4 |COL5 |COL6 |\n" +
                    "-------------------------------------\n" +
                    " 111 | AAA |true  | 111 | 1.1 |  a  |\n" +
                    " 222 | BBB |false | 222 | 2.2 |  b  |\n" +
                    " 333 | CCC |true  | 333 | 3.3 |  c  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs2 = methodWatcher.executeQuery("select col5, col2, col6 from textfile_partition_existing");
            assertEquals("COL5 |COL2 |COL6 |\n" +
                    "------------------\n" +
                    " 1.1 | AAA |  a  |\n" +
                    " 2.2 | BBB |  b  |\n" +
                    " 3.3 | CCC |  c  |", TestUtils.FormattedResult.ResultFactory.toString(rs2));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown. Error: " + e.getMessage());
        }
    }


    public static String getExternalResourceDirectory() {
        return SpliceUnitTest.getHBaseDirectory()+"/target/external/";
    }


    private int getNumberOfFiles(String dirPath) {
        int count = 0;
        File f = new File(dirPath);
        File[] files = f.listFiles();

        if (files == null)
            return 0;

        for (int i = 0; i < files.length; i++) {
            count++;
            File file = files[i];

            if (file.isDirectory()) {
                count += getNumberOfFiles(file.getAbsolutePath());
            }
        }
        return count;
    }

}
