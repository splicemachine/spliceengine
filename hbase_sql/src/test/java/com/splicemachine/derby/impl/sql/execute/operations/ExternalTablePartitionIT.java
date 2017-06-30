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
import static org.junit.Assert.fail;

/**
 * Created by tgildersleeve on 6/30/17.
 * SPLICE-1621
 */
public class ExternalTablePartitionIT {

    private static final String SCHEMA_NAME = ExternalTableIT.class.getSimpleName().toUpperCase();
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
    public void testParquetNoPartition() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_no_partition";
            methodWatcher.executeUpdate(String.format("create external table parquet_no_part (col1 int, col2 int) " +
                    "STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_no_part values (1,1), (2,2)");
            methodWatcher.executeQuery("select * from parquet_no_part");
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testAvroNoPartition() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_no_partition";
            methodWatcher.executeUpdate(String.format("create external table avro_no_part (col1 int, col2 int) " +
                    "STORED AS AVRO LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcNoPartition() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_no_partition";
            methodWatcher.executeUpdate(String.format("create external table orc_no_part (col1 int, col2 int) " +
                    "STORED AS ORC LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testTextfileNoPartition() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_no_partition";
            methodWatcher.executeUpdate(String.format("create external table textfile_no_part (col1 int, col2 int) " +
                    "STORED AS TEXTFILE LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionOneCol() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_one_col_partition";
            methodWatcher.executeUpdate(String.format("create external table parquet_one_col (col1 int) partitioned by (col1) " +
                    "STORED AS PARQUET LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","XJ001",e.getSQLState());
            Assert.assertTrue("Wrong Exception", e.getMessage().contains("EXT11"));
        }
    }

    @Test
    public void testAvroPartitionOneCol() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_one_col_partition";
            methodWatcher.executeUpdate(String.format("create external table avro_one_col (col1 int) partitioned by (col1) " +
                    "STORED AS AVRO LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","XJ001",e.getSQLState());
            Assert.assertTrue("Wrong Exception", e.getMessage().contains("EXT11"));
        }
    }

    @Test
    public void testOrcPartitionOneCol() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_one_col_partition";
            methodWatcher.executeUpdate(String.format("create external table orc_one_col (col1 int) partitioned by (col1) " +
                    "STORED AS ORC LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","XJ001",e.getSQLState());
            Assert.assertTrue("Wrong Exception", e.getMessage().contains("EXT11"));
        }
    }

    @Test
    public void testTextfilePartitionOneCol() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_one_col_partition";
            methodWatcher.executeUpdate(String.format("create external table textfile_one_col (col1 int) partitioned by (col1) " +
                    "STORED AS TEXTFILE LOCATION '%s'",tablePath));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","XJ001",e.getSQLState());
            Assert.assertTrue("Wrong Exception", e.getMessage().contains("EXT11"));
        }
    }

    @Test @Ignore // SPLICE-1764
    public void testParquetPartitionFirst2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_first2";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_1st_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_1st_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_1st_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_1st_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_1st_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_1st_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test @Ignore // SPLICE-1765
    public void testAvroPartitionFirst2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_first2";
            methodWatcher.executeUpdate(String.format("create external table avro_part_1st_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_1st_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_1st_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_1st_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_1st_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_1st_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionFirst2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_first2";
            methodWatcher.executeUpdate(String.format("create external table orc_part_1st_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_1st_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_1st_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_1st_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_1st_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_1st_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testTextfilePartitionFirst2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_first2";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_1st_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col1) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_1st_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_1st_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_1st_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_1st_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_1st_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test @Ignore // SPLICE-1765
    public void testParquetPartitionSecond2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_second_2";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_2nd_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_2nd_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_2nd_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_2nd_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_2nd_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_2nd_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test @Ignore // SPLICE-1765
    public void testAvroPartitionSecond2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_second_2";
            methodWatcher.executeUpdate(String.format("create external table avro_part_2nd_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_2nd_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_2nd_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_2nd_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_2nd_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_2nd_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testOrcPartitionSecond2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_second_2";
            methodWatcher.executeUpdate(String.format("create external table orc_part_2nd_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_2nd_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_2nd_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_2nd_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_2nd_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_2nd_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testTextfilePartitionSecond2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_second_2";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_2nd_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col2) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_2nd_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_2nd_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_2nd_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_2nd_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_2nd_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testParquetPartitionLast2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/parquet_partition_last_2";
            methodWatcher.executeUpdate(String.format("create external table parquet_part_last_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS PARQUET LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into parquet_part_last_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from parquet_part_last_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from parquet_part_last_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from parquet_part_last_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from parquet_part_last_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }


    @Test
    public void testAvroPartitionLast2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/avro_partition_last_2";
            methodWatcher.executeUpdate(String.format("create external table avro_part_last_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS AVRO LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into avro_part_last_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from avro_part_last_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from avro_part_last_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from avro_part_last_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from avro_part_last_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testOrcPartitionLast2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/orc_partition_last_2";
            methodWatcher.executeUpdate(String.format("create external table orc_part_last_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS ORC LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into orc_part_last_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from orc_part_last_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from orc_part_last_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from orc_part_last_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from orc_part_last_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    @Test
    public void testTextfilePartitionLast2() throws Exception {
        try {
            String tablePath = getExternalResourceDirectory()+"/textfile_partition_last_2";
            methodWatcher.executeUpdate(String.format("create external table textfile_part_last_2 (col1 int, col2 int, col3 varchar(10)) " +
                    "partitioned by (col3) STORED AS TEXTFILE LOCATION '%s'",tablePath));
            methodWatcher.executeUpdate("insert into textfile_part_last_2 values (1,2,'AAA'),(3,4,'BBB'),(5,6,'CCC')");
            ResultSet rs = methodWatcher.executeQuery("select * from textfile_part_last_2");
            Assert.assertEquals("COL1 |COL2 |COL3 |\n" +
                    "------------------\n" +
                    "  1  |  2  | AAA |\n" +
                    "  3  |  4  | BBB |\n" +
                    "  5  |  6  | CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs));
            ResultSet rs1 = methodWatcher.executeQuery("select col1 from textfile_part_last_2");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |\n" +
                    "  3  |\n" +
                    "  5  |",TestUtils.FormattedResult.ResultFactory.toString(rs1));
            ResultSet rs2 = methodWatcher.executeQuery("select col2 from textfile_part_last_2");
            Assert.assertEquals("COL2 |\n" +
                    "------\n" +
                    "  2  |\n" +
                    "  4  |\n" +
                    "  6  |",TestUtils.FormattedResult.ResultFactory.toString(rs2));
            ResultSet rs3 = methodWatcher.executeQuery("select col3 from textfile_part_last_2");
            Assert.assertEquals("COL3 |\n" +
                    "------\n" +
                    " AAA |\n" +
                    " BBB |\n" +
                    " CCC |",TestUtils.FormattedResult.ResultFactory.toString(rs3));
        } catch (SQLException e) {
            Assert.fail("An exception should not be thrown");
        }
    }

    public static String getExternalResourceDirectory() {
        return SpliceUnitTest.getHBaseDirectory()+"/target/external/";
    }

}
