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
 *
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.spark.SparkExternalTableUtil;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.system.CsvOptions;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ExternalTableUnitTests {

    @Test
    public void testParsePartitionsFromFiles() {
        String root = "hdfs://host:123/partition_test/web_sales5/";
        String[] spaths = {
                root + ".DS_Store",
                root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/part-00042.c000.snappy.parquet",
                root + "_SUCCESS",
                root + "c=3.14/ws_sold_date_sk=2450817/part-01434.c000.snappy.parquet",
                root + "c=3.14/ws_sold_date_sk=2450816/part-00026.c000.snappy.parquet",
                root + "c=3.14/ws_sold_date_sk=2450818/part-00780.c000.snappy.parquet",
                root + "c=3.14/ws_sold_date_sk=2450818/part-00780.c001.snappy.parquet",
                root + "c=3.14/ws_sold_date_sk=2450818/part-00780.c002.snappy.parquet"
        };
        Path basePath = new Path( root );
        HashSet<Path> basePaths = new HashSet<>(); basePaths.add(basePath);

        List<Path> files = Arrays.stream(spaths).map(Path::new).collect(toList());

        com.splicemachine.spark.splicemachine.PartitionSpec ps = SparkExternalTableUtil.parsePartitionsFromFiles(
                files, true, basePaths, null, null );
        Assert.assertEquals("StructType(StructField(c,DoubleType,true), StructField(ws_sold_date_sk,IntegerType,true))",
                ps.partitionColumns().toString());
        Assert.assertEquals("List(" +
                "PartitionPath([3.14,null]," + root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__), " +
                "PartitionPath([3.14,2450817]," + root + "c=3.14/ws_sold_date_sk=2450817), " +
                "PartitionPath([3.14,2450816]," + root + "c=3.14/ws_sold_date_sk=2450816), " +
                "PartitionPath([3.14,2450818]," + root + "c=3.14/ws_sold_date_sk=2450818))",
                ps.partitions().toString());
    }

    @Test
    public void testParsePartitionsFromFiles_one_userDefined() {
        String root = "hdfs://host:123/partition_test/web_sales5/";
        String[] spaths = {
                root + "c=3.14/ws_sold_date_sk=2450818/part-00780.c002.snappy.parquet"
        };
        Path basePath = new Path( root );
        HashSet<Path> basePaths = new HashSet<>(); basePaths.add(basePath);

        List<Path> files = Arrays.stream(spaths).map(Path::new).collect(toList());

        StructType s = new StructType();
        s = s.add("c", DataTypes.StringType);
        s = s.add("ws_sold_date_sk", DataTypes.DoubleType);

        com.splicemachine.spark.splicemachine.PartitionSpec ps = SparkExternalTableUtil.parsePartitionsFromFiles(
                files, true, basePaths, s, null );
        Assert.assertEquals("StructType(StructField(c,StringType,true), StructField(ws_sold_date_sk,DoubleType,true))",
                ps.partitionColumns().toString());
        Assert.assertEquals("List(PartitionPath([3.14,2450818.0]," + root + "c=3.14/ws_sold_date_sk=2450818))",
                ps.partitions().toString());
    }

    @Test
    public void testParsePartitionsFromFiles_wrong_type() {
        String root = "hdfs://host:123/partition_test/web_sales5/";
        String[] spaths = {
                root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/part-00780.c002.snappy.parquet",
                root + "c=3.14/ws_sold_date_sk=HELLO/part-00780.c002.snappy.parquet"
        };
        Path basePath = new Path( "hdfs://host:123/partition_test/web_sales5/" );
        HashSet<Path> basePaths = new HashSet<>(); basePaths.add(basePath);

        List<Path> files = Arrays.stream(spaths).map(Path::new).collect(toList());

        StructType s = new StructType();
        s = s.add("ws_sold_date_sk", DataTypes.DoubleType);

        try {
            com.splicemachine.spark.splicemachine.PartitionSpec ps = SparkExternalTableUtil.parsePartitionsFromFiles(
                    files, true, basePaths, s,null);
            Assert.fail("no exception");
        }
        catch(Exception e)
        {

        }
    }

    @Test
    public void testParsePartitionsFromFiles_wrong_type2() {
        String root = "hdfs://host:123/partition_test/web_sales5/";
        String[] spaths = {
                root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__/part-00780.c002.snappy.parquet"
        };
        Path basePath = new Path( root );
        HashSet<Path> basePaths = new HashSet<>(); basePaths.add(basePath);

        List<Path> files = Arrays.stream(spaths).map(Path::new).collect(toList());

        StructType s = new StructType();
        s = s.add("ws_sold_date_sk", DataTypes.DoubleType);

        com.splicemachine.spark.splicemachine.PartitionSpec ps = SparkExternalTableUtil.parsePartitionsFromFiles(
                files, true, basePaths, s, null);

        Assert.assertEquals("StructType(StructField(c,DoubleType,true), StructField(ws_sold_date_sk,DoubleType,true))",
                ps.partitionColumns().toString());
        Assert.assertEquals("List(PartitionPath([3.14,null]," + root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__))",
                ps.partitions().toString());
    }

    StructType structTypeSample1()
    {
        StructType s = new StructType();
        s = s.add("col0", DataTypes.DoubleType);
        s = s.add("col1", DataTypes.IntegerType);
        s = s.add("col2", DataTypes.BooleanType);
        s = s.add("col3", DataTypes.LongType);
        return s;
    }

    void testSort(int[] partition, int[] expected)
    {
        StructType s = structTypeSample1();
        StructType original = structTypeSample1();

        SparkExternalTableUtil.preSortColumns(s.fields(), partition);
        assert s.length() == original.length();
        for(int i = 0; i < s.length(); i++) {
            Assert.assertEquals( s.fields()[i].toString(), original.fields()[expected[i]].toString());
        }

        // use sortColumns to reverse the sorting done by preSortColumns
        SparkExternalTableUtil.sortColumns(s.fields(), partition);
        Assert.assertEquals(original.toString(), s.toString());
    }

    @Test
    public void testSortColumns()
    {
        testSort( new int[]{0, 2}, new int[]{1, 3, 0, 2} );
        testSort( new int[]{0},    new int[]{1, 2, 3, 0} );
        testSort( new int[]{3},    new int[]{0, 1, 2, 3} );
        testSort( new int[]{},     new int[]{0, 1, 2, 3} );
    }

    @Test
    public void testGetSuggestedSchema()
    {
        Assert.assertEquals( "CREATE EXTERNAL TABLE T (col0 DOUBLE, col1 INT, col2 BOOLEAN, col3 BIGINT); " +
                        "(note: could not check path, so no PARTITIONED BY information available)",
                SparkExternalTableUtil.getSuggestedSchema(structTypeSample1(), null).toString() );

        StructType s = new StructType();
        s = s.add("col0", DataTypes.DoubleType);
        s = s.add("col3", DataTypes.StringType);

        StructType part = new StructType();
        part = part.add("col1", DataTypes.IntegerType);
        part = part.add("col2", DataTypes.FloatType);
        Assert.assertEquals( "CREATE EXTERNAL TABLE T (col0 DOUBLE, col3 CHAR/VARCHAR(x), col1 INT, col2 REAL) " +
                        "PARTITIONED BY(col1, col2);",
                ExternalTableUtils.getSuggestedSchema(s, part).toString() );

    }

    @Test
    public void testGetCsvOptions() throws StandardException, IOException {
        CsvOptions opt = new CsvOptions("#", "!", "\n");

        String expected = "{lineSep=\n" +
                ", timestampFormat=yyyy-MM-dd'T'HH:mm:ss.SSSZZ, escape=!, sep=#}";
        Assert.assertEquals(expected, SparkExternalTableUtil.getCsvOptions(opt).toString());

        // test serde
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(stream);
        opt.writeExternal(oos);
        oos.flush();

        ByteArrayInputStream in = new ByteArrayInputStream(stream.toByteArray());
        ObjectInputStream ois = new ObjectInputStream(in);
        CsvOptions opt2 = new CsvOptions(ois);

        Assert.assertEquals(expected, SparkExternalTableUtil.getCsvOptions(opt2).toString());
    }
}
