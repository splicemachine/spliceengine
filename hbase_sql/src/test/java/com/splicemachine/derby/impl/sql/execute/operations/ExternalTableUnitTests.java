package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.stream.spark.SparkExternalTableUtil;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static java.util.stream.Collectors.toList;

public class ExternalTableUnitTests {

    // todo: add error test for
    // "hdfs://host:123/partition_test/web_sales5/ws_sold_date_sk=2450817/c=3.14/part-01434.c000.snappy.parquet",
    // "hdfs://host:123/partition_test/web_sales5/ws_sold_date_sk=2450817/c=3.14/part-01434.c000.snappy.parquet",
    // -> Conflicting partition column names detected
    // todo: add test for more types (date, decimal, string etc.)
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
                files, true, basePaths, null,
                null );
        Assert.assertEquals(ps.partitions().size(), 4);
        Assert.assertEquals(ps.partitionColumns().fields().length, 2);
        Assert.assertEquals(ps.partitionColumns().fields()[0].toString(), "StructField(c,DoubleType,true)");
        Assert.assertEquals(ps.partitionColumns().fields()[1].toString(), "StructField(ws_sold_date_sk,IntegerType,true)");
        Assert.assertEquals(ps.partitions().toList().last().toString(),
                "PartitionPath([3.14,2450818]," + root + "c=3.14/ws_sold_date_sk=2450818)");
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
                files, true, basePaths, s,
                null );
        Assert.assertEquals(1, ps.partitions().size(),1);
        Assert.assertEquals(2, ps.partitionColumns().fields().length);
        Assert.assertEquals("StructField(c,StringType,true)", ps.partitionColumns().fields()[0].toString());
        Assert.assertEquals("StructField(ws_sold_date_sk,DoubleType,true)", ps.partitionColumns().fields()[1].toString());
        Assert.assertEquals("PartitionPath([3.14,2450818.0]," + root + "c=3.14/ws_sold_date_sk=2450818)",
                ps.partitions().toList().last().toString());
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

        Assert.assertEquals(1, ps.partitions().size());
        Assert.assertEquals(2, ps.partitionColumns().fields().length);
        Assert.assertEquals("StructField(c,DoubleType,true)", ps.partitionColumns().fields()[0].toString());
        Assert.assertEquals("StructField(ws_sold_date_sk,DoubleType,true)", ps.partitionColumns().fields()[1].toString());
        Assert.assertEquals("PartitionPath([3.14,null]," + root + "c=3.14/ws_sold_date_sk=__HIVE_DEFAULT_PARTITION__)",
                ps.partitions().toList().last().toString());
    }
}
