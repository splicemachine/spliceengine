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

package com.splice.custom.reader;

import com.splicemachine.spark2.splicemachine.SplicemachineContext;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;


/**
 * Example streaming application that reads from an unbounded socket stream and writes
 * to a Splice Machine table
 *
 * This application works for any platform, on both an open or Kerberized cluster
 *
 * For Kerberos usage, specify the --principal and --keytab options on the spark-submit invocation
 */
public class ReaderWriterExample {

    public static void main(String[] args) throws Exception {

        final String dbUrl = args[0];
        final String hostname = args[1];
        final String port = args[2];
        final String inTargetSchema = args[3];
        final String inTargetTable = args[4];

        SparkConf conf = new SparkConf();

        JavaStreamingContext ssc = new JavaStreamingContext(conf, new Duration(500));

        JavaReceiverInputDStream<String> stream = ssc.socketTextStream(hostname, Integer.parseInt(port));

        SparkSession spark = SparkSession.builder().getOrCreate();

        // Create a SplicemachineContext based on the provided DB connection
        SplicemachineContext splicemachineContext = new SplicemachineContext(dbUrl);

        // Set target tablename and schemaname
        final String table = inTargetSchema + "." + inTargetTable;

        stream.foreachRDD((VoidFunction<JavaRDD<String>>) rdd -> {
            JavaRDD<Row> rowRDD = rdd.map((Function<String, Row>) s -> RowFactory.create(s));
            Dataset<Row> df = spark.createDataFrame(rowRDD, splicemachineContext.getSchema(table));

            splicemachineContext.insert(df, table);
        });

        ssc.start();
        ssc.awaitTermination();
    }
}
