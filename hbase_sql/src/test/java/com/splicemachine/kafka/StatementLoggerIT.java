/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.kafka;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import com.splicemachine.utils.Pair;
import kafka.server.KafkaConfig;

import kafka.server.KafkaServerStartable;
import kafka.utils.TestUtils;
import org.apache.kafka.common.protocol.SecurityProtocol;
import org.junit.*;
import scala.Option;

import java.io.File;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import org.apache.spark.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.*;
import org.apache.spark.streaming.api.java.*;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.regex.Pattern;
import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.KafkaUtils;

import java.util.*;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.*;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import scala.Tuple2;
/**
 * Created by mzweben on 7/17/17.
 */
public class StatementLoggerIT extends SpliceUnitTest
{
    private static final String SCHEMA = StatementLoggerIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);
    private static final int numThreads = 3;
    private static final String consumerGroup = "StatementLogger";
    private static final String topic = "myPrivateCluster";
    private static final String zkQuorum = "localhost:2181";
    private static final Map<String, Object> kafkaParams = new HashMap<>();
    private static final SparkConf sparkConf = new SparkConf().setAppName("StatementLogger");
    // Create the context with 2 seconds batch size
    private static JavaStreamingContext jssc = null;
    private static final String[] topics = {topic};

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {
        TestConnection conn = classWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table Logger (message varchar(2048))")
                .create();
    }

    @BeforeClass
    public static void defineKafkaQueue() throws Exception {
        kafkaParams.put("bootstrap.servers", "localhost:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "StatementLoggerGroup");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        jssc = new JavaStreamingContext(sparkConf, new Duration(2000));
    }

    @AfterClass
    public static void after() throws Exception
    {
        TestConnection conn = classWatcher.getOrCreateConnection();
        PreparedStatement ps = conn.prepareStatement("select count(*) from Logger");
        ResultSet rs = ps.executeQuery();
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));
        jssc.stop();
    }

    @Test
    public void testKafkaQueue() throws Exception {
        methodWatcher.execute("create table t (a int, b int, c int)");
        ResultSet rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();
        Assert.assertEquals(0, rs.getInt(1));

        methodWatcher.execute("insert into t values ((1,2,3),(10,20,30),(100,200,300)");
        rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();

        methodWatcher.execute("insert into t select * from t");

        // Insert Logger Queue into Table
        insertFromQueue();

        rs = methodWatcher.executeQuery("select count(*) from Logger");
        rs.next();
        Assert.assertEquals(6, rs.getInt(1));
    }


    /**
     * To run this example:
     * `$ bin/run-example org.apache.spark.examples.streaming.JavaKafkaWordCount zoo01,zoo02, \
     * zoo03 my-consumer-group topic1,topic2 1`
     */
    private void insertFromQueue() throws Exception {
        JavaInputDStream<ConsumerRecord<String,String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String ,String>Subscribe(Arrays.asList(topics), kafkaParams)
                );

        JavaPairDStream<String, String> lines = stream
                .mapToPair(new PairFunction<ConsumerRecord<String, String>, String, String>() {
                    @Override
                    public Tuple2<String, String> call(ConsumerRecord<String, String> record) {

                        return new Tuple2<>(record.key(), record.value());
                    }
                });

        lines.foreachRDD(rdd -> {
                    rdd.map( pair -> insertRecord(pair)
                    );
        });

        jssc.start();
        try {
            jssc.awaitTerminationOrTimeout(10000);
        }
        catch (Exception e){
            throw e;
        }
    }

    public String insertRecord(Tuple2<String,String> record){
        try {
            System.out.println("record="+record._1+","+record._2);
            methodWatcher.execute("insert into Logger values (" + record._2() + ")");
            ResultSet rs = methodWatcher.executeQuery("select count(*) from t");
            rs.next();
        }
        catch(Exception e){
        };

        return null;
    }

}

