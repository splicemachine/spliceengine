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
 *
 */

package com.splicemachine.stream;

import com.google.common.net.HostAndPort;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.BaseStreamTest;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDDTest_FailuresParallellism extends BaseStreamTest implements Serializable {
    private static final Logger LOG = Logger.getLogger(StreamableRDDTest_Failures.class);

    private static StreamListenerServer server;

    @BeforeClass
    public static void setup() throws StandardException {
        System.setProperty("splice.spark.master", "local[200,4]");
        server = new StreamListenerServer(0);
        server.start();
    }

    @AfterClass
    public static void shutdown() {
        server.stop();
    }

    @Before
    public void reset() {
        FailsFunction.reset();
        FailsTwiceFunction.reset();
    }

    @Test
    public void testEarlyFailureManyPartitions() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 200).mapToPair(new FailsFunction(200));;
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        int first = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(100000, count);
    }

    @Test
    public void testMultipleFailuresManyPartitions() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow, ExecRow>> manyRows = new ArrayList<>();
        for (int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 200)
                .mapToPair(new FailsTwiceFunction(200, 200));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        int first = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count + first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(100000, count);
    }

    @Test
    public void testFailureDuringRecoveryWarmup() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        int timeout = 5;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsTwiceFunction(10000, 100));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize, timeout);
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count, execRow.getInt(0));
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void testFailureAfterRecoveryWarmup() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        int timeout = 5;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsTwiceFunction(10000, 2000));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize, timeout);
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count, execRow.getInt(0));
            count++;
        }
        assertEquals(size, count);
    }


    @Test
    public void testPersistentFailureWithOffset() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        int timeout = 5;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 10, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsForeverFunction(0));
        final StreamableRDD srdd = new StreamableRDD(rdd.distinct().values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize, timeout);
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    sl.failed(e);
                    throw new RuntimeException(e);
                }

            }
        }.start();
        // This call shoul not raise an exception even though the Spark job fails
        Iterator<ExecRow> it = sl.getIterator();

        try {
            it.hasNext();
            fail("Should have raised exception");
        } catch (Exception e) {
            //expected exception
        }
    }

}
