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

package com.splicemachine.stream;

import com.google.common.net.HostAndPort;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.BaseStreamTest;
import org.apache.commons.collections.IteratorUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDDTest_Failures extends BaseStreamTest implements Serializable {
    private static final Logger LOG = Logger.getLogger(StreamableRDDTest_Failures.class);

    private static StreamListenerServer server;

    @BeforeClass
    public static void setup() throws StandardException {
        server = new StreamListenerServer(0);
        server.start();
    }

    @BeforeClass
    public static void startSpark() {
        SpliceSpark.getContextUnsafe();
    }

    @AfterClass
    public static void stopSpark() {
        SpliceSpark.getContextUnsafe().stop();
    }

    @Before
    public void reset() {
        FailsFunction.reset();
        FailsTwiceFunction.reset();
    }

    @Test
    public void testBasicStream() throws Exception {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);
        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(tenRows, 2).mapToPair(new FailsFunction(3));
        StreamableRDD srdd = new StreamableRDD(rdd.values(), sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        srdd.submit();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            LOG.trace(execRow);
            count++;
            assertNotNull(execRow);
            assertTrue(execRow.getColumn(1).getInt() < 10);
        }
        assertEquals(10, count);
    }

    @Test
    public void testFailureBoundary() throws Exception {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);
        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(tenRows, 20).mapToPair(new FailsFunction(4));
        StreamableRDD srdd = new StreamableRDD(rdd.values(), sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        srdd.submit();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            LOG.trace(execRow);
            count++;
            assertNotNull(execRow);
            assertTrue(execRow.getColumn(1).getInt() < 10);
        }
        assertEquals(10, count);
    }

    @Test
    public void testBlockingMedium() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 20000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsFunction(5000));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize);
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
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void testBlockingLarge() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 12).sortByKey().mapToPair(new FailsFunction(10000));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize);
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
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void testFailureBeforeLargeOffset() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(400, 30000);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 13).mapToPair(new FailsFunction(29500));;
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
        int first = 30000;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(400, count);
    }

    @Test
    public void testFailureBeforeOffset() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(40000, 300);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 13).mapToPair(new FailsFunction(200));;
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
        int first = 300;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(40000, count);
    }


    @Test
    public void testFailureAfterOffset() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(40000, 300);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 13).mapToPair(new FailsFunction(14000));;
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
        int first = 300;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(40000, count);
    }

    @Test
    public void testFailureAfterLimit() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(40000, 300);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 13).mapToPair(new FailsFunction(40301));;
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
        int first = 300;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(40000, count);
    }



    @Test
    public void testFailureDuringRecoveryWarmup() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsTwiceFunction(10000, 100));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize);
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
            count++;
        }
        assertEquals(size, count);
    }

    @Test
    public void testFailureAfterRecoveryWarmup() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 0, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsTwiceFunction(10000, 2000));
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize);
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
            count++;
        }
        assertEquals(size, count);
    }


    @Test
    public void testPersistentFailureWithOffset() throws StandardException, FileNotFoundException, UnsupportedEncodingException {
        int size = 100000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 10, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < size; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContextUnsafe().parallelizePairs(manyRows, 2).sortByKey().mapToPair(new FailsForeverFunction(0));
        final StreamableRDD srdd = new StreamableRDD(rdd.distinct().values(), null, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort(), batches, batchSize);
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

class FailsFunction implements PairFunction<Tuple2<ExecRow, ExecRow>, ExecRow, ExecRow> {
    static AtomicBoolean failed = new AtomicBoolean(false);
    final long toFail;

    public FailsFunction(long toFail) {
        this.toFail = toFail;
    }

    @Override
    public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, ExecRow> element) throws Exception {

        if (element._1().getColumn(1).getInt() == toFail && !failed.get()) {
            failed.set(true);
            throw new RuntimeException("Failure");
        }
        return element;
    }

    public static void reset() {
        failed.set(false);
    }
}


class FailsTwiceFunction implements PairFunction<Tuple2<ExecRow, ExecRow>, ExecRow, ExecRow> {
    static AtomicBoolean failed = new AtomicBoolean(false);
    static AtomicBoolean failed2 = new AtomicBoolean(false);
    final long toFail;
    final long toFail2;

    public FailsTwiceFunction(long toFail, long toFail2) {
        this.toFail = toFail;
        this.toFail2 = toFail2;
    }

    @Override
    public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, ExecRow> element) throws Exception {
        if (!failed.get()) {
            if (element._1().getColumn(1).getInt() == toFail) {
                failed.set(true);
                throw new RuntimeException("First Failure");
            }
        } else if (!failed2.get()) {
            if (element._1().getColumn(1).getInt() == toFail2) {
                failed2.set(true);
                throw new RuntimeException("Second Failure");
            }
        }

        return element;
    }

    public static void reset() {
        failed.set(false);
        failed2.set(false);
    }
}

class FailsForeverFunction implements PairFunction<Tuple2<ExecRow, ExecRow>, ExecRow, ExecRow> {
    final long toFail;

    public FailsForeverFunction(long toFail) {
        this.toFail = toFail;
    }

    @Override
    public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, ExecRow> element) throws Exception {

        if (element._1().getColumn(1).getInt() == toFail) {
            throw new RuntimeException("Failure");
        }
        return element;
    }

}
