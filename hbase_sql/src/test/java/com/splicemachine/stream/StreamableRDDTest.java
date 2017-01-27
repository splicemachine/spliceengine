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
import org.junit.BeforeClass;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDDTest extends BaseStreamTest implements Serializable {
    private static final Logger LOG = Logger.getLogger(StreamableRDDTest.class);

    private static StreamListenerServer server;

    @BeforeClass
    public static void setup() throws StandardException {
        server = new StreamListenerServer(0);
        server.start();
    }

    @Test
    public void testBasicStream() throws Exception {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);
        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(tenRows, 10);
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
    public void testOrder() throws Exception {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> shuffledRows = new ArrayList<>(tenRows);
        Collections.shuffle(shuffledRows);

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(shuffledRows, 10);
        JavaRDD<ExecRow> sorted = rdd.values().sortBy(new Function<ExecRow, Integer>() {
            @Override
            public Integer call(ExecRow execRow) throws Exception {
                return execRow.getColumn(1).getInt();
            }
        }, true, 4);
        StreamableRDD srdd = new StreamableRDD(sorted, sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        srdd.submit();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        int last = -1;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            LOG.trace(execRow);
            count++;
            assertNotNull(execRow);
            int value = execRow.getColumn(1).getInt();
            assertTrue("Results not in order", value > last);
            last = value;
        }
        assertEquals(10, count);
    }

    @Test
    public void testBlocking() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 10000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 6);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), sl.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    srdd.submit();
                } catch (Exception e) {
                    LOG.error(e);
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            count++;
            assertNotNull(execRow);
        }
        assertEquals(10000, count);
    }

    @Test
    public void testBlockingLarge() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 12);
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
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            count++;
            assertNotNull(execRow);
        }
        assertEquals(100000, count);
    }


    @Test
    public void testBlockingLargeOddPartitions() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
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
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            count++;
            assertNotNull(execRow);
        }
        assertEquals(100000, count);
    }


    @Test
    public void testSmallOffsetLimit() throws StandardException {
        int limit = 100;
        int offset = 2000;
        int total = 4000;
        StreamListener<ExecRow> sl = new StreamListener<>(limit, offset);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < total; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 1);
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
        int first = offset;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(limit, count);
    }

    @Test
    public void testSmallLimit() throws StandardException {
        int limit = 2000;
        int offset = 0;
        int total = 4000;
        int batches = 2;
        int batchSize = 512;
        StreamListener<ExecRow> sl = new StreamListener<>(limit, offset, batches, batchSize);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < total; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 1);
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
        int first = offset;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(limit, count);
    }


    @Test
    public void testOffsetLimit() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(400, 30000);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
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
    public void testLimit() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(400, 0);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
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
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(400, count);
    }


    @Test
    public void testOffset() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(-1, 60000);
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
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
        int first = 60000;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
            assertNotNull(execRow);
            assertEquals(count+first, execRow.getColumn(1).getInt());
            count++;
        }
        assertEquals(100000-60000, count);
    }


    @Test
    public void testConcurrentQueries() throws StandardException, ExecutionException, InterruptedException {
        final StreamListener<ExecRow> sl1 = new StreamListener<>();
        final StreamListener<ExecRow> sl2 = new StreamListener<>();
        final StreamListener<ExecRow> sl3 = new StreamListener<>();
        HostAndPort hostAndPort = server.getHostAndPort();
        server.register(sl1);
        server.register(sl2);
        server.register(sl3);

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 12);
        final StreamableRDD srdd1 = new StreamableRDD(rdd.values(), sl1.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        final StreamableRDD srdd2 = new StreamableRDD(rdd.values().map(new Function<ExecRow,ExecRow>() {
            @Override
            public ExecRow call(ExecRow o) throws Exception {
                o.getColumn(1).setValue(0);
                return o;
            }
        }), sl2.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        final StreamableRDD srdd3 = new StreamableRDD(rdd.values(), sl3.getUuid(), hostAndPort.getHostText(), hostAndPort.getPort());
        for (final StreamableRDD srdd : Arrays.asList(srdd1, srdd2, srdd3)) {
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
        }
        // We collect them asynchronously into memory so we are able to iterate over them at the same time. Otherwise
        // tasks for the third RDD might be blocked by tasks in other RDDs, and we are not consuming elements from the
        // other iterators so they can become unblocked.
        ExecutorService executor = Executors.newFixedThreadPool(3);
        Future<List<ExecRow>> future1 = executor.submit(new Callable<List<ExecRow>>() {
            @Override
            public List<ExecRow> call() throws Exception {
                return IteratorUtils.toList(sl1.getIterator());
            }
        });
        Future<List<ExecRow>> future2 = executor.submit(new Callable<List<ExecRow>>() {
            @Override
            public List<ExecRow> call() throws Exception {
                return IteratorUtils.toList(sl2.getIterator());
            }
        });
        Future<List<ExecRow>> future3 = executor.submit(new Callable<List<ExecRow>>() {
            @Override
            public List<ExecRow> call() throws Exception {
                return IteratorUtils.toList(sl3.getIterator());
            }
        });
        Iterator<ExecRow> it1 = future1.get().iterator();
        Iterator<ExecRow> it2 = future2.get().iterator();
        Iterator<ExecRow> it3 = future3.get().iterator();
        int count = 0;
        while (it1.hasNext()) {
            ExecRow r1 = it1.next();
            ExecRow r2 = it2.next();
            ExecRow r3 = it3.next();
            count++;
            assertNotNull(r1);
            assertNotNull(r2);
            assertNotNull(r3);
            assertEquals(0, r2.getColumn(1).getInt());
            assertEquals(r1.getColumn(1), r3.getColumn(1));
            assertEquals(r1.getColumn(2), r2.getColumn(2));
        }
        assertEquals(100000, count);
    }
}
