package com.splicemachine.stream;

import com.google.common.net.HostAndPort;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.BaseStreamTest;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.junit.Test;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by dgomezferro on 6/1/16.
 */
public class StreamableRDDTest extends BaseStreamTest implements Serializable {
    private static final Logger LOG = Logger.getLogger(StreamableRDDTest.class);

    @Test
    public void testBasicStream() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = sl.start();
        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(tenRows, 10);
        StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        Object result = srdd.result();
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
    public void testOrder() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> shuffledRows = new ArrayList<>(tenRows);
        Collections.shuffle(shuffledRows);

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(shuffledRows, 10);
        JavaRDD<ExecRow> sorted = rdd.values().sortBy(new Function<ExecRow, Integer>() {
            @Override
            public Integer call(ExecRow execRow) throws Exception {
                return execRow.getColumn(1).getInt();
            }
        }, true, 4);
        StreamableRDD srdd = new StreamableRDD(sorted, hostAndPort.getHostText(), hostAndPort.getPort());
        Object result = srdd.result();
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
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 10000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 6);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
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
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 12);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }

            }
        }.start();
        Iterator<ExecRow> it = sl.getIterator();
        int count = 0;
        while (it.hasNext()) {
            ExecRow execRow = it.next();
//            LOG.trace(execRow);
            count++;
            assertNotNull(execRow);
        }
        assertEquals(100000, count);
    }


    @Test
    public void testBlockingLargeOddPartitions() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>();
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
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
    public void testOffsetLimit() throws StandardException {
        StreamListener<ExecRow> sl = new StreamListener<>(400, 30000);
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
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
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
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
        HostAndPort hostAndPort = sl.start();

        List<Tuple2<ExecRow,ExecRow>> manyRows = new ArrayList<>();
        for(int i = 0; i < 100000; ++i) {
            manyRows.add(new Tuple2<ExecRow, ExecRow>(getExecRow(i, 1), getExecRow(i, 2)));
        }

        JavaPairRDD<ExecRow, ExecRow> rdd = SpliceSpark.getContext().parallelizePairs(manyRows, 13);
        final StreamableRDD srdd = new StreamableRDD(rdd.values(), hostAndPort.getHostText(), hostAndPort.getPort());
        new Thread() {
            @Override
            public void run() {
                try {
                    Object result = srdd.result();
                } catch (StandardException e) {
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
}
