
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

package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import io.netty.channel.ChannelHandlerContext;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class TimestampClientTest {
    private static final Logger LOG = Logger.getLogger(TimestampClientTest.class);

    private final TCFactory factory;

    public TimestampClientTest(TCFactory factory) {
        this.factory = factory;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Iterable<Object[]> data() {
        return Arrays.asList(new Object[][]{
                {new DirectTCFactory()},
                {new BatchedTCFactory()}
        });
    }

    @Test
    public void testExceptionDuringConnection() throws TimestampIOException, InterruptedException {
        TimestampServer ts = new TimestampServer(0, new TimestampServerHandler(
                TimestampOracle.getInstance(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 10)));
        ts.startServer();

        int port = ts.getBoundPort();

        TimestampHostProvider hostProvider = Mockito.mock(TimestampHostProvider.class, Mockito.RETURNS_DEEP_STUBS);
        TimestampClient tc = factory.create(1000, hostProvider);

        when(hostProvider.getHost()).thenThrow(new RuntimeException("Failure")).thenReturn("localhost");
        when(hostProvider.getPort()).thenReturn(port);


        // We raise an exception during the first connection, but the next should succeed
        try {
            tc.connectIfNeeded();
            fail("Expected exception");
        } catch (Exception e) {
            //ignore
        }

        // This one should succeed
        tc.connectIfNeeded();

        // We make sure the connection is active
        tc.getNextTimestamp();

        ts.stopServer();
    }


    @Test
    public void testExceptionDoesntLeaveUsedClientIds() throws Exception {

        TimestampOracle oracle = new TimestampOracle(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 1000) {
            // Force an exception the first time a message is received on the server side
            boolean first = true;

            @Override
            public long getNextTimestamp() throws TimestampIOException {
                if (first) {
                    first = false;
                    throw new RuntimeException("First call");
                }
                return super.getNextTimestamp();
            }
        };
        TimestampServerHandler tsh = new TimestampServerHandler(oracle);

        TimestampServer ts = new TimestampServer(0, tsh);
        ts.startServer();

        TimestampClient tc = factory.create(100000, getProvider(ts));

        try {
            tc.getNextTimestamp();
            fail("Expected exception");
        } catch (Exception e) {
            // expected
            e.printStackTrace();
        }

        // Make sure we use all client ids and wrap around (64K)
        for (int i = 0; i < 80000; ++i) {
            tc.getNextTimestamp();
        }

    }

    private TimestampHostProvider getProvider(TimestampServer ts) {
        int port = ts.getBoundPort();
        return new TimestampHostProvider() {
            @Override
            public String getHost() {
                return "localhost";
            }

            @Override
            public int getPort() {
                return port;
            }
        };
    }

    @Test
    public void testTimeout() throws TimestampIOException, InterruptedException {
        TimestampServer ts = new TimestampServer(0, new TimestampServerHandler(
                TimestampOracle.getInstance(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 10000)) {
            @Override
            protected void channelRead0(ChannelHandlerContext ctx, TimestampMessage.TimestampRequest request) throws Exception {
                Thread.sleep(20);
                super.channelRead0(ctx, request);
            }
        });
        try {
            ts.startServer();

            TimestampClient tc = factory.create(5, getProvider(ts));

            tc.getNextTimestamp();
            fail("Expected exception");
        } catch (TimestampIOException e) {
            assertThat(e.getCause(), is(instanceOf(TimeoutException.class)));
        } finally {
            ts.stopServer();
        }
    }

    @Test
    public void testConcurrentTimestampsAreUnique() throws InterruptedException, TimestampIOException {
        int nCalls = 200000;
        int nThreads = 200;

        final Map<Long, Boolean> usedTimestamps = new ConcurrentHashMap((int) (nCalls * 1.20), 0.9f, nThreads);
        TimestampServer ts = new TimestampServer(0, new TimestampServerHandler(
                TimestampOracle.getInstance(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 10000)));
        ts.startServer();
        TimestampHostProvider hostProvider = getProvider(ts);
        TimestampClient tc = factory.create(1000, hostProvider);

        final int callsPerThread = nCalls / nThreads;
        final AtomicInteger total = new AtomicInteger();

        Thread[] threads = new Thread[nThreads];

        total.set(0);
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new Thread(() -> run(tc, usedTimestamps, callsPerThread));
        }

        for (Thread thread : threads) {
            thread.start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals("Generated timestamps don't match", nCalls, usedTimestamps.size());
    }

    static void run(TimestampClient tc, Map<Long, Boolean> used, int n) {
        try {
            for (int i = 0; i < n; ++i) {
                long ts = tc.getNextTimestamp();
                Boolean result = used.put(ts, true);

                if (result != null) {
                    LOG.error("Timestamp was already used: " + ts);
                    break;
                }
            }
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

}

interface TCFactory {
    TimestampClient create(int timeoutMillis, TimestampHostProvider timestampHostProvider);
}

class DirectTCFactory implements TCFactory {
    @Override
    public TimestampClient create(int timeoutMillis, TimestampHostProvider timestampHostProvider) {
        return new TimestampClient(timeoutMillis, timestampHostProvider);
    }

    @Override
    public String toString() {
        return "direct";
    }
}

class BatchedTCFactory implements TCFactory {
    @Override
    public TimestampClient create(int timeoutMillis, TimestampHostProvider timestampHostProvider) {
        BatchedTimestampClient tc = new BatchedTimestampClient(timeoutMillis, timestampHostProvider, 10);
        tc.start();
        return tc;
    }

    @Override
    public String toString() {
        return "batched";
    }
}
