package com.splicemachine.derby.broadcast;

import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by dgomezferro on 11/12/13.
 */
public class ZookeeperBroadcastIT {
    private static final Logger LOG = Logger.getLogger(ZookeeperBroadcastIT.class);
    static final byte[] TEST_MESSAGE = new byte[] {(byte) 0xde, (byte) 0xad, (byte) 0xbe, (byte) 0xef};

    private class TestHandler implements MessageHandler {
        AtomicInteger count = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        @Override
        public void handleMessage(byte[] message) throws StandardException {
            if (Arrays.equals(message, TEST_MESSAGE)) {
                count.incrementAndGet();
            } else {
                failures.incrementAndGet();
            }
        }
    }

    @Test
    public void simpleBroadcast() throws StandardException {
        Map<String, MessageHandler> handlers = new HashMap();
        TestHandler handler = new TestHandler();
        handlers.put("test", handler);
        ZookeeperBroadcast zb1 = new ZookeeperBroadcast(handlers);
        ZookeeperBroadcast zb2 = new ZookeeperBroadcast(handlers);
        ZookeeperBroadcast zb3 = new ZookeeperBroadcast(handlers);
        zb1.start(); zb2.start(); zb3.start();
        zb1.broadcastMessage("test", TEST_MESSAGE);

        Assert.assertEquals("Corrupted messages", 0, handler.failures.get());
        Assert.assertEquals("Messages not received", 3, handler.count.get());

        zb1.broadcastMessage("test", new byte[] {2,3,4});

        Assert.assertEquals("Invalid messages not received", 3, handler.failures.get());

        zb1.stop(); zb2.stop(); zb3.stop();
    }

    @Test
    public void messagesRemovedAfterAcknowledgment() throws StandardException {
        Map<String, MessageHandler> handlers = new HashMap();
        TestHandler handler = new TestHandler();
        handlers.put("test", handler);
        ZookeeperBroadcast zb1 = new ZookeeperBroadcast(handlers);
        ZookeeperBroadcast zb2 = new ZookeeperBroadcast(handlers);
        ZookeeperBroadcast zb3 = new ZookeeperBroadcast(handlers);
        // Start 1 & 2 only
        zb1.start(); zb2.start();
        try {
            zb1.broadcastMessage("test", TEST_MESSAGE);

            Assert.assertEquals("Corrupted messages", 0, handler.failures.get());
            Assert.assertEquals("Messages not received", 2, handler.count.get());

            zb3.start();
            Assert.assertEquals("Corrupted messages", 0, handler.failures.get());
            Assert.assertEquals("New server shouldn't receive the message", 2, handler.count.get());
        } finally {
            zb1.stop(); zb2.stop(); zb3.stop();
        }
    }

    @Test
    public void newServerDuringBroadcastReceivesMessage() throws StandardException {
        // Here we start one 'server' and schedule a second one to start as soon as a message is received by the first server
        // Since the message is not acknowledged when the second server starts, it has to receive the message as well
        final AtomicInteger count = new AtomicInteger();
        final AtomicBoolean failure = new AtomicBoolean(false);
        final CountDownLatch finishLatch = new CountDownLatch(1);
        final CountDownLatch launchServerLatch = new CountDownLatch(1);
        Map<String, MessageHandler> handlers = new HashMap();
        MessageHandler blockingHandler = new MessageHandler() {
            @Override
            public void handleMessage(byte[] message) throws StandardException {
                count.incrementAndGet();
                launchServerLatch.countDown();
                try {
                    finishLatch.await();
                } catch (InterruptedException e) {
                    LOG.error("Unexpected exception", e);
                    failure.set(true);
                }
            }
        };
        handlers.put("test", blockingHandler);
        ZookeeperBroadcast zb1 = new ZookeeperBroadcast(handlers);
        zb1.start();

        // Second server is started after the message has been relied to the first server
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    launchServerLatch.await();
                    Map<String, MessageHandler> handlers = new HashMap();
                    handlers.put("test", new MessageHandler() {
                        @Override
                        public void handleMessage(byte[] message) throws StandardException {
                            count.incrementAndGet();
                        }
                    });
                    ZookeeperBroadcast zb2 = new ZookeeperBroadcast(handlers);
                    try {
                        zb2.start();
                        finishLatch.countDown();
                    } finally {
                        zb2.stop();
                    }
                } catch (Exception e) {
                    LOG.error("Unexpected exception", e);
                    failure.set(true);
                }
            }
        });
        thread.start();
        try {
            zb1.broadcastMessage("test", TEST_MESSAGE);

            Assert.assertEquals("Messages not received", 2, count.get());
        } finally {
            zb1.stop();
        }
    }
}
