package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForwardQueue;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class RollForwardQueueTest {
    private final int SHORT_DELAY = 50;
    private final int SHORT_WAIT = 2 * SHORT_DELAY;
    private final int LONG_DELAY = 200;
    private final int LONG_WAIT = LONG_DELAY + SHORT_DELAY;
    private final int VERY_LONG_DELAY = 3 * LONG_DELAY;

    @Before
    public void setup() {
        SynchronousRollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
    }

    @Test
    public void basic() throws InterruptedException {
        final byte[] out = new byte[] {0};
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                out[0] = ((byte[]) rowList.get(0))[0];
                return true;
            }
        };
        final RollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(10, out[0]);
    }

    @Test
    public void maxRowExceeded() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                for (Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
                return true;
            }
        };
        final RollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        queue.recordRow(2, new byte[] {20}, false);
        queue.recordRow(3, new byte[] {30}, false);
        queue.recordRow(4, new byte[] {40}, false);
        queue.recordRow(5, new byte[] {50}, false);
        queue.recordRow(6, new byte[] {60}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(4, out.size());
        Assert.assertTrue(out.contains((byte) 10));
        Assert.assertTrue(out.contains((byte) 20));
        Assert.assertTrue(out.contains((byte) 30));
        Assert.assertTrue(out.contains((byte) 40));
    }

    @Test
    public void maxRowExceededSameTransaction() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                for (Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
                return true;
            }
        };
        final RollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        queue.recordRow(1, new byte[] {20}, false);
        queue.recordRow(1, new byte[] {30}, false);
        queue.recordRow(1, new byte[] {40}, false);
        queue.recordRow(1, new byte[] {50}, false);
        queue.recordRow(1, new byte[] {60}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(4, out.size());
        Assert.assertTrue(out.contains((byte) 10));
        Assert.assertTrue(out.contains((byte) 20));
        Assert.assertTrue(out.contains((byte) 30));
        Assert.assertTrue(out.contains((byte) 40));
    }

    @Test
    public void autoReset() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                for(Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
                return true;
            }
        };
        final SynchronousRollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, VERY_LONG_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        queue.recordRow(1, new byte[] {20}, false);
        Assert.assertEquals(2, queue.getCount());
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(2, queue.getCount());
        Thread.sleep(LONG_WAIT);
        Assert.assertEquals(0, queue.getCount());
        Thread.sleep(VERY_LONG_DELAY);
        Assert.assertTrue(out.isEmpty());
    }

    @Test
    public void useBeforeAndAfterReset() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                for(Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
                return true;
            }
        };
        final RollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        queue.recordRow(1, new byte[] {11}, false);
        Thread.sleep(LONG_WAIT);
        queue.recordRow(2, new byte[] {20}, false);
        queue.recordRow(3, new byte[] {30}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(4, out.size());
        Assert.assertTrue(out.contains((byte) 10));
        Assert.assertTrue(out.contains((byte) 11));
        Assert.assertTrue(out.contains((byte) 20));
        Assert.assertTrue(out.contains((byte) 30));
    }

    @Test
    public void rollbackFails() throws InterruptedException {
        final byte[] out = new byte[] {0};
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                if (transactionId == 1) {
                    throw new RuntimeException("fail");
                } else {
                    Assert.assertEquals(1, rowList.size());
                    out[0] = ((byte[]) rowList.get(0))[0];
                    return true;
                }
            }
        };
        final RollForwardQueue queue = new SynchronousRollForwardQueue(action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10}, false);
        queue.recordRow(2, new byte[] {20}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(20, out[0]);
    }

    @Test
    public void autoShutoff() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        final AtomicReference<Boolean> actionSucceeds = new AtomicReference<Boolean>(false);
        RollForwardAction action = new RollForwardAction() {
            @Override
            public Boolean rollForward(long transactionId, List<byte[]> rowList) {
                for(Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
                return actionSucceeds.get();
            }
        };
        final SynchronousRollForwardQueue queue = new SynchronousRollForwardQueue(action, 5, SHORT_DELAY, VERY_LONG_DELAY, "test");

        queue.recordRow(1, new byte[]{10}, false);
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(1, out.size());
        queue.recordRow(1, new byte[]{20}, false);
        queue.recordRow(1, new byte[]{30}, false);
        queue.recordRow(1, new byte[]{40}, false);
        Assert.assertEquals(0, queue.getCount());
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(1, out.size());

        actionSucceeds.set(true);
        queue.recordRow(1, new byte[]{50}, true);
        queue.recordRow(1, new byte[]{60}, true);
        Assert.assertEquals(2, queue.getCount());
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(3, out.size());
    }

}
