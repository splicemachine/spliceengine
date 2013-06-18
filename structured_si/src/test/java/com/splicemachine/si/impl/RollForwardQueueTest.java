package com.splicemachine.si.impl;

import com.splicemachine.si.NoOpHasher;
import junit.extensions.TestSetup;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;

public class RollForwardQueueTest {
    private final int SHORT_DELAY = 50;
    private final int SHORT_WAIT = 2 * SHORT_DELAY;
    private final int LONG_DELAY = 200;
    private final int LONG_WAIT = LONG_DELAY + SHORT_DELAY;
    private final int VERY_LONG_DELAY = 3 * LONG_DELAY;

    @Before
    public void setup() {
        RollForwardQueue.scheduler = Executors.newScheduledThreadPool(1);
    }

    @Test
    public void basic() throws InterruptedException {
        final byte[] out = new byte[] {0};
        RollForwardAction action = new RollForwardAction() {
            @Override
            public void rollForward(long transactionId, List rowList) {
                out[0] = ((byte[]) rowList.get(0))[0];
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(10, out[0]);
    }

    @Test
    public void maxRowExceeded() throws InterruptedException {
        final Set<Byte> out = new HashSet<Byte>();
        RollForwardAction action = new RollForwardAction() {
            @Override
            public void rollForward(long transactionId, List rowList) {
                for (Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        queue.recordRow(2, new byte[] {20});
        queue.recordRow(3, new byte[] {30});
        queue.recordRow(4, new byte[] {40});
        queue.recordRow(5, new byte[] {50});
        queue.recordRow(6, new byte[] {60});
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
            public void rollForward(long transactionId, List rowList) {
                for (Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        queue.recordRow(1, new byte[] {20});
        queue.recordRow(1, new byte[] {30});
        queue.recordRow(1, new byte[] {40});
        queue.recordRow(1, new byte[] {50});
        queue.recordRow(1, new byte[] {60});
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
            public void rollForward(long transactionId, List rowList) {
                for(Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, VERY_LONG_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        queue.recordRow(1, new byte[] {20});
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
            public void rollForward(long transactionId, List rowList) {
                for(Object row : rowList) {
                    out.add(((byte[]) row)[0]);
                }
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        queue.recordRow(1, new byte[] {11});
        Thread.sleep(LONG_WAIT);
        queue.recordRow(2, new byte[] {20});
        queue.recordRow(3, new byte[] {30});
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
            public void rollForward(long transactionId, List rowList) {
                if (transactionId == 1) {
                    throw new RuntimeException("fail");
                } else {
                    Assert.assertEquals(1, rowList.size());
                    out[0] = ((byte[]) rowList.get(0))[0];
                }
            }
        };
        final RollForwardQueue queue = new RollForwardQueue(new NoOpHasher(), action, 4, SHORT_DELAY, LONG_DELAY, "test");
        queue.recordRow(1, new byte[] {10});
        queue.recordRow(2, new byte[] {20});
        Thread.sleep(SHORT_WAIT);
        Assert.assertEquals(20, out[0]);
    }
}
