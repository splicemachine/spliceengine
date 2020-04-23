package com.splicemachine.timestamp.hbase;

import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.*;

public class ZKTimestampBlockManagerTest {

    @Test
    public void testReserveNextBlock() throws Exception {
        long ts = 10000;
        final byte[] timestamp = Bytes.toBytes(ts);
        // Mock a znode that stores 10000 as a timestamp
        AtomicReference<byte[]> timestampRef = new AtomicReference<>(timestamp);
        RecoverableZooKeeper recoverableZooKeeper = Mockito.mock(RecoverableZooKeeper.class);
        ZkTimestampBlockManager blockManager = new ZkTimestampBlockManager(recoverableZooKeeper,
                "/splice/transactions/maxReservedTimestamp");
        // Mock setting timestamp on zookeeper
        doAnswer(invocation -> {
            Object[] args = invocation.getArguments();
            byte[] data = (byte[])args[1];
            timestampRef.set(data);
            return null;
        }).when(recoverableZooKeeper).setData(any(String.class), any(byte[].class), any(Integer.class));

        // Mock getting timestamp from zookeeper
        when(recoverableZooKeeper.getData(any(String.class), any(Boolean.class), any(Stat.class))).thenReturn(timestampRef.get());

        // mock the case when zookeeper does not update its timestamp values
        long nextTimestamp = blockManager.reserveNextBlock(100);
        Assert.assertEquals(nextTimestamp, ts);
        Assert.assertEquals(Bytes.toLong(timestampRef.get()), ts);

        // mock the case when zookeeper updates timestamp values
        nextTimestamp = blockManager.reserveNextBlock(10000000);
        Assert.assertEquals(nextTimestamp, 10000000);
        Assert.assertEquals(Bytes.toLong(timestampRef.get()), 10000000);
    }
}
