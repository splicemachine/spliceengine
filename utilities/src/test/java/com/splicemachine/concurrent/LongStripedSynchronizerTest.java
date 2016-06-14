package com.splicemachine.concurrent;

import org.sparkproject.guava.collect.Sets;
import org.junit.Test;

import java.util.Set;
import java.util.concurrent.locks.ReadWriteLock;

import static org.junit.Assert.assertEquals;

public class LongStripedSynchronizerTest {

    @Test
    public void stripedReadWriteLock() {
        // given
        LongStripedSynchronizer<ReadWriteLock> striped = LongStripedSynchronizer.stripedReadWriteLock(128, false);

        // when
        Set<ReadWriteLock> locks = Sets.newIdentityHashSet();
        for (long i = 0; i < 2000; i++) {
            ReadWriteLock e = striped.get(i);
            locks.add(e);
        }

        // then
        assertEquals(128, locks.size());
    }

}