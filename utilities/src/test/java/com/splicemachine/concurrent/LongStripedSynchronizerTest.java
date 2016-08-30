/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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