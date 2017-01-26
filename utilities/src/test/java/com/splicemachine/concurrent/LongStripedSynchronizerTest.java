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

package com.splicemachine.concurrent;

import org.spark_project.guava.collect.Sets;
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