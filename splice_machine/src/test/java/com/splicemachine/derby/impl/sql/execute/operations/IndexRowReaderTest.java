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
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category(ArchitectureIndependent.class)
public class IndexRowReaderTest {

    // Checks that the lowest maximum concurrency of an index row reader is 2, even if
    // fewer threads are requested.
    @Test
    public void testReaderConcurrency() throws Exception {
        IndexRowReader irr = new IndexRowReader(
            null,
            null,
            null,
            4000,
            0, // numConcurrentLookup = 0
            0,
            null,
            null,
            null,
            null,
            null,
            null);

        assertTrue("Expected a max concurrency of 2", irr.getMaxConcurrency() == 2);

        irr = new IndexRowReader(
            null,
            null,
            null,
            4000,
            -5000, // numConcurrentLookup = 0
            0,
            null,
            null,
            null,
            null,
            null,
            null);

        assertTrue("Expected a max concurrency of 2", irr.getMaxConcurrency() == 2);
    }
}
