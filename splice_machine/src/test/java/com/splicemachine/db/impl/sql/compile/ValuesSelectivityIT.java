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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Rule;
import org.junit.Test;

/**
 * Created by jleach on 8/31/15.
 */
public class ValuesSelectivityIT extends SpliceUnitTest {
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher("SPLICE");

    @Test
    public void testValuesContainsCostRowsAndHeap() throws Exception {
        rowContainsQuery(2,"explain values (1,1,1),(2,2,2),(3,3,3)","totalCost=0.003,outputRows=3,outputHeapSize=12 B,partitions=1",methodWatcher);
    }
}
