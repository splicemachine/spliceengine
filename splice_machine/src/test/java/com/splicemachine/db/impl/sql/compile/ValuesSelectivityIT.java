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
