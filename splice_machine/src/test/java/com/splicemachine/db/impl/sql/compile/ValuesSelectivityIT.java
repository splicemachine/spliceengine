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
