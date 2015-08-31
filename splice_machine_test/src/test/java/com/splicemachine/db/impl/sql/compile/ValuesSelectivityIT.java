package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

/**
 * Created by jleach on 8/31/15.
 */
public class ValuesSelectivityIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(ValuesSelectivityIT.class);
    public static final String CLASS_NAME = ValuesSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void testValuesContainsCostRowsAndHeap() throws Exception {
        rowContainsQuery(2,"explain values (1,1,1),(2,2,2),(3,3,3)","totalCost=0.003,outputRows=3,outputHeapSize=12 B,partitions=1",methodWatcher);
    }

}
