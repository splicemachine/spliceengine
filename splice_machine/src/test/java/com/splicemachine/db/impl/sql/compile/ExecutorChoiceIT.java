package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

public class ExecutorChoiceIT extends SpliceUnitTest {
    public static final String CLASS_NAME = DecimalIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {
        new TableCreator(conn)
                .withCreate("create table if not exists t1(cid char(36), cs date, l double, lz double)")
                .create();

        new TableCreator(conn)
                .withCreate("create table if not exists t2(wed date)")
                .create();

        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'T1', 5914, 13, 1)", schemaName));
        spliceClassWatcher.executeQuery(format(
                "call SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'T2', 143, 4, 1)", schemaName));

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testSimpleCrossProduct1() throws Exception {
        String query = "explain select * from t1, t2";
        firstRowContainsQuery(query, "engine=OLAP (cost)", methodWatcher);
    }

    @Test
    public void testSimpleCrossProduct2() throws Exception {
        String query = "explain select * from t1 join t2 on 1=1";
        firstRowContainsQuery(query, "engine=OLAP (cost)", methodWatcher);
    }

    @Test
    public void testCrossProductDT() throws Exception {
        String query = "explain select * from --splice-properties joinOrder=fixed\n" +
                       " t1, (select distinct wed from t2)";
        firstRowContainsQuery(query, "engine=OLAP (cost)", methodWatcher);
    }
}
