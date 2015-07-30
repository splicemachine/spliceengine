package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;

/**
 *
 *
 *
 */
public class IndexSelectivityIT extends SelectivityIT {
    public static final String CLASS_NAME = IndexSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(),spliceSchemaWatcher.toString());
    }

    @Test
    public void leftInnerJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from t1 inner join t1 foo on t1.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));

        rs = methodWatcher.executeQuery("explain select * from t2 inner join t2 foo on t2.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));

        rs = methodWatcher.executeQuery("explain select * from t3 inner join t3 foo on t3.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));

    }

    @Test
    public void leftInnerJoinNoStats() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from t4 inner join t4 foo on t4.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=16"));

        rs = methodWatcher.executeQuery("explain select * from t5 inner join t5 foo on t5.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));

        rs = methodWatcher.executeQuery("explain select * from t6 inner join t6 foo on t6.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));

    }


    @Test
    public void leftInnerJoinWithStatsAndNoNulls() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from t2 inner join t2 foo on t1.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=5"));
    }

    @Test
    public void leftOuterJoinWithNullsAndStats() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from t1 left outer join t1 foo on t1.c1 = foo.c1");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=8"));
    }

    @Test
    @Ignore("not Working yet")
    public void leftAntiJoinWithNullsAndStats() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("explain select * from t1 where not exists (select * from t1 foo where t1.c1 = foo.c1)");
        rs.next();
        Assert.assertTrue("row count inaccurate " + rs.getString(1),rs.getString(1).contains("outputRows=8"));
    }


}
