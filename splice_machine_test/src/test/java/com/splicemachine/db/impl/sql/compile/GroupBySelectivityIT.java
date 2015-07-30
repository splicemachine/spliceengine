package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import java.sql.Connection;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class GroupBySelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = GroupBySelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_low_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_low_cardinality values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        conn.commit();
        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();
    }

    @Test
    public void testGroupByCardinality() throws Exception {
        secondRowContainsQuery("explain select count(*), c1,c2 from ts_low_cardinality group by c1,c2", "outputRows=25", methodWatcher);
        secondRowContainsQuery("explain select count(*), c2 from ts_low_cardinality group by c2", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c3 from ts_low_cardinality group by c3", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c4 from ts_low_cardinality group by c4", "outputRows=1", methodWatcher);
    }

    /**
     *
     * Should booleans plan to return 2 rows?
     *
     * @throws Exception
     */
    @Test
    public void testGroupByCardinalityMultiplication() throws Exception {
        secondRowContainsQuery("explain select count(*), c1,c2 from ts_low_cardinality group by c1,c2", "outputRows=25", methodWatcher);
        secondRowContainsQuery("explain select count(*), c1,c3 from ts_low_cardinality group by c1,c3", "outputRows=25", methodWatcher);
        secondRowContainsQuery("explain select count(*), c1,c4 from ts_low_cardinality group by c1,c4", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c4,c2 from ts_low_cardinality group by c4,c2", "outputRows=5", methodWatcher);
    }

    /**
     *
     * Should Selectivity of releation effect the distribution of group by (probably when they are large, not when they are small?)
     *
     * @throws Exception
     */
    @Test
    public void testSelectivityEffectOnGroupBy() throws Exception {
        secondRowContainsQuery("explain select count(*), c1,c2 from ts_low_cardinality where c1 = 1 group by c1,c2", "outputRows=25", methodWatcher);
    }

}