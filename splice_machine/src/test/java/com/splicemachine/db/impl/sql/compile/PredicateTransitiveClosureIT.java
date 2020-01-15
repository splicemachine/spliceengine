package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 1/14/20.
 */
public class PredicateTransitiveClosureIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(PredicateTransitiveClosureIT.class);
    public static final String CLASS_NAME = PredicateTransitiveClosureIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int not null, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,2),
                        row(3,30,3),
                        row(4,40,null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int not null, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4,40,null),
                        row(5, 50, null),
                        row(6, 60, 6)))
                .create();


        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testTCForConstantExpression() throws Exception {
        String sqlText = "select b1, a1, b2, a2, c2 from --splice-properties joinOrder=fixed\n" +
                "t1, t2 --splice-properties joinStrategy=broadcast\n" +
                "where a1=a2 and a1=1+2";
        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery("explain " + sqlText);
        String explainString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertTrue(sqlText + " expected to derive a2=1+2", explainString.contains("preds=[(A2[2:1] = (1 + 2))])"));
        rs.close();
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
