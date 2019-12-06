package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 12/2/19.
 */
@RunWith(Parameterized.class)
public class FullOuterJoinIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(FullOuterJoinIT.class);
    public static final String CLASS_NAME = FullOuterJoinIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{"true", "sortmerge"});
        params.add(new Object[]{"true", "broadcast"});
        params.add(new Object[]{"false", "sortmerge"});
        params.add(new Object[]{"false", "broadcast"});
        return params;
    }

    private static String useSpark;
    private static String joinStrategy;

    public FullOuterJoinIT(String useSpark, String joinStrategy) {
        this.useSpark = useSpark;
        this.joinStrategy = joinStrategy;
    }

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

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int not null, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(3, 30, 3),
                        row(5, 50, null),
                        row(6, 60, 6),
                        row(6, 60, 6),
                        row(7, 70, 7)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void simpleTwoTableFullJoined() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1=c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 30  |  3  |  3  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void twoTableFullJoinedWithBothEqualityAndNonEqualityCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1+1=c2 and a1<a2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  | 20  |  2  |  2  |\n" +
                " 10  |  1  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 30  |  3  |  3  |\n" +
                " 20  |  2  | 30  |  3  |  3  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void twoTableFullJoinedThroughRDDImplementation() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and case when c1=2 then 2 end=c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void fullJoinWithATableWithSingleTableCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join (select * from t2 --splice-properties useSpark=%s\n " +
                "where a2=3) dt --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1 |A1 | B2  | A2  | C2  |\n" +
                "--------------------------\n" +
                "10 | 1 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "30 | 3 | 30  |  3  |  3  |\n" +
                "40 | 4 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void fullJoinWithInEqualityCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1>a2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }catch (SQLException e) {
            Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
        }
    }

    @Test
    public void fullJoinWithInEqualityConditionThroughRDDImplementation() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1>a2 and case when c1=3 then 3 end>c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            Assert.assertTrue("Invalid exception thrown: "+ e, e.getMessage().startsWith("No valid execution plan"));
        }
    }

    @Test
    public void testConversionOfFullJoinWithNoConstraintsToInnerJoin() throws Exception {
        String sqlText = format("explain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on 3=3 \n" +
                "full join t3 on 1=1", useSpark);
        queryDoesNotContainString(sqlText, "Full", methodWatcher);
    }

    @Test
    public void testOnClauseWithSingleTableCondition1() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and b2=20", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition2() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and b1=30", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 30  |  3  |  3  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 20  |  2  |  2  |\n" +
                "NULL |NULL | 20  |  2  |  2  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition3() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 --splice-properties useSpark=%s\n" +
                " full join (select * from t2 where b2=30) dt --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1 |A1 | B2  | A2  | C2  |\n" +
                "--------------------------\n" +
                "10 | 1 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "30 | 3 | 30  |  3  |  3  |\n" +
                "40 | 4 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition4() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from (select * from t1 --splice-properties useSpark=%s\n where b1=30) dt " +
                " full join t2 --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1  | A1  |B2 |A2 | C2  |\n" +
                "--------------------------\n" +
                " 30  |  3  |30 | 3 |  3  |\n" +
                "NULL |NULL |20 | 2 |  2  |\n" +
                "NULL |NULL |20 | 2 |  2  |\n" +
                "NULL |NULL |40 | 4 |NULL |\n" +
                "NULL |NULL |50 | 5 |NULL |\n" +
                "NULL |NULL |60 | 6 |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition5() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 --splice-properties useSpark=%s\n" +
                " full join (select * from t2 where b2=30) dt --splice-properties joinStrategy=%s\n on a1=a2 and c2=4", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testWhereClauseWithSingleTableCondition1() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where b1=30", useSpark, joinStrategy);
        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testWhereClauseWithSingleTableCondition2() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where b2>30", useSpark, joinStrategy);
        String expected = "B1  | A1  |B2 |A2 | C2  |\n" +
                "--------------------------\n" +
                " 40  |  4  |40 | 4 |NULL |\n" +
                "NULL |NULL |50 | 5 |NULL |\n" +
                "NULL |NULL |60 | 6 |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testConsecutiveFullOuterJoins() throws Exception {
        /*
        String sqlText = format("select * from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n" +
                "full join t3 --splice-properties joinStrategy=sortmerge\n " +
                "on a2=a3 on a1=a3", useSpark, joinStrategy);
        */
        String sqlText = format("select * from t1 full join t2 --splice-properties useSpark=%s\n" +
                "full join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a3", useSpark, joinStrategy);
        String expected = "A1  | B1  | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "------------------------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |  7  | 70  |  7  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    @Ignore
    public void testSubqueryInOnClauseOfFullOuterJoin() throws Exception {
        /* subquery is not flattened */
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and a1 in (select a3 from t3)", useSpark, joinStrategy);
    }

    @Test
    @Ignore
    public void testCorrelatedSubqueryWithFullJoin() throws Exception {
        /* test scalar subquery too */
    }
}
