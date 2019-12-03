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
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private static String useSpark;

    public FullOuterJoinIT(String useSpark) {
        this.useSpark = useSpark;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,2),
                        row(3,30,3),
                        row(4,40,null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int)")
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
    public void simpleTwoTableFullJoined() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);
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
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on c1+1=c2 and a1<a2", useSpark);
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
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2 and case when c1=2 then 2 end=c2", useSpark);
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
                "where a2=3) dt on a1=a2", useSpark);
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

}
