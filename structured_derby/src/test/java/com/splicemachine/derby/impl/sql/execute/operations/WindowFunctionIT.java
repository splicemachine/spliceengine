package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceXPlainTrace;
import com.splicemachine.homeless.TestUtils;

/**
 * Created by jyuan on 7/30/14.
 */
public class WindowFunctionIT extends SpliceUnitTest {
    public static final String CLASS_NAME = WindowFunctionIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(empnum int, dept int, salary int)";
    public static final String TABLE_NAME = "EMPTAB";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    private static String[] EMPTAB_ROWS = {
        "20,1,75000",
        "70,1,76000",
        "60,1,78000",
        "110,1,53000",
        "50,1,52000",
        "55,1,52000",
        "10,1,50000",
        "90,2,51000",
        "40,2,52000",
        "44,2,52000",
        "49,2,53000",
        "80,3,79000",
        "100,3,55000",
        "120,3,75000",
        "30,3,84000"
    };

    private static String table2Def = "(item int, price double, date timestamp)";
    public static final String TABLE2_NAME = "purchased";
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME,CLASS_NAME, table2Def);

    private static String[] PURCHASED_ROWS = {
        "1, 1.0, '2014-09-08 18:27:48.881'",
        "1, 1.0, '2014-09-08 17:45:15.204'",
        "1, 7.0, '2014-09-08 18:33:46.446'",
        "2, 12.0, '2014-09-08 18:40:15.480'",
        "2, 5.0, '2014-09-08 18:26:51.387'",
        "2, 6.0, '2014-09-08 17:50:17.182'",
        "3, 10.0, '2014-09-08 18:25:42.387'",
        "3, 7.0, '2014-09-08 18:00:44.742'",
        "3, 3.0, '2014-09-08 17:36:55.414'",
        "4, 10.0, '2014-09-08 17:50:17.182'",
        "4, 2.0, '2014-09-08 18:05:47.166'",
        "4, 8.0, '2014-09-08 18:08:04.986'",
        "5, 4.0, '2014-09-08 17:46:26.428'",
        "5, 10.0, '2014-09-08 18:11:23.645'",
        "5, 11.0, '2014-09-08 17:41:56.353'"
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(spliceTableWatcher)
                                            .around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                PreparedStatement ps;
                try {
                    for (String row : EMPTAB_ROWS) {
                        ps = spliceClassWatcher.prepareStatement(
                            String.format("insert into %s values (%s)", spliceTableWatcher, row));
                        ps.execute();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
                                            }).around(spliceTableWatcher2)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : PURCHASED_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", spliceTableWatcher2, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testMaxMin()throws Exception{
        {
            int[] result = {50000, 50000, 50000, 52000, 52000, 53000, 75000, 51000, 51000, 51000, 52000, 55000, 55000, 55000, 75000};

            String sqlText =
                "SELECT empnum,dept,salary,min(salary) over (Partition by dept ORDER BY salary ROWS 2 preceding) as minsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++], rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {52000, 53000, 75000, 76000, 78000, 78000, 78000, 52000, 53000, 53000, 53000, 79000, 84000, 84000, 84000};
            String sqlText =
                "SELECT empnum,dept,salary,max(salary) over (Partition by dept ORDER BY salary ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as maxsal from %s";


            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }

    @Test
    public void testWindowFrame() throws Exception {
        int[] result = {78000, 78000, 78000, 78000, 78000, 78000, 78000, 53000, 53000, 53000, 53000, 84000, 84000, 84000, 84000};
        String sqlText =
            "SELECT empnum, dept, salary, max(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as maxsal from %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));
        int i = 0;
        while(rs.next()) {
            Assert.assertEquals(result[i++],rs.getInt(4));
        }
        rs.close();

        sqlText =
            "SELECT empnum, dept, salary, max(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as maxsal from %s";

        rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        i = 0;
        while(rs.next()) {
            Assert.assertEquals(result[i++], rs.getInt(4));
        }
        rs.close();

        int[] result2 = {50000, 50000, 50000, 50000, 50000, 50000, 50000, 51000, 51000, 51000, 51000, 55000, 55000, 55000, 55000};
        sqlText =
            "SELECT empnum, dept, salary, min(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and current row) as maxsal from %s";

        rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        i = 0;
        while(rs.next()) {
            Assert.assertEquals(result2[i++], rs.getInt(4));
        }
        rs.close();
    }

    @Test
    public void testSum() throws Exception {
        {
            int[] result = {50000, 102000, 154000, 207000, 282000, 358000, 436000, 51000, 103000, 155000, 208000, 55000, 130000, 209000, 293000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {436000, 436000, 436000, 436000, 436000, 436000, 436000, 208000, 208000, 208000, 208000, 293000, 293000, 293000, 293000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {436000, 386000, 334000, 282000, 229000, 154000, 78000, 208000, 157000, 105000, 53000, 293000, 238000, 163000, 84000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {102000, 154000, 157000, 180000, 204000, 229000, 154000, 103000, 155000, 157000, 105000, 130000, 209000, 238000, 163000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between 1 preceding and 1 following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }


    }

    @Test
    public void testAvg() throws Exception {
        {
            int[] result = {50000, 51000, 51333, 51750, 56400, 59666, 62285, 51000, 51500, 51666, 52000, 55000, 65000, 69666, 73250};
            String sqlText =
                "SELECT empnum, dept, salary, avg(salary) over (Partition by dept ORDER BY salary) as avgsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));
            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }

    @Test
    public void testCount() throws Exception {

        {
            int[] result = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
            String sqlText =
                "SELECT empnum, dept, salary, count(*) over (Partition by dept ORDER BY salary) as count from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
        {
            int[] result = {7, 6, 5, 4, 3, 2, 1, 4, 3, 2, 1, 4, 3, 2, 1};
            String sqlText =
                "SELECT empnum, dept, salary, count(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
        {
            int[] result = {7, 7, 7, 7, 7, 7, 7, 4, 4, 4, 4, 4, 4, 4, 4};
            String sqlText =
                "SELECT empnum, dept, salary, count(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {7, 7, 7, 7, 7, 7, 7, 4, 4, 4, 4, 4, 4, 4, 4};
            String sqlText =
                    "SELECT empnum, dept, salary, count(salary) over (Partition by dept) as c from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }

    @Test
    public void TestRangeUnbounded() throws Exception {
        {
            int[] result = {436000, 386000, 334000, 282000, 229000, 154000, 78000, 208000, 157000, 105000, 53000, 293000, 238000, 163000, 84000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between current row and unbounded following) from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {50000, 102000, 154000, 207000, 282000, 358000, 436000, 51000, 103000, 155000, 208000, 55000, 130000, 209000, 293000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {436000, 436000, 436000, 436000, 436000, 436000, 436000, 208000, 208000, 208000, 208000, 293000, 293000, 293000, 293000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }

    @Test
    public void testRangeDescOrder() throws Exception {

        {
            int[] result = {75000, 53000, 52000, 52000, 50000, 50000, 50000, 52000, 51000, 51000, 51000, 75000, 55000, 55000, 55000};
            String sqlText =
                "SELECT empnum,dept,salary,min(salary) over (Partition by dept ORDER BY salary desc ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as minsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }


        {
            int[] result = {78000, 154000, 229000, 282000, 334000, 386000, 436000, 53000, 105000, 157000, 208000, 84000, 163000, 238000, 293000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary desc) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }
    @Test
    public void testSingleRow() throws Exception {
        {
            int[] result = {50000, 52000, 52000, 53000, 75000, 76000, 78000, 51000, 52000, 52000, 53000, 55000, 75000, 79000, 84000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between current row and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }

        {
            int[] result = {50000, 52000, 52000, 53000, 75000, 76000, 78000, 51000, 52000, 52000, 53000, 55000, 75000, 79000, 84000};
            String sqlText =
                "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between current row and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals(result[i++],rs.getInt(4));
            }
            rs.close();
        }
    }

    @Test
    public void testRowNumberWithinPartiion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, ROW_NUMBER() OVER (partition by dept ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartiion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRankWithinPartiion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 7, 1, 2, 2, 4, 1, 2, 3, 4};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRankWithoutPartiion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 7, 8, 8, 10, 10, 10, 10, 14, 15};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, dept, salary, RANK() OVER (ORDER BY salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRankWithinPartion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 6, 1, 2, 2, 3, 1, 2, 3, 4};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testXPlainTrace() throws Exception {
        SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();
        xPlainTrace.turnOnTrace();
        String s = "SELECT empnum, dept, salary, count(salary) over (Partition by dept) as c from %s";
        String sqlText = String.format(s, this.getTableReference(TABLE_NAME));
        ResultSet rs = xPlainTrace.executeQuery(sqlText);
        int i = 0;
        while (rs.next()) {
            ++i;
        }
        rs.close();
        Assert.assertEquals(EMPTAB_ROWS.length, i);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT)==0);
        operation = operation.getChildren().getFirst();
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.WINDOW)==0);
        Assert.assertEquals(EMPTAB_ROWS.length, operation.getInputRows());
        Assert.assertEquals(EMPTAB_ROWS.length, operation.getOutputRows());
        Assert.assertEquals(EMPTAB_ROWS.length * 2, operation.getWriteRows());
    }

    @Test
    public void testDenseRankWithoutPartition() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 8, 8, 9, 10};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRowNumber2OrderByCols() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRank2OrderByCols() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 7, 8, 9, 9, 11, 12, 13, 14, 15};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, RANK() OVER (ORDER BY dept, salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRank2OrderByCols() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 6, 7, 8, 8, 9, 10, 11, 12, 13};
        int[] colVal = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY dept, salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] colVal = {50000, 75000, 52000, 52000, 78000, 76000, 53000, 52000, 52000, 53000, 51000, 84000, 79000, 55000, 75000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS Rank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1683 - order by follows window function")
    public void testDenseRankWithoutPartitionOrderby() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 8, 8, 9, 10};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank FROM %s order by dept";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testScalarAggWithOrderBy() throws Exception {
        // DB-1774 - ClassCastException
        double[] result = {1.0, 2.0, 9.0, 6.0, 11.0, 23.0, 3.0, 10.0, 20.0, 10.0, 12.0, 20.0, 11.0, 15.0, 25.0};
        String sqlText =
            "SELECT sum(price) over (Partition by item ORDER BY date) as  sumprice from %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE2_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(result[i++],rs.getDouble(1), 0.00);
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1775 - returns wrong column values when other columns appear in select before scalar function")
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774 - ClassCastException
        double[] result = {1.0, 2.0, 9.0, 6.0, 11.0, 23.0, 3.0, 10.0, 20.0, 10.0, 12.0, 20.0, 11.0, 15.0, 25.0};
        String sqlText =
            "SELECT item, price, sum(price) over (Partition by item ORDER BY date) as sumsal, date from %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE2_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(result[i++],rs.getDouble(3), 0.00);
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1647 - functions with same over() work. Still working on functions with diff over()")
    public void testMultiFunction() throws Exception {
        String sqlText;
        {
            sqlText = "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS RowNumber FROM %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));
            TestUtils.printResult(sqlText, rs, System.out);
            rs.close();
        }
        {
            sqlText = "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, ROW_NUMBER() OVER (ORDER BY dept) AS RowNumber FROM %s";
            ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));
            TestUtils.printResult(sqlText, rs, System.out);
            rs.close();
        }
    }
}
