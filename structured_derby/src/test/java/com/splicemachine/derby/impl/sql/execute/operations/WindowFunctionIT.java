package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.derby.management.XPlainTreeNode;
import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

/**
 * Created by jyuan on 7/30/14.
 */
public class WindowFunctionIT extends SpliceUnitTest {
    public static final String CLASS_NAME = WindowFunctionIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "EMPTAB";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(empnum int, dept int, salary int)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    private static String[] rows = {
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

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                PreparedStatement ps;
                try {
                    for (String row : rows) {
                        ps = spliceClassWatcher.prepareStatement(
                            String.format("insert into %s values (%s)", spliceTableWatcher, row));
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
        Assert.assertEquals(rows.length, i);
        xPlainTrace.turnOffTrace();

        XPlainTreeNode operation = xPlainTrace.getOperationTree();
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT)==0);
        operation = operation.getChildren().getFirst();
        Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.WINDOW)==0);
        Assert.assertEquals(rows.length, operation.getInputRows());
        Assert.assertEquals(rows.length, operation.getOutputRows());
        Assert.assertEquals(rows.length*2, operation.getWriteRows());
    }

}
