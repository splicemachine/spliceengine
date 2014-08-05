package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

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
            "10,1,50000",
            "90,2,51000",
            "40,2,52000",
            "80,3,79000",
            "100,3,55000",
            "120,3,75000",
            "30,3,84000"
    };

    private static int[] minResult = { 50000, 50000, 50000, 52000, 53000, 75000, 51000,
            51000, 55000, 55000, 55000, 75000
    };

    private static int[] maxResult = { 53000, 75000, 76000, 78000, 78000, 78000, 52000,
            52000, 79000, 84000, 84000, 84000
    };
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        for (int i = 0; i < rows.length; ++i) {
                            ps = spliceClassWatcher.prepareStatement(
                                    String.format("insert into %s values (%s)", spliceTableWatcher, rows[i]));
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
        String sqlText =
               "SELECT empnum,dept,salary,min(salary) over (Partition by dept ORDER BY salary ROWS 2 preceding) as minsal from %s";

        ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while(rs.next()) {
            Assert.assertEquals((int) rs.getInt(4), minResult[i++]);
        }
        rs.close();

        sqlText =
                "SELECT empnum,dept,salary,max(salary) over (Partition by dept ORDER BY salary ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as maxsal from %s";

        rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        i = 0;
        while(rs.next()) {
            Assert.assertEquals((int) rs.getInt(4), maxResult[i++]);
        }
        rs.close();
    }

    @Test
    public void testWindowFrame() throws Exception {
        int[] result = {78000, 78000, 78000, 78000, 78000, 78000, 52000, 52000, 84000, 84000, 84000, 84000};
        int[] result2 = {50000, 50000, 50000, 50000, 50000, 50000, 51000, 51000, 55000, 55000, 55000, 55000};
        String sqlText =
                "SELECT empnum, dept, salary, max(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as maxsal from %s";

        ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while(rs.next()) {
            Assert.assertEquals((int) rs.getInt(4), result[i++]);
        }
        rs.close();

        sqlText =
                "SELECT empnum, dept, salary, max(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as maxsal from %s";

        rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        i = 0;
        while(rs.next()) {
            Assert.assertEquals((int) rs.getInt(4), result[i++]);
        }
        rs.close();

        sqlText =
                "SELECT empnum, dept, salary, min(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and current row) as maxsal from %s";

        rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        i = 0;
        while(rs.next()) {
            Assert.assertEquals((int) rs.getInt(4), result2[i++]);
        }
        rs.close();
    }

    @Test
    public void testSum() throws Exception {
        {
            int[] result = {50000, 102000, 155000, 230000, 306000, 384000, 51000, 103000, 55000, 130000, 209000, 293000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {384000, 384000, 384000, 384000, 384000, 384000, 103000, 103000, 293000, 293000, 293000, 293000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {384000, 334000, 282000, 229000, 154000, 78000, 103000, 52000, 293000, 238000, 163000, 84000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {102000, 155000, 180000, 204000, 229000, 154000, 103000, 103000, 130000, 209000, 238000, 163000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between 1 preceding and 1 following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }


    }

    @Test
    public void testAvg() throws Exception {
        {
            int[] result = {50000, 51000, 51666, 57500, 61200, 64000, 51000, 51500, 55000, 65000, 69666, 73250};
            String sqlText =
                    "SELECT empnum, dept, salary, avg(salary) over (Partition by dept ORDER BY salary) as avgsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
    }

    @Test
    public void testCount() throws Exception {

        {
            int[] result = {1, 2, 3, 4, 5, 6, 1, 2, 1, 2, 3, 4};
            String sqlText =
                    "SELECT empnum, dept, salary, count(*) over (Partition by dept ORDER BY salary) as count from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
        {
            int[] result = {6, 5, 4, 3, 2, 1 , 2, 1, 4, 3, 2, 1};
            String sqlText =
                    "SELECT empnum, dept, salary, count(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
        {
            int[] result = {6, 6, 6, 6, 6, 6, 2, 2, 4, 4, 4, 4};
            String sqlText =
                    "SELECT empnum, dept, salary, count(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
    }

    @Test
    public void TestRangeUnbounded() throws Exception {
        {
            int[] result = {384000, 334000, 282000, 229000, 154000, 78000, 103000, 52000, 293000, 238000, 163000, 84000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between current row and unbounded following) from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {50000, 102000, 155000, 230000, 306000, 384000, 51000, 103000, 55000, 130000, 209000, 293000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {384000, 384000, 384000, 384000, 384000, 384000, 103000, 103000, 293000, 293000, 293000, 293000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and unbounded following) as sumsal from %s";
            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
    }

    @Test
    public void TestRangeDescOrder() throws Exception {

        {
            int[] result = {75000, 53000, 52000, 50000, 50000, 50000, 51000, 51000, 75000, 55000, 55000, 55000};
            String sqlText =
                    "SELECT empnum,dept,salary,min(salary) over (Partition by dept ORDER BY salary desc ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as minsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }


        {
            int[] result = {78000, 154000, 229000, 282000, 334000, 384000, 52000, 103000, 84000, 163000, 238000, 293000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary desc) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
    }
    @Test
    public void testSingleRow() throws Exception {
        {
            int[] result = {50000, 52000, 53000, 75000, 76000, 78000, 51000, 52000, 55000, 75000, 79000, 84000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary range between current row and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }

        {
            int[] result = {50000, 52000, 53000, 75000, 76000, 78000, 51000, 52000, 55000, 75000, 79000, 84000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between current row and current row) as sumsal from %s";

            ResultSet rs = methodWatcher.executeQuery(
                    String.format(sqlText, this.getTableReference(TABLE_NAME)));

            int i = 0;
            while (rs.next()) {
                Assert.assertEquals((int) rs.getInt(4), result[i++]);
            }
            rs.close();
        }
    }
}
