package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;

/**
 * Created by jyuan on 7/30/14.
 */
public class WindowFunctionMultiIT extends SpliceUnitTest {
    public static final String CLASS_NAME = WindowFunctionMultiIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(empnum int, dept int, salary int, hiredate timestamp)";
    public static final String TABLE_NAME = "EMPTAB";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    private static String[] EMPTAB_ROWS = {
        "20,1,75000,'2014-09-08 18:27:48.881'",
        "70,1,76000,'2014-09-08 17:45:15.204'",
        "60,1,78000,'2014-09-08 18:33:46.446'",
        "110,1,53000, '2014-09-08 18:40:15.480'",
        "50,1,52000,'2014-09-08 18:26:51.387'",
        "55,1,52000,'2014-09-08 17:50:17.182'",
        "10,1,50000, '2014-09-08 18:25:42.387'",
        "90,2,51000,'2014-09-08 18:00:44.742'",
        "40,2,52000,'2014-09-08 17:36:55.414'",
        "44,2,52000, '2014-09-08 17:50:17.182'",
        "49,2,53000,'2014-09-08 18:05:47.166'",
        "80,3,79000,'2014-09-08 18:08:04.986'",
        "100,3,55000,'2014-09-08 17:46:26.428'",
        "120,3,75000, '2014-09-08 18:11:23.645'",
        "30,3,84000, '2014-09-08 17:41:56.353'"
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
            }}) ;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testMultiFunctionInQuerySameOverClause() throws Exception {
        // DB-1647 (partial; multiple functions with same over() work)
        int[] dept = {1 , 1 , 1 , 1, 1 , 1 , 1 , 2 , 2 , 2 , 2 , 3 , 3 , 3, 3};
        int[] denseRank = {1 , 2 , 3 , 4, 5 , 5 , 6 , 1 , 2 , 2 , 3 , 1 , 2 , 3, 4};
        int[] rank = {1, 2, 3, 4, 5, 5, 7, 1, 2, 2, 4, 1, 2, 3, 4};
        int[] rowNumber = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));
        TestUtils.printResult(sqlText, rs, System.out);

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(rowNumber[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        // DB-1647 (partial; multiple functions with same over() work)
        int[] dept = {1 , 1 , 1 , 1, 1 , 1 , 1 , 2 , 2 , 2 , 2 , 3 , 3 , 3, 3};
        int[] denseRank = {1 , 2 , 3 , 4, 5 , 5 , 6 , 1 , 2 , 2 , 3 , 1 , 2 , 3, 4};
        int[] rank = {1, 2, 3, 4, 5, 5, 7, 1, 2, 2, 4, 1, 2, 3, 4};
        int[] rowNumber = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        String sqlText =
            "SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY salary desc) AS RowNumber, RANK() OVER (PARTITION BY dept ORDER BY salary) AS Rank, DENSE_RANK() OVER (ORDER BY salary) AS DenseRank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));
        TestUtils.printResult(sqlText, rs, System.out);

        TestUtils.printResult(sqlText, rs, System.out);
        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(rowNumber[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartiion() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, hiredate, dept, salary, DENSE_RANK() OVER (PARTITION BY  ORDER BY hiredate) AS denserank FROM %s order by dept";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));
        // DEBUG
        TestUtils.printResult(sqlText, rs, System.out);

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(colVal[i],rs.getInt(3));
            Assert.assertEquals(result[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionInQueryDifferentOverClause() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] rowNum = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        // DB-1647 (multiple functions with different over() do not work)
        String sqlText = "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));
        TestUtils.printResult(sqlText, rs, System.out);

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(3));
            Assert.assertEquals(rowNum[i],rs.getInt(4));
            ++i;
        }
        rs.close();

        Assert.fail("You fool! You've ruined us all!");
    }
}
