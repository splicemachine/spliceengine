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

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Test multiple window functions in a query.
 */
public class WindowFunctionMultiIT extends SpliceUnitTest {
    public static final String CLASS_NAME = WindowFunctionMultiIT.class.getSimpleName().toUpperCase();

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(empnum int, dept int, salary int, hiredate date)";
    public static final String EMPTAB = "EMPTAB";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(EMPTAB,CLASS_NAME, tableDef);

    private static String[] EMPTAB_ROWS = {
        "20,1,75000,'2012-11-11'",
        "70,1,76000,'2012-04-03'",
        "60,1,78000,'2014-03-04'",
        "110,1,53000,'2010-03-20'",
        "50,1,52000,'2011-05-24'",
        "55,1,52000,'2011-10-15'",
        "10,1,50000,'2010-03-20'",
        "90,2,51000,'2012-04-03'",
        "40,2,52000,'2013-06-06'",
        "44,2,52000,'2013-12-20'",
        "49,2,53000,'2012-04-03'",
        "80,3,79000,'2013-04-24'",
        "100,3,55000,'2010-04-12'",
        "120,3,75000,'2012-04-03'",
        "30,3,84000,'2010-08-09'",
    };

    public static final String EMPTAB_NULLS = "EMPTAB_NULLS";
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(EMPTAB_NULLS,CLASS_NAME, tableDef);

    private static String[] EMPTAB_ROWS_NULL = {
        "20,1,75000,'2012-11-11'",
        "70,1,76000,'2012-04-03'",
        "60,1,78000,'2014-03-04'",
        "110,1,53000,'2010-03-20'",
        "50,1,52000,'2011-05-24'",
        "55,1,52000,'2011-10-15'",
        "10,1,50000,'2010-03-20'",
        "90,2,51000,'2012-04-03'",
        "40,2,52000,'2013-06-06'",
        "44,2,52000,'2013-12-20'",
        "49,2,53000,'2012-04-03'",
        "80,3,79000,'2013-04-24'",
        "100,3,55000,'2010-04-12'",
        "120,3,75000,'2012-04-03'",
        "30,3,84000,'2010-08-09'",
        "32,1,null,'2010-08-09'",
        "33,3,null,'2010-08-09'"
    };

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                                            .around(spliceSchemaWatcher)
                                            .around(spliceTableWatcher)
                                            .around(spliceTableWatcher2)
        .around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                PreparedStatement ps;
                try {
                    for (String row : EMPTAB_ROWS) {
                        String sql = String.format("insert into %s values (%s)", spliceTableWatcher, row);
//                        System.out.println(sql);
                        ps = spliceClassWatcher.prepareStatement(sql);
                        ps.execute();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })
        .around(new SpliceDataWatcher() {
            @Override
            protected void starting(Description description) {
                PreparedStatement ps;
                try {
                    for (String row : EMPTAB_ROWS_NULL) {
                        String sql = String.format("insert into %s values (%s)", spliceTableWatcher2, row);
//                        System.out.println(sql);
                        ps = spliceClassWatcher.prepareStatement(sql);
                        ps.execute();
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }) ;

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testRankDate() throws Exception {
        String[] hireDate = {"2010-03-20", "2010-03-20", "2011-05-24", "2011-10-15", "2012-04-03", "2012-11-11", "2014-03-04", "2012-04-03", "2012-04-03", "2013-06-06", "2013-12-20", "2010-04-12", "2010-08-09", "2012-04-03", "2013-04-24"};
        int[] dept = {1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
        int[] rankHire = {1, 1, 3, 4, 5, 6, 7, 1, 1, 3, 4, 1, 2, 3, 4};
        String sqlText =
            "SELECT hiredate, dept, rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(hireDate[i],rs.getDate(1).toString());
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(rankHire[i],rs.getInt(3));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionSameOverClause() throws Exception {
        int[] dept = {1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
        int[] denseRank = {1, 2, 3, 4, 5, 5, 6, 1, 2, 2, 3, 1, 2, 3, 4};
        int[] rank = {1, 2, 3, 4, 5, 5, 7, 1, 2, 2, 4, 1, 2, 3, 4};
        int[] rowNum = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(rowNum[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1989: An attempt was made to get a data value of type 'java.sql.Date' from a data value of type 'INTEGER'.")
    public void testMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 6, 7, 1, 2, 2, 3, 1, 2, 3, 4, 5};
        int[] rank = {1, 2, 3, 4, 5, 6, 6, 8, 1, 2, 2, 4, 1, 2, 3, 4, 5};
        int[] ruwNum = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 1, 2, 3, 4, 5};
        String sqlText = "SELECT empnum, hiredate, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(ruwNum[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1989: An attempt was made to get a data value of type 'java.sql.Date' from a data value of type 'INTEGER'.")
    public void testSelectDateMultiFunction() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 6, 7, 1, 2, 2, 3, 1, 2, 3, 4, 5};
        int[] ruwNum = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 1, 2, 3, 4, 5};
        String sqlText = "SELECT hiredate, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(ruwNum[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        int[] denseRank = {1, 2, 2, 3, 4, 5, 6, 1, 2, 2, 3, 1, 2, 3, 4};
        int[] ruwNum    = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        String sqlText = "SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(3));
            Assert.assertEquals(ruwNum[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_MissingKeyColumn() throws Exception {
        // DB-1988 Key column missing from select causes invalid output
        int[] denseRank = {1, 2, 2, 3, 4, 5, 6, 1, 2, 2, 3, 1, 2, 3, 4};
        int[] ruwNum    = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        String sqlText = "SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(3));
            Assert.assertEquals(ruwNum[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        // Note that, because dept column is sorted arbitrarily for the same values
        // in the salary column, dept column output cannot be compared.
        // The ranking functions, however, remain deterministic for the query.
        int[] rowNumber = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};
        int[] rank = {1, 2, 3, 4, 5, 5, 7, 8, 8, 10, 10, 10, 10, 14, 15};
        int[] denseRank = {10, 9, 8, 7, 6, 6, 5, 4, 4,  3,  3,  3,  3,  2,  1};
        String sqlText =
            "SELECT dept, salary, ROW_NUMBER() OVER (ORDER BY salary desc) AS RowNumber, RANK() OVER (ORDER BY salary desc) AS Rank, DENSE_RANK() OVER (ORDER BY salary) AS DenseRank FROM %s order by salary desc";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(rowNumber[i],rs.getInt(3));
            Assert.assertEquals(rank[i],rs.getInt(4));
            Assert.assertEquals(denseRank[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }


    @Test
    public void testNullsRankDate() throws Exception {
        String[] hireDate = {"2010-03-20", "2010-03-20", "2010-08-09", "2011-05-24", "2011-10-15", "2012-04-03", "2012-11-11", "2014-03-04", "2012-04-03", "2012-04-03", "2013-06-06", "2013-12-20", "2010-04-12", "2010-08-09", "2010-08-09", "2012-04-03", "2013-04-24"};
        int[] dept = {1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 3};
        int[] rankHire = {1, 1, 3, 4, 5, 6, 7, 8, 1, 1, 3, 4, 1, 2, 2, 4, 5};
        String sqlText =
            "SELECT hiredate, dept, rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB_NULLS)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(hireDate[i],rs.getDate(1).toString());
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(rankHire[i],rs.getInt(3));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSameOverClause() throws Exception {
        int[] dept = {1, 1, 1 , 1 , 1, 1 , 1 , 1 , 2 , 2 , 2 , 2 , 3 , 3 , 3, 3, 3};
        int[] denseRank = {1, 2, 3, 4, 5, 6, 6, 7, 1, 2, 2, 3, 1, 2, 3, 4, 5};
        int[] rank = {1, 2, 3, 4, 5, 6, 6, 8, 1, 2, 2, 4, 1, 2, 3, 4, 5};
        int[] rowNum = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 1, 2, 3, 4, 5};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS Rank, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB_NULLS)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(dept[i],rs.getInt(2));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(rowNum[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    @Ignore("DB-1989: An attempt was made to get a data value of type 'java.sql.Date' from a data value of type 'INTEGER'.")
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 6, 7, 1, 2, 2, 3, 1, 2, 3, 4, 5};
        int[] rank = {1, 2, 3, 4, 5, 6, 6, 8, 1, 2, 2, 4, 1, 2, 3, 4, 5};
        int[] ruwNum = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 1, 2, 3, 4, 5};
        String sqlText = "SELECT empnum, hiredate, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS DenseRank, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept desc) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB_NULLS)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            Assert.assertEquals(rank[i],rs.getInt(5));
            Assert.assertEquals(ruwNum[i],rs.getInt(6));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        // Note that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        int[] denseRank = {1, 2, 3, 3, 4, 5, 6, 7, 1, 2, 2, 3, 1, 2, 3, 4, 5};
        int[] ruwNum    = {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 1, 2, 3, 4, 5};
        String sqlText = "SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, dept, ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept) AS RowNumber FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB_NULLS)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(denseRank[i],rs.getInt(3));
            Assert.assertEquals(ruwNum[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        // Note that, because dept column is sorted arbitrarily for the same values
        // in the salary column, dept column output cannot be compared.
        // The ranking functions, however, remain deterministic for the query.

        // Note also that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        int[] rowNumber = { 1,  2,  3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17};
        int[] rank = {1, 1, 3, 4, 5, 6, 7, 7, 9, 10, 10, 12, 12, 12, 12, 16, 17};
        int[] denseRank = {1, 1, 11, 10, 9, 8, 7, 7, 6, 5, 5, 4, 4, 4, 4, 3, 2};
        String sqlText =
            "SELECT dept, salary, ROW_NUMBER() OVER (ORDER BY salary desc) AS RowNumber, RANK() OVER (ORDER BY salary desc) AS Rank, DENSE_RANK() OVER (ORDER BY salary) AS DenseRank FROM %s order by salary desc";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(EMPTAB_NULLS)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(rowNumber[i],rs.getInt(3));
            Assert.assertEquals(rank[i],rs.getInt(4));
            Assert.assertEquals(denseRank[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }
}
