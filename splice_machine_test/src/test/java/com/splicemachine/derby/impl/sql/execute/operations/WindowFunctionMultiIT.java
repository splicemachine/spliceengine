package com.splicemachine.derby.impl.sql.execute.operations;

import static org.junit.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

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

    private static String table6Def = "(SOURCE_SALES_INSTANCE_ID BIGINT, TRANSACTION_DT DATE NOT NULL, ORIGINAL_SKU_CATEGORY_ID INTEGER, SALES_AMT DECIMAL(9,2), CUSTOMER_MASTER_ID BIGINT)";
    public static final String TABLE6_NAME = "TXN_DETAIL";
    protected static SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher(TABLE6_NAME,CLASS_NAME, table6Def);

    private static String[] TXN_DETAIL = {
        "0,'2013-05-12',44871,329.18,74065939",
        "0,'2013-05-12',44199,35.46,74065939",
        "0,'2013-05-12',44238,395.44,74065939",
        "0,'2013-05-12',44410,1763.41,74065939",
        "0,'2013-05-12',44797,915.97,74065939",
        "0,'2013-05-12',44837,179.88,74065939",
        "0,'2013-05-12',44600,0,74065939",
        "0,'2013-05-12',44880,467.33,74065939"
    };

    private static String table7Def = "(years INTEGER, month INTEGER, prd_type_id INTEGER, emp_id INTEGER , amount NUMERIC(8, 2))";
    public static final String TABLE7_NAME = "ALL_SALES";
    protected static SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher(TABLE7_NAME,CLASS_NAME, table7Def);

    private static String[] ALL_SALES = {
        "2006,1,1,21,16034.84",
        "2006,2,1,21,15644.65",
        "2006,3,2,21,20167.83",
        "2006,4,2,21,25056.45",
        "2006,5,2,21,NULL",
        "2006,6,1,21,15564.66",
        "2006,7,1,21,15644.65",
        "2006,8,1,21,16434.82",
        "2006,9,1,21,19654.57",
        "2006,10,1,21,21764.19",
        "2006,11,1,21,13026.73",
        "2006,12,2,21,10034.64",
        "2005,1,2,22,16634.84",
        "2005,1,2,21,26034.84",
        "2005,2,1,21,12644.65",
        "2005,3,1,21,NULL",
        "2005,4,1,21,25026.45",
        "2005,5,1,21,17212.66",
        "2005,6,1,21,15564.26",
        "2005,7,2,21,62654.82",
        "2005,8,2,21,26434.82",
        "2005,9,2,21,15644.65",
        "2005,10,2,21,21264.19",
        "2005,11,1,21,13026.73",
        "2005,12,1,21,10032.64"
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
                                            })
                                            .around(spliceTableWatcher6)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : TXN_DETAIL) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", spliceTableWatcher6, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcher7)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : ALL_SALES) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)", spliceTableWatcher7, row));
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
    public void testRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2012-04-03 |  3  |    3    |\n" +
                "2013-04-24 |  3  |    4    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2011-05-24 |  1  |    3    |\n" +
                "2011-10-15 |  1  |    4    |\n" +
                "2012-04-03 |  1  |    5    |\n" +
                "2012-11-11 |  1  |    6    |\n" +
                "2014-03-04 |  1  |    7    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testNullsRankDate() throws Exception {
        String sqlText =
            String.format("SELECT hiredate, dept, " +
                              "rank() OVER (partition by dept ORDER BY hiredate) AS rankhire FROM %s",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT |RANKHIRE |\n" +
                "----------------------------\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2012-04-03 |  2  |    1    |\n" +
                "2013-06-06 |  2  |    3    |\n" +
                "2013-12-20 |  2  |    4    |\n" +
                "2010-04-12 |  3  |    1    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2010-08-09 |  3  |    2    |\n" +
                "2012-04-03 |  3  |    4    |\n" +
                "2013-04-24 |  3  |    5    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-03-20 |  1  |    1    |\n" +
                "2010-08-09 |  1  |    3    |\n" +
                "2011-05-24 |  1  |    4    |\n" +
                "2011-10-15 |  1  |    5    |\n" +
                "2012-04-03 |  1  |    6    |\n" +
                "2012-11-11 |  1  |    7    |\n" +
                "2014-03-04 |  1  |    8    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS DenseRank, " +
                              "RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS Rank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  30   |  3  | 84000 |     1     |  1  |     1     |\n" +
                "  80   |  3  | 79000 |     2     |  2  |     2     |\n" +
                "  120  |  3  | 75000 |     3     |  3  |     3     |\n" +
                "  100  |  3  | 55000 |     4     |  4  |     4     |\n" +
                "  60   |  1  | 78000 |     1     |  1  |     1     |\n" +
                "  70   |  1  | 76000 |     2     |  2  |     2     |\n" +
                "  20   |  1  | 75000 |     3     |  3  |     3     |\n" +
                "  110  |  1  | 53000 |     4     |  4  |     4     |\n" +
                "  50   |  1  | 52000 |     5     |  5  |     5     |\n" +
                "  55   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  10   |  1  | 50000 |     7     |  7  |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS DenseRank, " +
                              "RANK() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS Rank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY salary desc, empnum) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |RANK | ROWNUMBER |\n" +
                "----------------------------------------------------\n" +
                "  49   |  2  | 53000 |     1     |  1  |     1     |\n" +
                "  40   |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  44   |  2  | 52000 |     3     |  3  |     3     |\n" +
                "  90   |  2  | 51000 |     4     |  4  |     4     |\n" +
                "  33   |  3  | NULL  |     1     |  1  |     1     |\n" +
                "  30   |  3  | 84000 |     2     |  2  |     2     |\n" +
                "  80   |  3  | 79000 |     3     |  3  |     3     |\n" +
                "  120  |  3  | 75000 |     4     |  4  |     4     |\n" +
                "  100  |  3  | 55000 |     5     |  5  |     5     |\n" +
                "  32   |  1  | NULL  |     1     |  1  |     1     |\n" +
                "  60   |  1  | 78000 |     2     |  2  |     2     |\n" +
                "  70   |  1  | 76000 |     3     |  3  |     3     |\n" +
                "  20   |  1  | 75000 |     4     |  4  |     4     |\n" +
                "  110  |  1  | 53000 |     5     |  5  |     5     |\n" +
                "  50   |  1  | 52000 |     6     |  6  |     6     |\n" +
                "  55   |  1  | 52000 |     7     |  7  |     7     |\n" +
                "  10   |  1  | 50000 |     8     |  8  |     8     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT empnum, hiredate, dept, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc, hiredate) AS DenseRank, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate desc) AS RowNumber " +
                              "FROM %s order by hiredate desc, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM | HIREDATE  |DEPT |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------------------------\n" +
                "  60   |2014-03-04 |  1  | 78000 |     1     |  1  |     1     |\n" +
                "  44   |2013-12-20 |  2  | 52000 |     3     |  2  |     1     |\n" +
                "  40   |2013-06-06 |  2  | 52000 |     2     |  2  |     2     |\n" +
                "  80   |2013-04-24 |  3  | 79000 |     2     |  3  |     1     |\n" +
                "  20   |2012-11-11 |  1  | 75000 |     3     |  1  |     2     |\n" +
                "  49   |2012-04-03 |  2  | 53000 |     1     |  2  |     3     |\n" +
                "  70   |2012-04-03 |  1  | 76000 |     2     |  1  |     3     |\n" +
                "  90   |2012-04-03 |  2  | 51000 |     4     |  2  |     4     |\n" +
                "  120  |2012-04-03 |  3  | 75000 |     3     |  3  |     2     |\n" +
                "  55   |2011-10-15 |  1  | 52000 |     6     |  1  |     4     |\n" +
                "  50   |2011-05-24 |  1  | 52000 |     5     |  1  |     5     |\n" +
                "  30   |2010-08-09 |  3  | 84000 |     1     |  3  |     3     |\n" +
                "  100  |2010-04-12 |  3  | 55000 |     4     |  3  |     4     |\n" +
                "  10   |2010-03-20 |  1  | 50000 |     7     |  1  |     7     |\n" +
                "  110  |2010-03-20 |  1  | 53000 |     4     |  1  |     6     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT empnum, hiredate, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY hiredate, salary) AS DR_Hire_Sal_By_Dept, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate, dept) AS RN_Hire_Sal_By_Dept " +
                              "FROM %s order by dept, hiredate",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM | HIREDATE  |SALARY | DR_HIRE_SAL_BY_DEPT |DEPT | RN_HIRE_SAL_BY_DEPT |\n" +
                "------------------------------------------------------------------------------\n" +
                "  10   |2010-03-20 | 50000 |          1          |  1  |          1          |\n" +
                "  110  |2010-03-20 | 53000 |          2          |  1  |          2          |\n" +
                "  32   |2010-08-09 | NULL  |          3          |  1  |          3          |\n" +
                "  50   |2011-05-24 | 52000 |          4          |  1  |          4          |\n" +
                "  55   |2011-10-15 | 52000 |          5          |  1  |          5          |\n" +
                "  70   |2012-04-03 | 76000 |          6          |  1  |          6          |\n" +
                "  20   |2012-11-11 | 75000 |          7          |  1  |          7          |\n" +
                "  60   |2014-03-04 | 78000 |          8          |  1  |          8          |\n" +
                "  90   |2012-04-03 | 51000 |          1          |  2  |          1          |\n" +
                "  49   |2012-04-03 | 53000 |          2          |  2  |          2          |\n" +
                "  40   |2013-06-06 | 52000 |          3          |  2  |          3          |\n" +
                "  44   |2013-12-20 | 52000 |          4          |  2  |          4          |\n" +
                "  100  |2010-04-12 | 55000 |          1          |  3  |          1          |\n" +
                "  33   |2010-08-09 | NULL  |          2          |  3  |          2          |\n" +
                "  30   |2010-08-09 | 84000 |          3          |  3  |          3          |\n" +
                "  120  |2012-04-03 | 75000 |          4          |  3  |          4          |\n" +
                "  80   |2013-04-24 | 79000 |          5          |  3  |          5          |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectDateMultiFunction() throws Exception {
        // DB-1989 - Attempted to encode a value that does not have a scalar type format id
        String sqlText =
            String.format("SELECT hiredate, dept, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY hiredate, salary) AS DenseRank_HireDate_Salary_By_Dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY hiredate, dept) AS RowNumber_HireDate_Salary_By_Dept " +
                              "FROM %s order by hiredate, RowNumber_HireDate_Salary_By_Dept",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "HIREDATE  |DEPT | DENSERANK_HIREDATE_SALARY_BY_DEPT | ROWNUMBER_HIREDATE_SALARY_BY_DEPT |\n" +
                "------------------------------------------------------------------------------------------\n" +
                "2010-03-20 |  1  |                 1                 |                 1                 |\n" +
                "2010-03-20 |  1  |                 2                 |                 2                 |\n" +
                "2010-04-12 |  3  |                 1                 |                 1                 |\n" +
                "2010-08-09 |  3  |                 2                 |                 2                 |\n" +
                "2011-05-24 |  1  |                 3                 |                 3                 |\n" +
                "2011-10-15 |  1  |                 4                 |                 4                 |\n" +
                "2012-04-03 |  2  |                 1                 |                 1                 |\n" +
                "2012-04-03 |  2  |                 2                 |                 2                 |\n" +
                "2012-04-03 |  3  |                 3                 |                 3                 |\n" +
                "2012-04-03 |  1  |                 5                 |                 5                 |\n" +
                "2012-11-11 |  1  |                 6                 |                 6                 |\n" +
                "2013-04-24 |  3  |                 4                 |                 4                 |\n" +
                "2013-06-06 |  2  |                 3                 |                 3                 |\n" +
                "2013-12-20 |  2  |                 4                 |                 4                 |\n" +
                "2014-03-04 |  1  |                 7                 |                 7                 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        String sqlText = String.format("SELECT empnum, salary, " +
                                           "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, " +
                                           "dept, " +
                                           "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s order by empnum",
                                       this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------\n" +
                "  10   | 50000 |     1     |  1  |     1     |\n" +
                "  20   | 75000 |     4     |  1  |     2     |\n" +
                "  30   | 84000 |     4     |  3  |     1     |\n" +
                "  40   | 52000 |     2     |  2  |     1     |\n" +
                "  44   | 52000 |     2     |  2  |     2     |\n" +
                "  49   | 53000 |     3     |  2  |     3     |\n" +
                "  50   | 52000 |     2     |  1  |     3     |\n" +
                "  55   | 52000 |     2     |  1  |     4     |\n" +
                "  60   | 78000 |     6     |  1  |     5     |\n" +
                "  70   | 76000 |     5     |  1  |     6     |\n" +
                "  80   | 79000 |     3     |  3  |     2     |\n" +
                "  90   | 51000 |     1     |  2  |     4     |\n" +
                "  100  | 55000 |     1     |  3  |     3     |\n" +
                "  110  | 53000 |     3     |  1  |     7     |\n" +
                "  120  | 75000 |     2     |  3  |     4     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionSamePartitionDifferentOrderBy_WO_hiredate() throws Exception {
        // Note that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        String sqlText =
            String.format("SELECT empnum, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary, empnum) AS DenseRank, " +
                              "dept, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber FROM %s order by dept, empnum",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |DEPT | ROWNUMBER |\n" +
                "----------------------------------------------\n" +
                "  10   | 50000 |     2     |  1  |     1     |\n" +
                "  20   | 75000 |     6     |  1  |     2     |\n" +
                "  32   | NULL  |     1     |  1  |     3     |\n" +
                "  50   | 52000 |     3     |  1  |     4     |\n" +
                "  55   | 52000 |     4     |  1  |     5     |\n" +
                "  60   | 78000 |     8     |  1  |     6     |\n" +
                "  70   | 76000 |     7     |  1  |     7     |\n" +
                "  110  | 53000 |     5     |  1  |     8     |\n" +
                "  40   | 52000 |     2     |  2  |     1     |\n" +
                "  44   | 52000 |     3     |  2  |     2     |\n" +
                "  49   | 53000 |     4     |  2  |     3     |\n" +
                "  90   | 51000 |     1     |  2  |     4     |\n" +
                "  30   | 84000 |     5     |  3  |     1     |\n" +
                "  33   | NULL  |     1     |  3  |     2     |\n" +
                "  80   | 79000 |     4     |  3  |     3     |\n" +
                "  100  | 55000 |     2     |  3  |     4     |\n" +
                "  120  | 75000 |     3     |  3  |     5     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionSamePartitionDifferentOrderBy_MissingKeyColumn() throws Exception {
        // DB-1988 Key column missing from select causes invalid output
        String sqlText =
            String.format("SELECT empnum, salary, " +
                              "DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary) AS DenseRank, " +
                              "ROW_NUMBER() OVER (PARTITION BY dept ORDER BY dept, empnum) AS RowNumber " +
                              "FROM %s order by salary, empnum",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK | ROWNUMBER |\n" +
                "----------------------------------------\n" +
                "  10   | 50000 |     1     |     1     |\n" +
                "  90   | 51000 |     1     |     4     |\n" +
                "  40   | 52000 |     2     |     1     |\n" +
                "  44   | 52000 |     2     |     2     |\n" +
                "  50   | 52000 |     2     |     3     |\n" +
                "  55   | 52000 |     2     |     4     |\n" +
                "  49   | 53000 |     3     |     3     |\n" +
                "  110  | 53000 |     3     |     7     |\n" +
                "  100  | 55000 |     1     |     3     |\n" +
                "  20   | 75000 |     4     |     2     |\n" +
                "  120  | 75000 |     2     |     4     |\n" +
                "  70   | 76000 |     5     |     6     |\n" +
                "  60   | 78000 |     6     |     5     |\n" +
                "  80   | 79000 |     3     |     2     |\n" +
                "  30   | 84000 |     4     |     1     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        String sqlText =
            String.format("SELECT salary, dept, " +
                              "ROW_NUMBER() OVER (ORDER BY salary, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s order by salary desc, dept",
                          this.getTableReference(EMPTAB));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " 84000 |  3  |    15     |  1  |     1     |\n" +
                " 79000 |  3  |    14     |  2  |     2     |\n" +
                " 78000 |  1  |    13     |  3  |     3     |\n" +
                " 76000 |  1  |    12     |  4  |     4     |\n" +
                " 75000 |  1  |    10     |  5  |     5     |\n" +
                " 75000 |  3  |    11     |  6  |     6     |\n" +
                " 55000 |  3  |     9     |  7  |     7     |\n" +
                " 53000 |  1  |     7     |  8  |     8     |\n" +
                " 53000 |  2  |     8     |  9  |     9     |\n" +
                " 52000 |  1  |     3     | 10  |    10     |\n" +
                " 52000 |  1  |     4     | 10  |    10     |\n" +
                " 52000 |  2  |     5     | 12  |    11     |\n" +
                " 52000 |  2  |     6     | 12  |    11     |\n" +
                " 51000 |  2  |     2     | 14  |    12     |\n" +
                " 50000 |  1  |     1     | 15  |    13     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testNullsMultiFunctionInQueryDiffAndSameOverClause() throws Exception {
        // Note that, because nulls are sorted last by default in PostgreSQL and
        // we sort nulls first by default, the comparison of ranking function output
        // cannot be compared with PostgreSQL's. Verification of this output is manual.
        String sqlText =
            String.format("SELECT salary, dept, " +
                              "ROW_NUMBER() OVER (ORDER BY salary, dept) AS RowNumber, " +
                              "RANK() OVER (ORDER BY salary desc, dept) AS Rank, " +
                              "DENSE_RANK() OVER (ORDER BY salary desc, dept) AS DenseRank " +
                              "FROM %s order by salary desc, dept",
                          this.getTableReference(EMPTAB_NULLS));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SALARY |DEPT | ROWNUMBER |RANK | DENSERANK |\n" +
                "--------------------------------------------\n" +
                " NULL  |  1  |     1     |  1  |     1     |\n" +
                " NULL  |  3  |     2     |  2  |     2     |\n" +
                " 84000 |  3  |    17     |  3  |     3     |\n" +
                " 79000 |  3  |    16     |  4  |     4     |\n" +
                " 78000 |  1  |    15     |  5  |     5     |\n" +
                " 76000 |  1  |    14     |  6  |     6     |\n" +
                " 75000 |  1  |    12     |  7  |     7     |\n" +
                " 75000 |  3  |    13     |  8  |     8     |\n" +
                " 55000 |  3  |    11     |  9  |     9     |\n" +
                " 53000 |  1  |     9     | 10  |    10     |\n" +
                " 53000 |  2  |    10     | 11  |    11     |\n" +
                " 52000 |  1  |     5     | 12  |    12     |\n" +
                " 52000 |  1  |     6     | 12  |    12     |\n" +
                " 52000 |  2  |     7     | 14  |    13     |\n" +
                " 52000 |  2  |     8     | 14  |    13     |\n" +
                " 51000 |  2  |     4     | 16  |    14     |\n" +
                " 50000 |  1  |     3     | 17  |    15     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPullFunctionInputColumnUp4Levels() throws Exception {
        // DB-2087 - Kryo exception
        String sqlText =
            String.format("select Transaction_Detail5.SOURCE_SALES_INSTANCE_ID C0, " +
                              "min(Transaction_Detail5.TRANSACTION_DT) over (partition by Transaction_Detail5.ORIGINAL_SKU_CATEGORY_ID) C1, " +
                              "sum(Transaction_Detail5.SALES_AMT) over (partition by Transaction_Detail5.TRANSACTION_DT) C10 " +
                              "from %s AS Transaction_Detail5 " +
                              "where Transaction_Detail5.TRANSACTION_DT between DATE('2010-01-21') " +
                              "and DATE('2013-11-21') and Transaction_Detail5.CUSTOMER_MASTER_ID=74065939",
                          this.getTableReference(TABLE6_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "C0 |    C1     |  C10   |\n" +
                "-------------------------\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |\n" +
                " 0 |2013-05-12 |4086.67 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowDefnEquivalent() throws Exception {
        // DB-2165 - NPE, operator null in OverClause.isEquivalent(OrderByCol, OrderByCol)
        String sqlText =
            String.format("SELECT\n" +
                              "prd_type_id, SUM(amount),\n" +
                              "RANK() OVER (ORDER BY SUM(amount) DESC) AS rank,\n" +
                              "DENSE_RANK() OVER (ORDER BY SUM(amount) DESC) AS dense_rank\n" +
                              "FROM %s\n" +
                              "WHERE amount IS NOT NULL\n" +
                              "GROUP BY prd_type_id\n" +
                              "ORDER BY prd_type_id", this.getTableReference(TABLE7_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PRD_TYPE_ID |    2     |RANK |DENSE_RANK |\n" +
                "-------------------------------------------\n" +
                "      1      |227276.50 |  1  |     1     |\n" +
                "      2      |223927.08 |  2  |     2     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
