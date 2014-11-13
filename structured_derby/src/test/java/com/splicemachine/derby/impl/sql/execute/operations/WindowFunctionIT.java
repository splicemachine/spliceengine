package com.splicemachine.derby.impl.sql.execute.operations;

import static junit.framework.Assert.assertEquals;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

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
import com.splicemachine.derby.test.framework.SpliceViewWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceXPlainTrace;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;

/**
 *
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

    private static String table3Def = "(PersonID int,FamilyID int,FirstName varchar(10),LastName varchar(25),DOB timestamp)";
    public static final String TABLE3_NAME = "people";
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE3_NAME,CLASS_NAME, table3Def);

    private static String[] PEOPLE_ROWS = {
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'",
        "3,2,'Karly','Matthews','2000-05-20 04:00:00'",
        "4,2,'Kacy','Matthews','2000-05-20 04:02:00'",
        "5,2,'Tom','Matthews','2001-09-15 11:52:00'",
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'",
        "3,2,'Karly','Matthews','2000-05-20 04:00:00'",
        "5,2,'Tom','Matthews','2001-09-15 11:52:00'",
        "1,1,'Joe','Johnson', '2000-10-23 13:00:00'",
        "2,1,'Jim','Johnson','2001-12-15 05:45:00'"
    };

    private static String table4Def = "(EMPNO NUMERIC(4) NOT NULL, ENAME VARCHAR(10), JOB VARCHAR(9), MGR numeric(4), HIREDATE DATE, SAL NUMERIC(7, 2), COMM numeric(7, 2), DEPTNO numeric(2))";
    public static final String TABLE4_NAME = "emp";
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE4_NAME,CLASS_NAME, table4Def);

    private static String[] EMP_ROWS = {
        "7369, 'SMITH', 'CLERK',    7902, '1980-12-17', 800, NULL, 20",
        "7499, 'ALLEN', 'SALESMAN', 7698, '1981-02-20', 1600, 300, 30",
        "7521, 'WARD',  'SALESMAN', 7698, '1981-02-22', 1250, 500, 30",
        "7566, 'JONES', 'MANAGER',  7839, '1981-04-02', 2975, NULL, 20",
        "7654, 'MARTIN', 'SALESMAN', 7698,'1981-09-28', 1250, 1400, 30",
        "7698, 'BLAKE', 'MANAGER', 7839,'1981-05-01', 2850, NULL, 30",
        "7782, 'CLARK', 'MANAGER', 7839,'1981-06-09', 2450, NULL, 10",
        "7788, 'SCOTT', 'ANALYST', 7566,'1982-12-09', 3000, NULL, 20",
        "7839, 'KING', 'PRESIDENT', NULL,'1981-11-17', 5000, NULL, 10",
        "7844, 'TURNER', 'SALESMAN', 7698,'1981-09-08', 1500, 0, 30",
        "7876, 'ADAMS', 'CLERK', 7788,'1983-01-12', 1100, NULL, 20",
        "7900, 'JAMES', 'CLERK', 7698,'1981-12-03', 950, NULL, 30",
        "7902, 'FORD', 'ANALYST', 7566,'1981-12-03', 3000, NULL, 20",
        "7934, 'MILLER', 'CLERK', 7782,'1982-01-23', 1300, NULL, 10"
    };

    private static final String VIEW_NAME = "YEAR_VIEW";
    private static String viewDef = String.format("as select to_char(hiredate,'yy') as yr,ename,hiredate from %s.%s group by hiredate,ename",
                                                  CLASS_NAME, TABLE4_NAME);
    private static SpliceViewWatcher yearView = new SpliceViewWatcher(VIEW_NAME,CLASS_NAME, viewDef);

    private static String table5aDef = "(ID INT generated always as identity (START WITH 1, INCREMENT BY 1) PRIMARY KEY, Nome_Dep VARCHAR(200))";
    public static final String TABLE5a_NAME = "Departamentos";
    protected static SpliceTableWatcher spliceTableWatcher5a = new SpliceTableWatcher(TABLE5a_NAME,CLASS_NAME, table5aDef);

    private static String[] DEPT = {
        "'Vendas'",
        "'IT'",
        "'Recursos Humanos'"
    };

    private static String table5bDef = "(ID INT generated always as identity (START WITH 1, INCREMENT BY 1) PRIMARY KEY, ID_Dep INT, Nome VARCHAR(200), Salario Numeric(18,2))";
    public static final String TABLE5b_NAME = "Funcionarios";
    protected static SpliceTableWatcher spliceTableWatcher5b = new SpliceTableWatcher(TABLE5b_NAME,CLASS_NAME, table5bDef);

    private static String[] FUNC = {
        "1, 'Fabiano', 2000",
        "1, 'Amorim', 2500",
        "1, 'Diego', 9000",
        "2, 'Felipe', 2000",
        "2, 'Ferreira', 2500",
        "2, 'Nogare', 11999",
        "3, 'Laerte', 5000",
        "3, 'Luciano', 23500",
        "3, 'Zavaschi', 13999"
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
                                                }})
                                            .around(spliceTableWatcher2)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : PURCHASED_ROWS) {
                                                            String sql = String.format("insert into %s values (%s)",
                                                                                       spliceTableWatcher2, row);
//                            System.out.println(sql+";");  // will print insert statements
                                                            ps = spliceClassWatcher.prepareStatement(sql);
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcher3)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : PEOPLE_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)",
                                                                              spliceTableWatcher3, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcher4)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : EMP_ROWS) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s values (%s)",
                                                                              spliceTableWatcher4, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(yearView)
                                            .around(spliceTableWatcher5a)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : DEPT) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s (Nome_Dep) values (%s)" +
                                                                                  "", spliceTableWatcher5a, row));
                                                            ps.execute();
                                                        }
                                                    } catch (Exception e) {
                                                        throw new RuntimeException(e);
                                                    }
                                                }
                                            })
                                            .around(spliceTableWatcher5b)
                                            .around(new SpliceDataWatcher() {
                                                @Override
                                                protected void starting(Description description) {
                                                    PreparedStatement ps;
                                                    try {
                                                        for (String row : FUNC) {
                                                            ps = spliceClassWatcher.prepareStatement(
                                                                String.format("insert into %s (ID_Dep, Nome, " +
                                                                                  "Salario) values (%s)",
                                                                              spliceTableWatcher5b, row));
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
    public void testMinRows2Preceding() throws Exception {
        String sqlText =
            String.format("SELECT empnum,dept,salary," +
                              "min(salary) over (Partition by dept ORDER BY salary ROWS 2 preceding) as minsal " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 52000 |\n" +
                "  30   |  3  | 84000 | 75000 |\n" +
                "  40   |  2  | 52000 | 51000 |\n" +
                "  44   |  2  | 52000 | 51000 |\n" +
                "  49   |  2  | 53000 | 52000 |\n" +
                "  50   |  1  | 52000 | 50000 |\n" +
                "  55   |  1  | 52000 | 50000 |\n" +
                "  60   |  1  | 78000 | 75000 |\n" +
                "  70   |  1  | 76000 | 53000 |\n" +
                "  80   |  3  | 79000 | 55000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  110  |  1  | 53000 | 52000 |\n" +
                "  120  |  3  | 75000 | 55000 |";

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMaxRowsCurrent2Following()throws Exception{
        String sqlText =
            String.format("SELECT empnum,dept,salary," +
                              "max(salary) over (Partition by dept ORDER BY salary, empnum ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING ) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 52000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 53000 |\n" +
                "  55   |  1  | 52000 | 75000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 52000 |\n" +
                "  100  |  3  | 55000 | 79000 |\n" +
                "  110  |  1  | 53000 | 76000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFrameRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept ORDER BY salary, empnum rows between unbounded preceding and unbounded following) as maxsal " +
                              "from %s order by dept",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  120  |  3  | 75000 | 84000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  30   |  3  | 84000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testFrameRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testFrameRowsUnboundedPrecedingCurrentRow() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "min(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and current row) as minsal " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 50000 |\n" +
                "  30   |  3  | 84000 | 55000 |\n" +
                "  40   |  2  | 52000 | 51000 |\n" +
                "  44   |  2  | 52000 | 51000 |\n" +
                "  49   |  2  | 53000 | 51000 |\n" +
                "  50   |  1  | 52000 | 50000 |\n" +
                "  55   |  1  | 52000 | 50000 |\n" +
                "  60   |  1  | 78000 | 50000 |\n" +
                "  70   |  1  | 76000 | 50000 |\n" +
                "  80   |  3  | 79000 | 55000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  110  |  1  | 53000 | 50000 |\n" +
                "  120  |  3  | 75000 | 55000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSum() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |334000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |105000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumRows1Preceding1Following() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between 1 preceding and 1 following) as sumsal from %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  90   |  2  | 51000 |103000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |157000 |\n" +
                "  49   |  2  | 53000 |105000 |\n" +
                "  100  |  3  | 55000 |130000 |\n" +
                "  120  |  3  | 75000 |209000 |\n" +
                "  80   |  3  | 79000 |238000 |\n" +
                "  30   |  3  | 84000 |163000 |\n" +
                "  10   |  1  | 50000 |102000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |157000 |\n" +
                "  110  |  1  | 53000 |180000 |\n" +
                "  20   |  1  | 75000 |204000 |\n" +
                "  70   |  1  | 76000 |229000 |\n" +
                "  60   |  1  | 78000 |154000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testAvg() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "avg(salary) over (Partition by dept ORDER BY salary) as avgsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |AVGSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 | 56400 |\n" +
                "  50   |  1  | 52000 | 51333 |\n" +
                "  55   |  1  | 52000 | 51333 |\n" +
                "  60   |  1  | 78000 | 62285 |\n" +
                "  70   |  1  | 76000 | 59666 |\n" +
                "  110  |  1  | 53000 | 51750 |\n" +
                "  40   |  2  | 52000 | 51666 |\n" +
                "  44   |  2  | 52000 | 51666 |\n" +
                "  49   |  2  | 53000 | 52000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 | 73250 |\n" +
                "  80   |  3  | 79000 | 69666 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 | 65000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountStar() throws Exception {

        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(*) over (Partition by dept ORDER BY salary) as count from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | COUNT |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |   1   |\n" +
                "  20   |  1  | 75000 |   5   |\n" +
                "  50   |  1  | 52000 |   3   |\n" +
                "  55   |  1  | 52000 |   3   |\n" +
                "  60   |  1  | 78000 |   7   |\n" +
                "  70   |  1  | 76000 |   6   |\n" +
                "  110  |  1  | 53000 |   4   |\n" +
                "  40   |  2  | 52000 |   3   |\n" +
                "  44   |  2  | 52000 |   3   |\n" +
                "  49   |  2  | 53000 |   4   |\n" +
                "  90   |  2  | 51000 |   1   |\n" +
                "  30   |  3  | 84000 |   4   |\n" +
                "  80   |  3  | 79000 |   3   |\n" +
                "  100  |  3  | 55000 |   1   |\n" +
                "  120  |  3  | 75000 |   2   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountRowsCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) as countsal " +
                              "from %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |COUNTSAL |\n" +
                "--------------------------------\n" +
                "  90   |  2  | 51000 |    4    |\n" +
                "  40   |  2  | 52000 |    3    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    1    |\n" +
                "  100  |  3  | 55000 |    4    |\n" +
                "  120  |  3  | 75000 |    3    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  10   |  1  | 50000 |    7    |\n" +
                "  50   |  1  | 52000 |    6    |\n" +
                "  55   |  1  | 52000 |    5    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  20   |  1  | 75000 |    3    |\n" +
                "  70   |  1  | 76000 |    2    |\n" +
                "  60   |  1  | 78000 |    1    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCountRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by salary, dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |   7   |\n" +
                "  90   |  2  | 51000 |   4   |\n" +
                "  50   |  1  | 52000 |   7   |\n" +
                "  55   |  1  | 52000 |   7   |\n" +
                "  40   |  2  | 52000 |   4   |\n" +
                "  44   |  2  | 52000 |   4   |\n" +
                "  110  |  1  | 53000 |   7   |\n" +
                "  49   |  2  | 53000 |   4   |\n" +
                "  100  |  3  | 55000 |   4   |\n" +
                "  20   |  1  | 75000 |   7   |\n" +
                "  120  |  3  | 75000 |   4   |\n" +
                "  70   |  1  | 76000 |   7   |\n" +
                "  60   |  1  | 78000 |   7   |\n" +
                "  80   |  3  | 79000 |   4   |\n" +
                "  30   |  3  | 84000 |   4   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testCount() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "count(salary) over (Partition by dept) as c from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | C |\n" +
                "--------------------------\n" +
                "  10   |  1  | 50000 | 7 |\n" +
                "  20   |  1  | 75000 | 7 |\n" +
                "  50   |  1  | 52000 | 7 |\n" +
                "  55   |  1  | 52000 | 7 |\n" +
                "  60   |  1  | 78000 | 7 |\n" +
                "  70   |  1  | 76000 | 7 |\n" +
                "  110  |  1  | 53000 | 7 |\n" +
                "  40   |  2  | 52000 | 4 |\n" +
                "  44   |  2  | 52000 | 4 |\n" +
                "  49   |  2  | 53000 | 4 |\n" +
                "  90   |  2  | 51000 | 4 |\n" +
                "  30   |  3  | 84000 | 4 |\n" +
                "  80   |  3  | 79000 | 4 |\n" +
                "  100  |  3  | 55000 | 4 |\n" +
                "  120  |  3  | 75000 | 4 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeCurrentRowUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between current row and unbounded following) " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |   4   |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |386000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |157000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeUnboundedPrecedingCurrentRow() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and current row) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |154000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |155000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary range between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowsCurrentRowUnboundedFollowingSortOnResult() throws Exception {
        // had to add empnum to fn order by so that results would be deterministic/repeatable
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between current row and unbounded following) " +
                              "from %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |   4   |\n" +
                "------------------------------\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  40   |  2  | 52000 |157000 |\n" +
                "  44   |  2  | 52000 |105000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |238000 |\n" +
                "  80   |  3  | 79000 |163000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  50   |  1  | 52000 |386000 |\n" +
                "  55   |  1  | 52000 |334000 |\n" +
                "  110  |  1  | 53000 |282000 |\n" +
                "  20   |  1  | 75000 |229000 |\n" +
                "  70   |  1  | 76000 |154000 |\n" +
                "  60   |  1  | 78000 | 78000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowsUnboundedPrecedingCurrentRowSortOnResult() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary, empnum rows between unbounded preceding and current row) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 50000 |\n" +
                "  20   |  1  | 75000 |282000 |\n" +
                "  50   |  1  | 52000 |102000 |\n" +
                "  55   |  1  | 52000 |154000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |358000 |\n" +
                "  110  |  1  | 53000 |207000 |\n" +
                "  40   |  2  | 52000 |103000 |\n" +
                "  44   |  2  | 52000 |155000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 | 51000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |209000 |\n" +
                "  100  |  3  | 55000 | 55000 |\n" +
                "  120  |  3  | 75000 |130000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testRowsUnboundedPrecedingUnboundedFollowing() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and unbounded following) as sumsal " +
                              "from %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 |436000 |\n" +
                "  20   |  1  | 75000 |436000 |\n" +
                "  50   |  1  | 52000 |436000 |\n" +
                "  55   |  1  | 52000 |436000 |\n" +
                "  60   |  1  | 78000 |436000 |\n" +
                "  70   |  1  | 76000 |436000 |\n" +
                "  110  |  1  | 53000 |436000 |\n" +
                "  40   |  2  | 52000 |208000 |\n" +
                "  44   |  2  | 52000 |208000 |\n" +
                "  49   |  2  | 53000 |208000 |\n" +
                "  90   |  2  | 51000 |208000 |\n" +
                "  30   |  3  | 84000 |293000 |\n" +
                "  80   |  3  | 79000 |293000 |\n" +
                "  100  |  3  | 55000 |293000 |\n" +
                "  120  |  3  | 75000 |293000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRangeDescOrder() throws Exception {

        {
            String sqlText =
                String.format("SELECT empnum,dept,salary," +
                                  "min(salary) over (Partition by dept ORDER BY salary desc RANGE BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING ) as minsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(TABLE_NAME));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |MINSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 50000 |\n" +
                    "  50   |  1  | 52000 | 50000 |\n" +
                    "  55   |  1  | 52000 | 50000 |\n" +
                    "  60   |  1  | 78000 | 50000 |\n" +
                    "  70   |  1  | 76000 | 50000 |\n" +
                    "  110  |  1  | 53000 | 50000 |\n" +
                    "  40   |  2  | 52000 | 51000 |\n" +
                    "  44   |  2  | 52000 | 51000 |\n" +
                    "  49   |  2  | 53000 | 51000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 55000 |\n" +
                    "  80   |  3  | 79000 | 55000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 55000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }


        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary desc) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(TABLE_NAME));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 |436000 |\n" +
                    "  20   |  1  | 75000 |229000 |\n" +
                    "  50   |  1  | 52000 |386000 |\n" +
                    "  55   |  1  | 52000 |386000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 |154000 |\n" +
                    "  110  |  1  | 53000 |282000 |\n" +
                    "  40   |  2  | 52000 |157000 |\n" +
                    "  44   |  2  | 52000 |157000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 |208000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 |163000 |\n" +
                    "  100  |  3  | 55000 |293000 |\n" +
                    "  120  |  3  | 75000 |238000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }
    }
    @Test
    public void testSingleRow() throws Exception {
        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary range between current row and current row) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(TABLE_NAME));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 75000 |\n" +
                    "  50   |  1  | 52000 |104000 |\n" +
                    "  55   |  1  | 52000 |104000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 | 76000 |\n" +
                    "  110  |  1  | 53000 | 53000 |\n" +
                    "  40   |  2  | 52000 |104000 |\n" +
                    "  44   |  2  | 52000 |104000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 | 79000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 75000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }

        {
            String sqlText =
                String.format("SELECT empnum, dept, salary, " +
                                  "sum(salary) over (Partition by dept ORDER BY salary rows between current row and current row) as sumsal " +
                                  "from %s order by dept, empnum",
                              this.getTableReference(TABLE_NAME));

            ResultSet rs = methodWatcher.executeQuery(sqlText);
            String expected =
                "EMPNUM |DEPT |SALARY |SUMSAL |\n" +
                    "------------------------------\n" +
                    "  10   |  1  | 50000 | 50000 |\n" +
                    "  20   |  1  | 75000 | 75000 |\n" +
                    "  50   |  1  | 52000 | 52000 |\n" +
                    "  55   |  1  | 52000 | 52000 |\n" +
                    "  60   |  1  | 78000 | 78000 |\n" +
                    "  70   |  1  | 76000 | 76000 |\n" +
                    "  110  |  1  | 53000 | 53000 |\n" +
                    "  40   |  2  | 52000 | 52000 |\n" +
                    "  44   |  2  | 52000 | 52000 |\n" +
                    "  49   |  2  | 53000 | 53000 |\n" +
                    "  90   |  2  | 51000 | 51000 |\n" +
                    "  30   |  3  | 84000 | 84000 |\n" +
                    "  80   |  3  | 79000 | 79000 |\n" +
                    "  100  |  3  | 55000 | 55000 |\n" +
                    "  120  |  3  | 75000 | 75000 |";
            assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }
    }

    @Test
    public void testRowNumberWithinPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "ROW_NUMBER() OVER (partition by dept ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | ROWNUMBER |\n" +
                "----------------------------------\n" +
                "  40   |  2  | 52000 |     1     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     3     |\n" +
                "  90   |  2  | 51000 |     4     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  100  |  3  | 55000 |     3     |\n" +
                "  120  |  3  | 75000 |     4     |\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, empnum, salary desc) AS RowNumber " +
                              "FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | ROWNUMBER |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |\n" +
                "  40   |  2  | 52000 |     8     |\n" +
                "  44   |  2  | 52000 |     9     |\n" +
                "  49   |  2  | 53000 |    10     |\n" +
                "  90   |  2  | 51000 |    11     |\n" +
                "  30   |  3  | 84000 |    12     |\n" +
                "  80   |  3  | 79000 |    13     |\n" +
                "  100  |  3  | 55000 |    14     |\n" +
                "  120  |  3  | 75000 |    15     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithinPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS Rank FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  40   |  2  | 52000 |  1  |\n" +
                "  44   |  2  | 52000 |  2  |\n" +
                "  49   |  2  | 53000 |  3  |\n" +
                "  90   |  2  | 51000 |  4  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  100  |  3  | 55000 |  3  |\n" +
                "  120  |  3  | 75000 |  4  |\n" +
                "  10   |  1  | 50000 |  1  |\n" +
                "  20   |  1  | 75000 |  2  |\n" +
                "  50   |  1  | 52000 |  3  |\n" +
                "  55   |  1  | 52000 |  4  |\n" +
                "  60   |  1  | 78000 |  5  |\n" +
                "  70   |  1  | 76000 |  6  |\n" +
                "  110  |  1  | 53000 |  7  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithoutPartiion() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (ORDER BY salary desc) AS Rank FROM %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 | 15  |\n" +
                "  20   |  1  | 75000 |  5  |\n" +
                "  50   |  1  | 52000 | 10  |\n" +
                "  55   |  1  | 52000 | 10  |\n" +
                "  60   |  1  | 78000 |  3  |\n" +
                "  70   |  1  | 76000 |  4  |\n" +
                "  110  |  1  | 53000 |  8  |\n" +
                "  40   |  2  | 52000 | 10  |\n" +
                "  44   |  2  | 52000 | 10  |\n" +
                "  49   |  2  | 53000 |  8  |\n" +
                "  90   |  2  | 51000 | 14  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  100  |  3  | 55000 |  7  |\n" +
                "  120  |  3  | 75000 |  5  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithinPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY salary desc) AS denseank " +
                              "FROM %s order by dept, empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |DENSEANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |    6    |\n" +
                "  20   |  1  | 75000 |    3    |\n" +
                "  50   |  1  | 52000 |    5    |\n" +
                "  55   |  1  | 52000 |    5    |\n" +
                "  60   |  1  | 78000 |    1    |\n" +
                "  70   |  1  | 76000 |    2    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  40   |  2  | 52000 |    2    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    1    |\n" +
                "  90   |  2  | 51000 |    3    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  100  |  3  | 55000 |    4    |\n" +
                "  120  |  3  | 75000 |    3    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore
    public void testXPlainTrace() throws Exception {
        SpliceXPlainTrace xPlainTrace = new SpliceXPlainTrace();
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        long txnId = conn.getCurrentTransactionId();
        try{
            xPlainTrace.setConnection(conn);
            xPlainTrace.turnOnTrace();
            String s = "SELECT empnum, dept, salary, count(salary) over (Partition by dept) as c from %s";
            String sqlText = String.format(s, this.getTableReference(TABLE_NAME));
            long count = conn.count(sqlText);
            Assert.assertEquals(EMPTAB_ROWS.length, count);
            xPlainTrace.turnOffTrace();

            ResultSet rs = conn.query("select * from SYS.SYSSTATEMENTHISTORY where transactionid = " + txnId);
            Assert.assertTrue("XPLAIN does not have a record for this transaction!",rs.next());
            long statementId = rs.getLong("STATEMENTID");
            Assert.assertFalse("No statement id is found!",rs.wasNull());

            XPlainTreeNode operation = xPlainTrace.getOperationTree(statementId);
            Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.PROJECTRESTRICT)==0);
            operation = operation.getChildren().getFirst();
            Assert.assertTrue(operation.getOperationType().compareToIgnoreCase(SpliceXPlainTrace.WINDOW)==0);
            Assert.assertEquals(EMPTAB_ROWS.length, operation.getInputRows());
            Assert.assertEquals(EMPTAB_ROWS.length, operation.getOutputRows());
            Assert.assertEquals(EMPTAB_ROWS.length * 2, operation.getWriteRows());
        }
        catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
        finally{
            conn.rollback();
        }
    }

    @Test
    public void testDenseRankWithoutPartition() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS denseank " +
                              "FROM %s order by dept, empnum" ,
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |DENSEANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |   10    |\n" +
                "  20   |  1  | 75000 |    5    |\n" +
                "  50   |  1  | 52000 |    8    |\n" +
                "  55   |  1  | 52000 |    8    |\n" +
                "  60   |  1  | 78000 |    3    |\n" +
                "  70   |  1  | 76000 |    4    |\n" +
                "  110  |  1  | 53000 |    7    |\n" +
                "  40   |  2  | 52000 |    8    |\n" +
                "  44   |  2  | 52000 |    8    |\n" +
                "  49   |  2  | 53000 |    7    |\n" +
                "  90   |  2  | 51000 |    9    |\n" +
                "  30   |  3  | 84000 |    1    |\n" +
                "  80   |  3  | 79000 |    2    |\n" +
                "  100  |  3  | 55000 |    6    |\n" +
                "  120  |  3  | 75000 |    5    |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumber3OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, ROW_NUMBER() OVER (ORDER BY dept, salary desc, empnum) AS rownum " +
                              "FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |ROWNUM |\n" +
                "------------------------------\n" +
                "  60   |  1  | 78000 |   1   |\n" +
                "  70   |  1  | 76000 |   2   |\n" +
                "  20   |  1  | 75000 |   3   |\n" +
                "  110  |  1  | 53000 |   4   |\n" +
                "  50   |  1  | 52000 |   5   |\n" +
                "  55   |  1  | 52000 |   6   |\n" +
                "  10   |  1  | 50000 |   7   |\n" +
                "  49   |  2  | 53000 |   8   |\n" +
                "  40   |  2  | 52000 |   9   |\n" +
                "  44   |  2  | 52000 |  10   |\n" +
                "  90   |  2  | 51000 |  11   |\n" +
                "  30   |  3  | 84000 |  12   |\n" +
                "  80   |  3  | 79000 |  13   |\n" +
                "  120  |  3  | 75000 |  14   |\n" +
                "  100  |  3  | 55000 |  15   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRank2OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, RANK() OVER (ORDER BY dept, salary desc) AS Rank " +
                              "FROM %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 |  7  |\n" +
                "  20   |  1  | 75000 |  3  |\n" +
                "  30   |  3  | 84000 | 12  |\n" +
                "  40   |  2  | 52000 |  9  |\n" +
                "  44   |  2  | 52000 |  9  |\n" +
                "  49   |  2  | 53000 |  8  |\n" +
                "  50   |  1  | 52000 |  5  |\n" +
                "  55   |  1  | 52000 |  5  |\n" +
                "  60   |  1  | 78000 |  1  |\n" +
                "  70   |  1  | 76000 |  2  |\n" +
                "  80   |  3  | 79000 | 13  |\n" +
                "  90   |  2  | 51000 | 11  |\n" +
                "  100  |  3  | 55000 | 15  |\n" +
                "  110  |  1  | 53000 |  4  |\n" +
                "  120  |  3  | 75000 | 14  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRank2OrderByCols() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY dept, salary desc) AS denserank " +
                              "FROM %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     6     |\n" +
                "  20   |  1  | 75000 |     3     |\n" +
                "  30   |  3  | 84000 |    10     |\n" +
                "  40   |  2  | 52000 |     8     |\n" +
                "  44   |  2  | 52000 |     8     |\n" +
                "  49   |  2  | 53000 |     7     |\n" +
                "  50   |  1  | 52000 |     5     |\n" +
                "  55   |  1  | 52000 |     5     |\n" +
                "  60   |  1  | 78000 |     1     |\n" +
                "  70   |  1  | 76000 |     2     |\n" +
                "  80   |  3  | 79000 |    11     |\n" +
                "  90   |  2  | 51000 |     9     |\n" +
                "  100  |  3  | 55000 |    13     |\n" +
                "  110  |  1  | 53000 |     4     |\n" +
                "  120  |  3  | 75000 |    12     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols_duplicateKey() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  40   |  2  | 52000 |     1     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     3     |\n" +
                "  90   |  2  | 51000 |     4     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  100  |  3  | 55000 |     3     |\n" +
                "  120  |  3  | 75000 |     4     |\n" +
                "  10   |  1  | 50000 |     1     |\n" +
                "  20   |  1  | 75000 |     2     |\n" +
                "  50   |  1  | 52000 |     3     |\n" +
                "  55   |  1  | 52000 |     4     |\n" +
                "  60   |  1  | 78000 |     5     |\n" +
                "  70   |  1  | 76000 |     6     |\n" +
                "  110  |  1  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition2OrderByCols_duplicateKey() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, salary desc) AS DenseRank " +
                              "FROM %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY | DENSERANK |\n" +
                "----------------------------------\n" +
                "  10   |  1  | 50000 |     6     |\n" +
                "  20   |  1  | 75000 |     3     |\n" +
                "  30   |  3  | 84000 |     1     |\n" +
                "  40   |  2  | 52000 |     2     |\n" +
                "  44   |  2  | 52000 |     2     |\n" +
                "  49   |  2  | 53000 |     1     |\n" +
                "  50   |  1  | 52000 |     5     |\n" +
                "  55   |  1  | 52000 |     5     |\n" +
                "  60   |  1  | 78000 |     1     |\n" +
                "  70   |  1  | 76000 |     2     |\n" +
                "  80   |  3  | 79000 |     2     |\n" +
                "  90   |  2  | 51000 |     3     |\n" +
                "  100  |  3  | 55000 |     4     |\n" +
                "  110  |  1  | 53000 |     4     |\n" +
                "  120  |  3  | 75000 |     3     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols_KeyColMissingFromSelect() throws Exception {
        String sqlText =
            String.format("SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank " +
                              "FROM %s",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |SALARY | DENSERANK |\n" +
                "----------------------------\n" +
                "  40   | 52000 |     1     |\n" +
                "  44   | 52000 |     2     |\n" +
                "  49   | 53000 |     3     |\n" +
                "  90   | 51000 |     4     |\n" +
                "  30   | 84000 |     1     |\n" +
                "  80   | 79000 |     2     |\n" +
                "  100  | 55000 |     3     |\n" +
                "  120  | 75000 |     4     |\n" +
                "  10   | 50000 |     1     |\n" +
                "  20   | 75000 |     2     |\n" +
                "  50   | 52000 |     3     |\n" +
                "  55   | 52000 |     4     |\n" +
                "  60   | 78000 |     5     |\n" +
                "  70   | 76000 |     6     |\n" +
                "  110  | 53000 |     7     |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDenseRankWithoutPartitionOrderby() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank " +
                              "FROM %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPNUM |DEPT |SALARY |RANK |\n" +
                "----------------------------\n" +
                "  10   |  1  | 50000 | 10  |\n" +
                "  20   |  1  | 75000 |  5  |\n" +
                "  30   |  3  | 84000 |  1  |\n" +
                "  40   |  2  | 52000 |  8  |\n" +
                "  44   |  2  | 52000 |  8  |\n" +
                "  49   |  2  | 53000 |  7  |\n" +
                "  50   |  1  | 52000 |  8  |\n" +
                "  55   |  1  | 52000 |  8  |\n" +
                "  60   |  1  | 78000 |  3  |\n" +
                "  70   |  1  | 76000 |  4  |\n" +
                "  80   |  3  | 79000 |  2  |\n" +
                "  90   |  2  | 51000 |  9  |\n" +
                "  100  |  3  | 55000 |  6  |\n" +
                "  110  |  1  | 53000 |  7  |\n" +
                "  120  |  3  | 75000 |  5  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartitionOrderby() throws Exception {
        // DB-1683
        String sqlText =
            String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number, dob " +
                              "from %s order by PersonID",
                          this.getTableReference(TABLE3_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |         DOB          |\n" +
                "-------------------------------------------------------------------------\n" +
                "    1    |    1    |    Joe    | Johnson |   4   |2000-10-23 13:00:00.0 |\n" +
                "    1    |    1    |    Joe    | Johnson |   5   |2000-10-23 13:00:00.0 |\n" +
                "    1    |    1    |    Joe    | Johnson |   6   |2000-10-23 13:00:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |   9   |2001-12-15 05:45:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |  10   |2001-12-15 05:45:00.0 |\n" +
                "    2    |    1    |    Jim    | Johnson |  11   |2001-12-15 05:45:00.0 |\n" +
                "    3    |    2    |   Karly   |Matthews |   1   |2000-05-20 04:00:00.0 |\n" +
                "    3    |    2    |   Karly   |Matthews |   2   |2000-05-20 04:00:00.0 |\n" +
                "    4    |    2    |   Kacy    |Matthews |   3   |2000-05-20 04:02:00.0 |\n" +
                "    5    |    2    |    Tom    |Matthews |   7   |2001-09-15 11:52:00.0 |\n" +
                "    5    |    2    |    Tom    |Matthews |   8   |2001-09-15 11:52:00.0 |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartitionOrderby_OrderbyColNotInSelect() throws Exception {
        // DB-1683
        String sqlText =
            String.format("select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number " +
                              "from %s order by PersonID",
                          this.getTableReference(TABLE3_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "PERSONID |FAMILYID | FIRSTNAME |LASTNAME |NUMBER |\n" +
                "--------------------------------------------------\n" +
                "    1    |    1    |    Joe    | Johnson |   4   |\n" +
                "    1    |    1    |    Joe    | Johnson |   5   |\n" +
                "    1    |    1    |    Joe    | Johnson |   6   |\n" +
                "    2    |    1    |    Jim    | Johnson |   9   |\n" +
                "    2    |    1    |    Jim    | Johnson |  10   |\n" +
                "    2    |    1    |    Jim    | Johnson |  11   |\n" +
                "    3    |    2    |   Karly   |Matthews |   1   |\n" +
                "    3    |    2    |   Karly   |Matthews |   2   |\n" +
                "    4    |    2    |   Kacy    |Matthews |   3   |\n" +
                "    5    |    2    |    Tom    |Matthews |   7   |\n" +
                "    5    |    2    |    Tom    |Matthews |   8   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testScalarAggWithOrderBy() throws Exception {
        // DB-1775
        String sqlText =
            String.format("SELECT sum(price) over (Partition by item ORDER BY date) as  sumprice from %s",
                          this.getTableReference(TABLE2_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "SUMPRICE |\n" +
                "----------\n" +
                "  10.0   |\n" +
                "  12.0   |\n" +
                "  20.0   |\n" +
                "   6.0   |\n" +
                "  11.0   |\n" +
                "  23.0   |\n" +
                "   3.0   |\n" +
                "  10.0   |\n" +
                "  20.0   |\n" +
                "   1.0   |\n" +
                "   2.0   |\n" +
                "   9.0   |\n" +
                "  11.0   |\n" +
                "  15.0   |\n" +
                "  25.0   |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774
        String sqlText =
            String.format("SELECT item, price, sum(price) over (Partition by item ORDER BY date) as sumsal, date " +
                              "from %s",
                          this.getTableReference(TABLE2_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "ITEM | PRICE |SUMSAL |         DATE           |\n" +
                "-----------------------------------------------\n" +
                "  4  | 10.0  | 10.0  |2014-09-08 17:50:17.182 |\n" +
                "  4  |  2.0  | 12.0  |2014-09-08 18:05:47.166 |\n" +
                "  4  |  8.0  | 20.0  |2014-09-08 18:08:04.986 |\n" +
                "  2  |  6.0  |  6.0  |2014-09-08 17:50:17.182 |\n" +
                "  2  |  5.0  | 11.0  |2014-09-08 18:26:51.387 |\n" +
                "  2  | 12.0  | 23.0  |2014-09-08 18:40:15.48  |\n" +
                "  3  |  3.0  |  3.0  |2014-09-08 17:36:55.414 |\n" +
                "  3  |  7.0  | 10.0  |2014-09-08 18:00:44.742 |\n" +
                "  3  | 10.0  | 20.0  |2014-09-08 18:25:42.387 |\n" +
                "  1  |  1.0  |  1.0  |2014-09-08 17:45:15.204 |\n" +
                "  1  |  1.0  |  2.0  |2014-09-08 18:27:48.881 |\n" +
                "  1  |  7.0  |  9.0  |2014-09-08 18:33:46.446 |\n" +
                "  5  | 11.0  | 11.0  |2014-09-08 17:41:56.353 |\n" +
                "  5  |  4.0  | 15.0  |2014-09-08 17:46:26.428 |\n" +
                "  5  | 10.0  | 25.0  |2014-09-08 18:11:23.645 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testWindowFunctionWithGroupBy() throws Exception {

        String sqlText =
            "select empnum, dept, sum(salary)," +
                "rank() over(partition by dept order by salary desc) rank " +
                "from %s " +
                "group by empnum, dept order by empnum";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        String expected =
            "EMPNUM |DEPT |  3   |RANK |\n" +
                "---------------------------\n" +
                "  10   |  1  |50000 |  7  |\n" +
                "  20   |  1  |75000 |  3  |\n" +
                "  30   |  3  |84000 |  1  |\n" +
                "  40   |  2  |52000 |  2  |\n" +
                "  44   |  2  |52000 |  2  |\n" +
                "  49   |  2  |53000 |  1  |\n" +
                "  50   |  1  |52000 |  5  |\n" +
                "  55   |  1  |52000 |  5  |\n" +
                "  60   |  1  |78000 |  1  |\n" +
                "  70   |  1  |76000 |  2  |\n" +
                "  80   |  3  |79000 |  2  |\n" +
                "  90   |  2  |51000 |  4  |\n" +
                "  100  |  3  |55000 |  4  |\n" +
                "  110  |  1  |53000 |  4  |\n" +
                "  120  |  3  |75000 |  3  |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMaxInOrderBy() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "max(salary) over (Partition by dept) as maxsal " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY |MAXSAL |\n" +
                "------------------------------\n" +
                "  10   |  1  | 50000 | 78000 |\n" +
                "  20   |  1  | 75000 | 78000 |\n" +
                "  30   |  3  | 84000 | 84000 |\n" +
                "  40   |  2  | 52000 | 53000 |\n" +
                "  44   |  2  | 52000 | 53000 |\n" +
                "  49   |  2  | 53000 | 53000 |\n" +
                "  50   |  1  | 52000 | 78000 |\n" +
                "  55   |  1  | 52000 | 78000 |\n" +
                "  60   |  1  | 78000 | 78000 |\n" +
                "  70   |  1  | 76000 | 78000 |\n" +
                "  80   |  3  | 79000 | 84000 |\n" +
                "  90   |  2  | 51000 | 53000 |\n" +
                "  100  |  3  | 55000 | 84000 |\n" +
                "  110  |  1  | 53000 | 78000 |\n" +
                "  120  |  3  | 75000 | 84000 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankInOrderBy() throws Exception {
        String sqlText =
            String.format("SELECT empnum, dept, salary, " +
                              "rank() over (Partition by dept order by salary) as salrank " +
                              "from %s order by empnum",
                          this.getTableReference(TABLE_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "EMPNUM |DEPT |SALARY | SALRANK |\n" +
                "--------------------------------\n" +
                "  10   |  1  | 50000 |    1    |\n" +
                "  20   |  1  | 75000 |    5    |\n" +
                "  30   |  3  | 84000 |    4    |\n" +
                "  40   |  2  | 52000 |    2    |\n" +
                "  44   |  2  | 52000 |    2    |\n" +
                "  49   |  2  | 53000 |    4    |\n" +
                "  50   |  1  | 52000 |    2    |\n" +
                "  55   |  1  | 52000 |    2    |\n" +
                "  60   |  1  | 78000 |    7    |\n" +
                "  70   |  1  | 76000 |    6    |\n" +
                "  80   |  3  | 79000 |    3    |\n" +
                "  90   |  2  | 51000 |    1    |\n" +
                "  100  |  3  | 55000 |    1    |\n" +
                "  110  |  1  | 53000 |    4    |\n" +
                "  120  |  3  | 75000 |    2    |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRankWithAggAsOrderByCol() throws Exception {
        // have to order by ename here because it's the only col with unique values and forces repeatable results
        String sqlText =
            String.format("select sum(sal), " +
                              "rank() over ( order by sum(sal) ) empsal, ename from %s " +
                              "group by ename order by ename",
                          this.getTableReference(TABLE4_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "1    |EMPSAL | ENAME |\n" +
                "-------------------------\n" +
                "1100.00 |   3   | ADAMS |\n" +
                "1600.00 |   8   | ALLEN |\n" +
                "2850.00 |  10   | BLAKE |\n" +
                "2450.00 |   9   | CLARK |\n" +
                "3000.00 |  12   | FORD  |\n" +
                "950.00  |   2   | JAMES |\n" +
                "2975.00 |  11   | JONES |\n" +
                "5000.00 |  14   | KING  |\n" +
                "1250.00 |   4   |MARTIN |\n" +
                "1300.00 |   6   |MILLER |\n" +
                "3000.00 |  12   | SCOTT |\n" +
                "800.00  |   1   | SMITH |\n" +
                "1500.00 |   7   |TURNER |\n" +
                "1250.00 |   4   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test(expected=SQLException.class)
    public void testRankWithAggAsOrderByColNoGroupBy() throws Exception {
        // expecting an exception here because there's an aggregate with no
        // group by specified for ename column
        String sqlText =
            "select sal, rank() over ( order by sum(sal) ) empsal, ename from %s";

        methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE4_NAME)));
    }

    @Test
    public void testRankWith2AggAsOrderByCol() throws Exception {
        // have to order by ename here because it's the only col with unique values and forces repeatable results
        String sqlText =
            String.format("select sum(sal) as sum_sal, avg(sal) as avg_sal, " +
                              "rank() over ( order by sum(sal), avg(sal) ) empsal, ename " +
                              "from %s group by ename order by ename",
                          this.getTableReference(TABLE4_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "SUM_SAL | AVG_SAL  |EMPSAL | ENAME |\n" +
                "-------------------------------------\n" +
                " 1100.00 |1100.0000 |   3   | ADAMS |\n" +
                " 1600.00 |1600.0000 |   8   | ALLEN |\n" +
                " 2850.00 |2850.0000 |  10   | BLAKE |\n" +
                " 2450.00 |2450.0000 |   9   | CLARK |\n" +
                " 3000.00 |3000.0000 |  12   | FORD  |\n" +
                " 950.00  |950.0000  |   2   | JAMES |\n" +
                " 2975.00 |2975.0000 |  11   | JONES |\n" +
                " 5000.00 |5000.0000 |  14   | KING  |\n" +
                " 1250.00 |1250.0000 |   4   |MARTIN |\n" +
                " 1300.00 |1300.0000 |   6   |MILLER |\n" +
                " 3000.00 |3000.0000 |  12   | SCOTT |\n" +
                " 800.00  |800.0000  |   1   | SMITH |\n" +
                " 1500.00 |1500.0000 |   7   |TURNER |\n" +
                " 1250.00 |1250.0000 |   4   | WARD  |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testMediaForDept() throws Exception {
        // DB-1650, DB-2020
        String sqlText = String.format("SELECT %1$s.Nome_Dep, %2$s.Nome AS Funcionario, %2$s.Salario, " +
                                           "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) \"Média por " +
                                           "Departamento\", " +
                                           "%2$s.Salario - AVG(%2$s.Salario) as \"Diferença de Salário\" FROM %2$s " +
                                           "INNER" +
                                           " JOIN %1$s ON %2$s.ID_Dep = %1$s.ID group by %1$s.Nome_Dep," +
                                           "%2$s.Nome, %2$s.Salario ORDER BY 3 DESC, 1",
                                       this.getTableReference(TABLE5a_NAME), this.getTableReference(TABLE5b_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |       0.0000        |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |       0.0000        |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |       0.0000        |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |       0.0000        |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6666       |       0.0000        |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |       0.0000        |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testConstMinusAvg1() throws Exception {
        // DB-2124
        String sqlText = String.format("SELECT %2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM %2$s " +
                                           "INNER JOIN %1$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID",
                                       this.getTableReference(TABLE5a_NAME), this.getTableReference(TABLE5b_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "Diferença de Salário |\n" +
                "----------------------\n" +
                "     -9166.3333      |\n" +
                "      9333.6666      |\n" +
                "      -167.3333      |\n" +
                "     -2500.0000      |\n" +
                "     -2000.0000      |\n" +
                "      4500.0000      |\n" +
                "     -3499.6666      |\n" +
                "     -2999.6666      |\n" +
                "      6499.3333      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testConstMinusAvg2() throws Exception {
        // DB-2124
        String sqlText = String.format("SELECT %1$s.Nome_Dep, " +
                                           "%2$s.Nome AS Funcionario, " +
                                           "%2$s.Salario, " +
                                           "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) " +
                                           "\"Média por Departamento\", " +
                                           "%2$s.Salario - AVG(%2$s.Salario) OVER(PARTITION BY " +
                                           "%1$s.Nome_Dep) \"Diferença de Salário\" FROM %2$s " +
                                           "INNER JOIN %1$s " +
                                           "ON %2$s.ID_Dep = %1$s.ID ORDER BY 5 DESC",
                                       this.getTableReference(TABLE5a_NAME), this.getTableReference(TABLE5b_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "NOME_DEP     | FUNCIONARIO | SALARIO |Média por Departamento |Diferença de Salário |\n" +
                "----------------------------------------------------------------------------------------\n" +
                "Recursos Humanos |   Luciano   |23500.00 |      14166.3333       |      9333.6666      |\n" +
                "       IT        |   Nogare    |11999.00 |       5499.6666       |      6499.3333      |\n" +
                "     Vendas      |    Diego    | 9000.00 |       4500.0000       |      4500.0000      |\n" +
                "Recursos Humanos |  Zavaschi   |13999.00 |      14166.3333       |      -167.3333      |\n" +
                "     Vendas      |   Amorim    | 2500.00 |       4500.0000       |     -2000.0000      |\n" +
                "     Vendas      |   Fabiano   | 2000.00 |       4500.0000       |     -2500.0000      |\n" +
                "       IT        |  Ferreira   | 2500.00 |       5499.6666       |     -2999.6666      |\n" +
                "       IT        |   Felipe    | 2000.00 |       5499.6666       |     -3499.6666      |\n" +
                "Recursos Humanos |   Laerte    | 5000.00 |      14166.3333       |     -9166.3333      |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testSumTimesConstDivSum() throws Exception {
        // DB-2086 - identical agg gets removed from aggregates array
        String sqlText = String.format("SELECT SUM(%2$s.Salario) * 100 / " +
                                           "SUM(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) " +
                                           "\"Média por Departamento\" " +
                                           "FROM %2$s, %1$s GROUP BY %1$s.Nome_Dep",
                                       this.getTableReference(TABLE5a_NAME), this.getTableReference(TABLE5b_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "Média por Departamento |\n" +
                "------------------------\n" +
                "       3624.9000       |\n" +
                "       3624.9000       |\n" +
                "       3624.9000       |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170RankOverView() throws Exception {
        String sqlText =
            String.format("select yr, rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s", this.getTableReference(VIEW_NAME));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "YR | EMPRANK | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                "81 |    1    | ALLEN |1981-02-20 |\n" +
                "81 |    2    | WARD  |1981-02-22 |\n" +
                "81 |    3    | JONES |1981-04-02 |\n" +
                "81 |    4    | BLAKE |1981-05-01 |\n" +
                "81 |    5    | CLARK |1981-06-09 |\n" +
                "81 |    6    |TURNER |1981-09-08 |\n" +
                "81 |    7    |MARTIN |1981-09-28 |\n" +
                "81 |    8    | KING  |1981-11-17 |\n" +
                "81 |    9    | FORD  |1981-12-03 |\n" +
                "81 |    9    | JAMES |1981-12-03 |\n" +
                "83 |    1    | ADAMS |1983-01-12 |\n" +
                "82 |    1    |MILLER |1982-01-23 |\n" +
                "82 |    2    | SCOTT |1982-12-09 |\n" +
                "80 |    1    | SMITH |1980-12-17 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170RankOverViewMissingKey() throws Exception {
        String sqlText =
            String.format("select rank() over ( partition by yr order by hiredate ) as EMPRANK, ename," +
                              "hiredate from %s", this.getTableReference(VIEW_NAME));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "EMPRANK | ENAME | HIREDATE  |\n" +
                "------------------------------\n" +
                "    1    | ALLEN |1981-02-20 |\n" +
                "    2    | WARD  |1981-02-22 |\n" +
                "    3    | JONES |1981-04-02 |\n" +
                "    4    | BLAKE |1981-05-01 |\n" +
                "    5    | CLARK |1981-06-09 |\n" +
                "    6    |TURNER |1981-09-08 |\n" +
                "    7    |MARTIN |1981-09-28 |\n" +
                "    8    | KING  |1981-11-17 |\n" +
                "    9    | FORD  |1981-12-03 |\n" +
                "    9    | JAMES |1981-12-03 |\n" +
                "    1    | ADAMS |1983-01-12 |\n" +
                "    1    |MILLER |1982-01-23 |\n" +
                "    2    | SCOTT |1982-12-09 |\n" +
                "    1    | SMITH |1980-12-17 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    @Ignore("DB-2170 - window function over view. (works periodically, why?)")
    public void testDB2170MaxOverView() throws Exception {
        String sqlText =
            String.format("select max(hiredate) over () as maxhiredate, ename," +
                              "hiredate from %s order by ename", this.getTableReference(VIEW_NAME));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1983-01-12  | ALLEN |1981-02-20 |\n" +
                " 1983-01-12  | BLAKE |1981-05-01 |\n" +
                " 1983-01-12  | CLARK |1981-06-09 |\n" +
                " 1983-01-12  | FORD  |1981-12-03 |\n" +
                " 1983-01-12  | JAMES |1981-12-03 |\n" +
                " 1983-01-12  | JONES |1981-04-02 |\n" +
                " 1983-01-12  | KING  |1981-11-17 |\n" +
                " 1983-01-12  |MARTIN |1981-09-28 |\n" +
                " 1983-01-12  |MILLER |1982-01-23 |\n" +
                " 1983-01-12  | SCOTT |1982-12-09 |\n" +
                " 1983-01-12  | SMITH |1980-12-17 |\n" +
                " 1983-01-12  |TURNER |1981-09-08 |\n" +
                " 1983-01-12  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testDB2170MaxOverTable() throws Exception {
        String sqlText =
            String.format("select max(hiredate) over () as maxhiredate, ename, hiredate from %s order by ename",
                          this.getTableReference(TABLE4_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1983-01-12  | ALLEN |1981-02-20 |\n" +
                " 1983-01-12  | BLAKE |1981-05-01 |\n" +
                " 1983-01-12  | CLARK |1981-06-09 |\n" +
                " 1983-01-12  | FORD  |1981-12-03 |\n" +
                " 1983-01-12  | JAMES |1981-12-03 |\n" +
                " 1983-01-12  | JONES |1981-04-02 |\n" +
                " 1983-01-12  | KING  |1981-11-17 |\n" +
                " 1983-01-12  |MARTIN |1981-09-28 |\n" +
                " 1983-01-12  |MILLER |1982-01-23 |\n" +
                " 1983-01-12  | SCOTT |1982-12-09 |\n" +
                " 1983-01-12  | SMITH |1980-12-17 |\n" +
                " 1983-01-12  |TURNER |1981-09-08 |\n" +
                " 1983-01-12  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
    @Test
    public void testDB2170WithAggOverView() throws Exception {
        String sqlText =
            String.format("select max(hiredate) as maxhiredate, ename,hiredate from %s group by ename, hiredate order by ename",
                          this.getTableReference(VIEW_NAME));
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
            "MAXHIREDATE | ENAME | HIREDATE  |\n" +
                "----------------------------------\n" +
                " 1983-01-12  | ADAMS |1983-01-12 |\n" +
                " 1981-02-20  | ALLEN |1981-02-20 |\n" +
                " 1981-05-01  | BLAKE |1981-05-01 |\n" +
                " 1981-06-09  | CLARK |1981-06-09 |\n" +
                " 1981-12-03  | FORD  |1981-12-03 |\n" +
                " 1981-12-03  | JAMES |1981-12-03 |\n" +
                " 1981-04-02  | JONES |1981-04-02 |\n" +
                " 1981-11-17  | KING  |1981-11-17 |\n" +
                " 1981-09-28  |MARTIN |1981-09-28 |\n" +
                " 1982-01-23  |MILLER |1982-01-23 |\n" +
                " 1982-12-09  | SCOTT |1982-12-09 |\n" +
                " 1980-12-17  | SMITH |1980-12-17 |\n" +
                " 1981-09-08  |TURNER |1981-09-08 |\n" +
                " 1981-02-22  | WARD  |1981-02-22 |";
        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
