package com.splicemachine.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.derby.management.XPlainTreeNode;

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
                            String sql = String.format("insert into %s values (%s)", spliceTableWatcher2, row);
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
                                String.format("insert into %s values (%s)", spliceTableWatcher3, row));
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
                                String.format("insert into %s values (%s)", spliceTableWatcher4, row));
                            ps.execute();
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher5a)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        for (String row : DEPT) {
                            ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (Nome_Dep) values (%s)", spliceTableWatcher5a, row));
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
                                String.format("insert into %s (ID_Dep, Nome, Salario) values (%s)", spliceTableWatcher5b, row));
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
            int[] result = {50000, 154000, 154000, 207000, 282000, 358000, 436000, 51000, 155000, 155000, 208000, 55000, 130000, 209000, 293000};
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
            int[] result = {50000, 51333, 51333, 51750, 56400, 59666, 62285, 51000, 51666, 51666, 52000, 55000, 65000, 69666, 73250};
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
            int[] result = {1, 3, 3, 4, 5, 6, 7, 1, 3, 3, 4, 1, 2, 3, 4};
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
            int[] result = {436000, 386000, 386000, 282000, 229000, 154000, 78000, 208000, 157000, 157000, 53000, 293000, 238000, 163000, 84000};
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
            int[] result = {50000, 154000, 154000, 207000, 282000, 358000, 436000, 51000, 155000, 155000, 208000, 55000, 130000, 209000, 293000};
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
    public void TestRowsUnbounded() throws Exception {
        {
            int[] result = {436000, 386000, 334000, 282000, 229000, 154000, 78000, 208000, 157000, 105000, 53000, 293000, 238000, 163000, 84000};
            String sqlText =
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between current row and unbounded following) from %s";
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
                    "SELECT empnum, dept, salary, sum(salary) over (Partition by dept ORDER BY salary rows between unbounded preceding and current row) as sumsal from %s";

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
            int[] result = {78000, 154000, 229000, 282000, 386000, 386000, 436000, 53000, 157000, 157000, 208000, 84000, 163000, 238000, 293000};
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
            int[] result = {50000, 104000, 104000, 53000, 75000, 76000, 78000, 51000, 104000, 104000, 53000, 55000, 75000, 79000, 84000};
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
        }finally{
            conn.rollback();
        }
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
    public void testDenseRankWithPartition3OrderByCols_duplicateKey() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] salary = {50000, 75000, 52000, 52000, 78000, 76000, 53000, 52000, 52000, 53000, 51000, 84000, 79000, 55000, 75000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(salary[i],rs.getInt(3));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition2OrderByCols_duplicateKey() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 5, 6, 1, 2, 2, 3, 1, 2, 3, 4};
        int[] salary = {78000, 76000, 75000,  53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000,  75000,  55000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, salary desc) AS DenseRank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(salary[i],rs.getInt(3));
            Assert.assertEquals(denseRank[i],rs.getInt(4));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRankWithPartition3OrderByCols_KeyColMissingFromSelect() throws Exception {
        int[] denseRank = {1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 1, 2, 3, 4};
        int[] salary = {50000, 75000, 52000, 52000, 78000, 76000, 53000, 52000, 52000, 53000, 51000, 84000, 79000, 55000, 75000};
        String sqlText =
            "SELECT empnum, salary, DENSE_RANK() OVER (PARTITION BY dept ORDER BY dept, empnum, salary desc) AS DenseRank FROM %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(salary[i],rs.getInt(2));
            Assert.assertEquals(denseRank[i],rs.getInt(3));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testDenseRankWithoutPartitionOrderby() throws Exception {
        int[] result = {1, 2, 3, 4, 5, 5, 6, 7, 7, 8, 8, 8, 8, 9, 10};
        int[] colVal = {84000, 79000, 78000, 76000, 75000, 75000, 55000, 53000, 53000, 52000, 52000, 52000, 52000, 51000, 50000};
        String sqlText =
            "SELECT empnum, dept, salary, DENSE_RANK() OVER (ORDER BY salary desc) AS Rank FROM %s order by salary desc";

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
    public void testRowNumberWithoutPartitionOrderby() throws Exception {
        // DB-1683
        int[] personID = {1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 5};
        int[] number = { 4,  5,  6,  9, 10, 11,  1,  2,  3,  7,  8};
        String sqlText =
            "select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number, dob from %s order by PersonID";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE3_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(personID[i],rs.getInt(1));
            Assert.assertEquals(number[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testRowNumberWithoutPartitionOrderby_OrderbyColNotInSelect() throws Exception {
        // DB-1683
        int[] personID = {1, 1, 1, 2, 2, 2, 3, 3, 4, 5, 5};
        int[] number = { 4,  5,  6,  9, 10, 11,  1,  2,  3,  7,  8};
        String sqlText =
            "select PersonID,FamilyID,FirstName,LastName, ROW_NUMBER() over (order by DOB) as Number from %s order by PersonID";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE3_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(personID[i],rs.getInt(1));
            Assert.assertEquals(number[i],rs.getInt(5));
            ++i;
        }
        rs.close();
    }

    @Test
    public void testScalarAggWithOrderBy() throws Exception {
        // DB-1775
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
    public void testSelectAllColsScalarAggWithOrderBy() throws Exception {
        // DB-1774
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
    public void testWindowFunctionWithGroupBy() throws Exception {

        int[] col2 = {1, 1, 1, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3};
        int[] col3 = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        int[] col4 = {1, 2, 3, 4, 5, 5, 7, 1, 2, 2, 4, 1, 2, 3, 4};
        String sqlText =
                "select empnum, dept, sum(salary)," +
                "rank() over(partition by dept order by salary desc) rank " +
                "from %s " +
                "group by empnum, dept";

        ResultSet rs = methodWatcher.executeQuery(
                String.format(sqlText, this.getTableReference(TABLE_NAME)));

        int i = 0;
        while (rs.next()) {
            Assert.assertEquals(col2[i],rs.getInt(2));
            Assert.assertEquals(col3[i],rs.getInt(3));
            Assert.assertEquals(col4[i],rs.getInt(4));
            i++;
        }
        rs.close();
    }

    @Test
    public void testMaxInOrderBy() throws Exception {

        int[] salaryExpected = {55000, 75000, 79000, 84000, 50000, 52000, 52000, 53000, 75000, 76000, 78000, 51000, 52000, 52000, 53000};
        int[] maxSalExpected = {84000, 84000, 84000, 84000, 78000, 78000, 78000, 78000, 78000, 78000, 78000, 53000, 53000, 53000, 53000};
        String sqlText =
            "SELECT empnum, dept, salary, max(salary) over (Partition by dept) as maxsal from %s order by maxsal desc, salary";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        List<Integer> salaryActual = new ArrayList<Integer>(salaryExpected.length);
        List<Integer> maxSalActual = new ArrayList<Integer>(maxSalExpected.length);
        while (rs.next()) {
            salaryActual.add(rs.getInt(3));
            maxSalActual.add(rs.getInt(4));
        }
        rs.close();

        compareArrays(salaryExpected, salaryActual);
        compareArrays(maxSalExpected, maxSalActual);
    }

    @Test
    public void testRankInOrderBy() throws Exception {

        int[] salaryExpected = {78000, 76000, 75000, 53000, 52000, 52000, 50000, 53000, 52000, 52000, 51000, 84000, 79000, 75000, 55000};
        int[] maxSalExpected = {7, 6, 5, 4, 2, 2, 1, 4, 2, 2, 1, 4, 3, 2, 1};
        String sqlText =
            "SELECT empnum, dept, salary, rank() over (Partition by dept order by salary) as salrank from %s order by dept, salrank desc";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE_NAME)));

        List<Integer> salaryActual = new ArrayList<Integer>(salaryExpected.length);
        List<Integer> maxSalActual = new ArrayList<Integer>(maxSalExpected.length);
        while (rs.next()) {
            salaryActual.add(rs.getInt(3));
            maxSalActual.add(rs.getInt(4));
        }
        rs.close();

        compareArrays(salaryExpected, salaryActual);
        compareArrays(maxSalExpected, maxSalActual);
    }

    @Test
    public void testRankWithAggAsOrderByCol() throws Exception {

        double[] col1Expected = { 800,  950, 1100, 1250, 1250, 1300, 1500, 1600, 2450, 2850, 2975, 3000, 3000, 5000};
        int[] empSalExpected = { 1,  2,  3,  4,  4,  6,  7,  8,  9, 10, 11, 12, 12, 14};
        String[] enameExpected = { "SMITH",  "JAMES",  "ADAMS", "MARTIN",   "WARD", "MILLER", "TURNER",  "ALLEN",  "CLARK",  "BLAKE",  "JONES", "SCOTT", "FORD",   "KING"};
        String sqlText =
            "select sum(sal), rank() over ( order by sum(sal) ) empsal, ename from %s group by ename";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE4_NAME)));

        List<Double> col1Actual = new ArrayList<Double>(col1Expected.length);
        List<Integer> empSalActual = new ArrayList<Integer>(empSalExpected.length);
        List<String> enameActual = new ArrayList<String>(enameExpected.length);
        while (rs.next()) {
            col1Actual.add(rs.getDouble(1));
            empSalActual.add(rs.getInt(2));
            enameActual.add(rs.getString(3));
        }
        rs.close();

        compareArrays(col1Expected, col1Actual);
        compareArrays(empSalExpected, empSalActual);
        // enames, SCOTT and FORD have same salary and so rank the same and results
        // order the names arbitrarily, therefore, we can't use ename with static comparison.
//        compareArrays(enameExpected, enameActual);
    }

    @Test(expected=SQLException.class)
    public void testRankWithAggAsOrderByColNoGroupBy() throws Exception {
        // expecting an exception here because there's an aggregate with no
        // group by specified for ename column
        String sqlText =
            "select sal, rank() over ( order by sum(sal) ) empsal, ename from %s";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE4_NAME)));
    }

    @Test
    public void testRankWith2AggAsOrderByCol() throws Exception {

        double[] col1Expected = { 800,  950, 1100, 1250, 1250, 1300, 1500, 1600, 2450, 2850, 2975, 3000, 3000, 5000};
        int[] empSalExpected = { 1,  2,  3,  4,  4,  6,  7,  8,  9, 10, 11, 12, 12, 14};
        String[] enameExpected = { "SMITH",  "JAMES",  "ADAMS", "MARTIN",   "WARD", "MILLER", "TURNER",  "ALLEN",  "CLARK",  "BLAKE",  "JONES", "SCOTT", "FORD",   "KING"};
        String sqlText =
            "select sum(sal) as sum_sal, avg(sal) as avg_sal, rank() over ( order by sum(sal), avg(sal) ) empsal, ename from %s group by ename";

        ResultSet rs = methodWatcher.executeQuery(
            String.format(sqlText, this.getTableReference(TABLE4_NAME)));

        List<Double> col1Actual = new ArrayList<Double>(col1Expected.length);
        List<Integer> empSalActual = new ArrayList<Integer>(empSalExpected.length);
        List<String> enameActual = new ArrayList<String>(enameExpected.length);
        while (rs.next()) {
            col1Actual.add(rs.getDouble(1));
            empSalActual.add(rs.getInt(3));
            enameActual.add(rs.getString(4));
        }
        rs.close();

        compareArrays(col1Expected, col1Actual);
        compareArrays(empSalExpected, empSalActual);
        // enames, SCOTT and FORD have same salary and so rank the same and results
        // order the names arbitrarily, therefore, we can't use ename with static comparison.
//        compareArrays(enameExpected, enameActual);
    }

    @Test
    public void testMediaForDept() throws Exception {
        // DB-1650, DB-2020
        String[] funcionarioExpected = { "Luciano", "Zavaschi",   "Nogare",    "Diego",   "Laerte", "Ferreira", "Amorim", "Felipe",  "Fabiano"};
        double[] mpdExpected = {14166.3333, 14166.3333, 5499.6666, 4500.0000, 14166.3333, 5499.6666, 4500.0000, 5499.6666, 4500.0000};

        String sqlText = String.format("SELECT %1$s.Nome_Dep, %2$s.Nome AS Funcionario, %2$s.Salario, " +
                "AVG(%2$s.Salario) OVER(PARTITION BY %1$s.Nome_Dep) \"Média por Departamento\", " +
                "%2$s.Salario - AVG(%2$s.Salario) as \"Diferença de Salário\" FROM %2$s INNER" +
                " JOIN %1$s ON %2$s.ID_Dep = %1$s.ID group by %1$s.Nome_Dep," +
                "%2$s.Nome, %2$s.Salario ORDER BY 3 DESC",
                                       this.getTableReference(TABLE5a_NAME), this.getTableReference(TABLE5b_NAME));

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        List<String> funcionarioActual = new ArrayList<String>(funcionarioExpected.length);
        List<Double> mpdActual = new ArrayList<Double>(mpdExpected.length);
        while (rs.next()) {
            funcionarioActual.add(rs.getString(2));
            mpdActual.add(rs.getDouble(4));
        }
        rs.close();

        compareArrays(funcionarioExpected, funcionarioActual);
        compareArrays(mpdExpected, mpdActual);
    }

    private static void compareArrays(int[] expected, List<Integer> actualList) {
        int[] actual = new int[actualList.size()];
        for (int i=0; i<actualList.size(); i++) {
            actual[i] = actualList.get(i);
        }
        Assert.assertArrayEquals(expected, actual);
    }

    private static void compareArrays(double[] expected, List<Double> actualList) {
        double[] actual = new double[actualList.size()];
        for (int i=0; i<actualList.size(); i++) {
            actual[i] = actualList.get(i);
        }
        Assert.assertArrayEquals(expected, actual, 0.0);
    }

    private static void compareArrays(String[] expected, List<String> actualList) {
        String[] actual = new String[actualList.size()];
        for (int i=0; i<actualList.size(); i++) {
            actual[i] = (actualList.get(i));
        }
        Assert.assertArrayEquals(expected, actual);
    }
}
