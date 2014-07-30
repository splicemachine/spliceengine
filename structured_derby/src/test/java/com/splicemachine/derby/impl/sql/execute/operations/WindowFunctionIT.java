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
}
