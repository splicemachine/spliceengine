package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Jeff Cunningham
 *         Date: 6/24/13
 */
public class NullNumericalComparisonIT { 

    public static final String CLASS_NAME = NullNumericalComparisonIT.class.getSimpleName().toUpperCase();
    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static final List<String> empHourVals = Arrays.asList(
            "('E1','P2',20)",
            "('E2','P1',40)",
            "('E3','P2',20)",
            "('E4','P5',80)",
            "('E5',NULL,80)",
            "('E8','P8',NULL)");

    public static final String EMP_HOUR_TABLE = "works";
    private static String eHourDef = "(EMPNUM VARCHAR(2) NOT NULL, PNUM VARCHAR(2), HOURS DECIMAL(5))";
    protected static SpliceTableWatcher empHourTable = new SpliceTableWatcher(EMP_HOUR_TABLE,CLASS_NAME, eHourDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(empHourTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        for (String rowVal : empHourVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empHourTable.toString() + " values " + rowVal);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static void verifyResults(String context, ResultSet rs, int expectedCnt, boolean printMap) throws SQLException {
        List<Map> maps = TestUtils.resultSetToMaps(rs);
        if (printMap) {
            System.out.println(context);
        }
        if (maps.isEmpty()) {
            if (printMap) {
                System.out.println("No results");
            }
            Assert.assertEquals(expectedCnt,0);
            return;
        }
        int i = 0;
        for(Map map : maps) {
            i++;
            if (printMap) {
                System.out.println(i+":");
                for (Object entryObj : map.entrySet()) {
                    Map.Entry entry = (Map.Entry) entryObj;
                    System.out.println("    "+entry.getKey() + ": " + entry.getValue());
                }
            }
        }
        Assert.assertEquals(expectedCnt,i);
    }

    public void helpTestQuery(String query, int expectedCnt) throws Exception {
        Connection connection = methodWatcher.createConnection();
        ResultSet resultSet = connection.createStatement().executeQuery(query);
        verifyResults(query, resultSet, expectedCnt, false);
    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testStringNullNumericalComparisonGT() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE PNUM > (SELECT PNUM FROM %1$s WHERE EMPNUM = 'E5')",
                empHourTable.toString());
        helpTestQuery(query, 0);
    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testNullNumericalComparisonGT() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE HOURS > (SELECT HOURS FROM %1$s WHERE EMPNUM = 'E8')",
                empHourTable.toString());
        helpTestQuery(query, 0);
    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testNullNumericalComparisonLT() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE HOURS < (SELECT HOURS FROM %1$s WHERE EMPNUM = 'E8')",
                empHourTable.toString());
        helpTestQuery(query, 0);

    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testNullNumericalComparisonET() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE HOURS = (SELECT HOURS FROM %1$s WHERE EMPNUM = 'E8')",
                empHourTable.toString());
        helpTestQuery(query, 0);

    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testNullNumericalComparisonNE() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE HOURS != (SELECT HOURS FROM %1$s WHERE EMPNUM = 'E8')",
                empHourTable.toString());
        helpTestQuery(query,0);

    }

    /**
     * Test for bug 507 - Numerical comparison in queries with sub selects returning NULL
     * values should not return results
     * @throws Exception
     */
    @Test
    public void testNullNumericalComparisonNLT() throws Exception {
        String query = String.format("SELECT EMPNUM FROM %1$s WHERE NOT HOURS < (SELECT HOURS FROM %1$s WHERE EMPNUM = 'E8')",
                empHourTable.toString());
        helpTestQuery(query,0);

    }
}
