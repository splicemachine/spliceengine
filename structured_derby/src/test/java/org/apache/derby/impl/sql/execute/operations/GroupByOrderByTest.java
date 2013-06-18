package org.apache.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 *
 * @author Jeff Cunningham
 * Date: 5/31/13
 *
 */
public class GroupByOrderByTest {
    private static List<String> t1Values = Arrays.asList(
            "('E1','P1',40)",
            "('E1','P2',20)",
            "('E1','P3',80)",
            "('E1','P4',20)",
            "('E1','P5',12)",
            "('E1','P6',12)",
            "('E2','P1',40)",
            "('E2','P2',80)",
            "('E3','P2',20)",
            "('E4','P2',20)",
            "('E4','P4',40)",
            "('E4','P5',80)");

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(GroupByOrderByTest.class.getSimpleName());
    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("WORKS",schemaWatcher.schemaName,
            "(EMPNUM VARCHAR(3) NOT NULL, PNUM VARCHAR(3) NOT NULL,HOURS DECIMAL(5))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        //  load t3
                        for (String rowVal : t1Values) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into "+ t1Watcher.toString()+" values " + rowVal);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testSelectAllColumns() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("select EMPNUM,PNUM,HOURS from %1$s", t1Watcher.toString()));
        System.out.println("testSelectAllColumns");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));
    }

    @Test
    public void testSelectAllColumnsGroupBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM, HOURS FROM %1$s GROUP BY PNUM,EMPNUM,HOURS", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupBy");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));
    }

    @Test
    public void testSelectAllColumnsGroupBySum() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM, sum(HOURS) AS SUM_HOURS FROM %1$s GROUP BY PNUM,EMPNUM", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupBySum");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "SUM_HOURS"), Arrays.asList("SUM_HOURS"), true));
    }

    @Test
    public void testSelectAllColumnsOrderBy() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM,HOURS FROM %1$s ORDER BY PNUM,EMPNUM,HOURS", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsOrderBy");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));
    }

    @Test
    public void testSelectAllColumnsGroupByOrderBy() throws Exception {
        TestUtils.tableLookupByNumber(spliceClassWatcher);
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM,HOURS FROM %1$s GROUP BY PNUM,EMPNUM,HOURS ORDER BY " +
                        "PNUM, EMPNUM, HOURS", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupByOrderBy");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));
    }

    private static int verifyColumns(ResultSet rs, List<String> expectedColNames,
                                     List<String> excludedColNames, boolean print) throws Exception {
        ResultSetMetaData meta = rs.getMetaData();
        List<String> actualColNames = new ArrayList<String>(meta.getColumnCount());
        for (int i = 0; i < meta.getColumnCount(); i++) {
            actualColNames.add(meta.getColumnName(i+1));
        }

        List<String> errors = new ArrayList<String>();
        List<List<String>> rows = new ArrayList<List<String>>();
        while (rs.next()) {
            List<String> row = new ArrayList<String>();
            for (int j = 0; j < actualColNames.size(); j++) {
                String expectedColName = expectedColNames.get(j);
                String actualColumn = rs.getObject(j+1).toString();
                row.add(actualColumn);
                if (! excludedColNames.contains(expectedColName) && ! actualColumn.startsWith(expectedColName.substring(0, 1))) {
                    errors.add((rows.size()+1) +":"+ (j+1) + " ["+ actualColumn +
                            "] did not match expected column ["+ expectedColName +"]");
                }
            }
            rows.add(row);
        }

        Assert.assertEquals(printResults(Arrays.asList("Column names didn't match: "), actualColNames, rows), expectedColNames, actualColNames);
        Assert.assertFalse(printResults(errors, actualColNames, rows), errors.size() > 0);
        if (print) {
            String results = printResults(Collections.EMPTY_LIST, actualColNames, rows);
            System.out.println(results);
        }

        return rows.size();
    }

    private static String printResults(List<String> msgs, List<String> colNames, List<List<String>> rowSet) throws Exception {
        StringBuilder buf = new StringBuilder("\n");
        for (String msg : msgs) {
            buf.append(msg);
            buf.append('\n');
        }

        for (String colName : colNames) {
            buf.append(colName);
            buf.append("  ");
        }
        buf.append('\n');

        for (List<String> row : rowSet) {
            for (String col : row) {
                buf.append("  ");
                buf.append(col);
                buf.append("  ");
            }
            buf.append('\n');
        }
        return buf.toString();
    }
}
