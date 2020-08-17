/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author Jeff Cunningham
 * Date: 5/31/13
 *
 */
public class GroupByOrderByIT { 
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

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(GroupByOrderByIT.class.getSimpleName());
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

        /* test group by position number */
        rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM, HOURS FROM %1$s GROUP BY 2,1,3", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupBy");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));

        // group by position must be 1-based
        try {
            methodWatcher.executeQuery(String.format("SELECT EMPNUM, PNUM, HOURS FROM %1$s GROUP BY 1,0,2", t1Watcher.toString()));
            Assert.fail("expect group by position error");
        } catch (SQLException e) {
            Assert.assertEquals("42X77", e.getSQLState());
        }

        // group by position must be positive
        try {
            methodWatcher.executeQuery(String.format("SELECT EMPNUM, PNUM, HOURS FROM %1$s GROUP BY -1", t1Watcher.toString()));
            Assert.fail("expect group by position error");
        } catch (SQLException e) {
            Assert.assertEquals("42X77", e.getSQLState());
        }
    }

    @Test
    public void testSelectAllColumnsGroupBySum() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM, sum(HOURS) AS SUM_HOURS FROM %1$s GROUP BY PNUM,EMPNUM", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupBySum");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "SUM_HOURS"), Arrays.asList("SUM_HOURS"), true));

        /* test group by position number */
        rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM, sum(HOURS) AS SUM_HOURS FROM %1$s GROUP BY 2,1", t1Watcher.toString()));
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

        /* test group by position number */
        rs = methodWatcher.executeQuery(
                String.format("SELECT EMPNUM,PNUM,HOURS FROM %1$s GROUP BY 2,1,3 ORDER BY " +
                        "PNUM, EMPNUM, HOURS", t1Watcher.toString()));
        System.out.println("testSelectAllColumnsGroupByOrderBy");
        Assert.assertEquals(12, verifyColumns(rs, Arrays.asList("EMPNUM", "PNUM", "HOURS"), Arrays.asList("HOURS"), true));
    }

    @Test
    public void testGroupByPositionNumber() throws Exception {
        /* Q1 */
        String sqlText = String.format("SELECT EMPNUM, sum(HOURS), PNUM, count(*) AS SUM_HOURS FROM %1$s GROUP BY 3,1 order by 1,3", t1Watcher.toString());
        String expected = "EMPNUM | 2 |PNUM | SUM_HOURS |\n" +
                "------------------------------\n" +
                "  E1   |40 | P1  |     1     |\n" +
                "  E1   |20 | P2  |     1     |\n" +
                "  E1   |80 | P3  |     1     |\n" +
                "  E1   |20 | P4  |     1     |\n" +
                "  E1   |12 | P5  |     1     |\n" +
                "  E1   |12 | P6  |     1     |\n" +
                "  E2   |40 | P1  |     1     |\n" +
                "  E2   |80 | P2  |     1     |\n" +
                "  E3   |20 | P2  |     1     |\n" +
                "  E4   |20 | P2  |     1     |\n" +
                "  E4   |40 | P4  |     1     |\n" +
                "  E4   |80 | P5  |     1     |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q2 */
        sqlText = String.format("SELECT EMPNUM, sum(HOURS), count(*) AS SUM_HOURS FROM %1$s GROUP BY 1 order by 1", t1Watcher.toString());
        expected = "EMPNUM | 2  | SUM_HOURS |\n" +
                "-------------------------\n" +
                "  E1   |184 |     6     |\n" +
                "  E2   |120 |     2     |\n" +
                "  E3   |20  |     1     |\n" +
                "  E4   |140 |     3     |";
        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q3 test expression */
        sqlText = String.format("SELECT EMPNUM || '-' || PNUM, sum(HOURS), count(*) AS SUM_HOURS FROM %1$s GROUP BY 1 order by 1", t1Watcher.toString());
        expected = "1   | 2 | SUM_HOURS |\n" +
                "-----------------------\n" +
                "E1-P1 |40 |     1     |\n" +
                "E1-P2 |20 |     1     |\n" +
                "E1-P3 |80 |     1     |\n" +
                "E1-P4 |20 |     1     |\n" +
                "E1-P5 |12 |     1     |\n" +
                "E1-P6 |12 |     1     |\n" +
                "E2-P1 |40 |     1     |\n" +
                "E2-P2 |80 |     1     |\n" +
                "E3-P2 |20 |     1     |\n" +
                "E4-P2 |20 |     1     |\n" +
                "E4-P4 |40 |     1     |\n" +
                "E4-P5 |80 |     1     |";
        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 test rollup */
        sqlText = String.format("SELECT EMPNUM, sum(HOURS), PNUM, count(*) AS SUM_HOURS FROM %1$s GROUP BY rollup(1,3) order by 1,3", t1Watcher.toString());
        expected = "EMPNUM | 2  |PNUM | SUM_HOURS |\n" +
                "-------------------------------\n" +
                "  E1   |40  | P1  |     1     |\n" +
                "  E1   |20  | P2  |     1     |\n" +
                "  E1   |80  | P3  |     1     |\n" +
                "  E1   |20  | P4  |     1     |\n" +
                "  E1   |12  | P5  |     1     |\n" +
                "  E1   |12  | P6  |     1     |\n" +
                "  E1   |184 |NULL |     6     |\n" +
                "  E2   |40  | P1  |     1     |\n" +
                "  E2   |80  | P2  |     1     |\n" +
                "  E2   |120 |NULL |     2     |\n" +
                "  E3   |20  | P2  |     1     |\n" +
                "  E3   |20  |NULL |     1     |\n" +
                "  E4   |20  | P2  |     1     |\n" +
                "  E4   |40  | P4  |     1     |\n" +
                "  E4   |80  | P5  |     1     |\n" +
                "  E4   |140 |NULL |     3     |\n" +
                " NULL  |464 |NULL |    12     |";
        rs = methodWatcher.executeQuery(sqlText);
        resultString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals("\n" + sqlText + "\n" + "expected result: " + expected + "\n,actual result: " + resultString, expected, resultString);
        rs.close();

        /* Q4 negative test case */
        sqlText = String.format("SELECT EMPNUM || '-' || PNUM, sum(HOURS), count(*) AS SUM_HOURS FROM %1$s GROUP BY 2", t1Watcher.toString());
        expected = "";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_AGGREGATE_IN_GROUPBY_LIST);
        }

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
