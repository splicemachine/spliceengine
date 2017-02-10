/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import com.splicemachine.test_dao.TableDAO;
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

    /**
     * Test for bug DB-551 - order by changes the values of null fields for most data types.
     * Was:
     *
     * splice> select * from tbl order by 1,2,3,4,5,6;
     * I |D |DA |T |TP |VC
     * -----------------------------------------------------------------------------------------------
     * 0 |0.0 |1969-12-31|16:00:00|1969-12-31 16:00:00.0 |NULL
     * 1 row selected
     *
     * Where expected behavior is that all returned cols should be NULL
     * @throws Exception
     */
    @Test
    public void testSelectNullsWithOrderBy() throws Exception {
        String TABLE_NAME = "tbl";
        SpliceUnitTest.MyWatcher tableWatcher =
                new SpliceUnitTest.MyWatcher(TABLE_NAME,CLASS_NAME,
                        "(i int, d double, da date, t time, tp timestamp, vc varchar(10))");
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(CLASS_NAME, TABLE_NAME);
        tableWatcher.create(Description.createSuiteDescription(CLASS_NAME, "testSelectNullsWithOrderBy"));
        Connection connection = methodWatcher.getOrCreateConnection();
        Statement statement = connection.createStatement();

        // insert nulls
        statement.execute(String.format("insert into %s.%s values (null,null,null,null,null,null)", CLASS_NAME, TABLE_NAME));
        connection.commit();

        ResultSet rs = methodWatcher.getOrCreateConnection().createStatement().executeQuery(
                        String.format("select * from %s.%s", CLASS_NAME, TABLE_NAME));
        Assert.assertTrue(rs.next());
        ResultSetMetaData meta = rs.getMetaData();
        for (int i=1; i<=meta.getColumnCount(); i++) {
            Assert.assertNull(rs.getObject(i));
        }

        rs = methodWatcher.getOrCreateConnection().createStatement().executeQuery(
                        String.format("select * from %s.%s order by 1,2,3,4,5,6", CLASS_NAME, TABLE_NAME));
        Assert.assertTrue(rs.next());
        meta = rs.getMetaData();
        for (int i=1; i<=meta.getColumnCount(); i++) {
            Assert.assertNull(rs.getObject(i));
        }
    }
}
