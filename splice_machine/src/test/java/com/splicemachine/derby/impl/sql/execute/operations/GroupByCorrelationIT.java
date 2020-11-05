/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

public class GroupByCorrelationIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(GroupByCorrelationIT.class.getSimpleName());
    protected static SpliceTableWatcher otWatcher = new SpliceTableWatcher("OUTER_TABLE", schemaWatcher.schemaName, "(oc1 VARCHAR(3), oc2 VARCHAR(3), oc3 INT)");
    protected static SpliceTableWatcher itWatcher = new SpliceTableWatcher("INNER_TABLE", schemaWatcher.schemaName, "(ic1 INTEGER, ic2 VARCHAR(3))");
    private static List<String> otValues = Arrays.asList(
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
    private static List<String> itValues = Arrays.asList(
            "(2, 'E1')",
            "(2, 'E1')",
            "(2, 'E1')",
            "(2, 'E1')",
            "(2, 'E1')",
            "(2, 'E1')",
            "(4, 'E2')",
            "(4, 'E2')",
            "(4, 'E2')",
            "(4, 'E2')",
            "(4, 'E2')",
            "(5, 'E3')");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(otWatcher)
            .around(itWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        // load OUTER_TABLE
                        for (String rowVal : otValues) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + otWatcher.toString() + " values " + rowVal);
                        }
                        // load INNER_TABLE
                        for (String rowVal : itValues) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + itWatcher.toString() + " values " + rowVal);
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

    @Test
    public void ssqWithCorrelationOnGroupByColumnWorksCorrectly() throws Exception {
        try(ResultSet resultSet = methodWatcher.executeQuery(String.format("SELECT oc1, (SELECT MAX(ic1) FROM %s WHERE ic2 = %s.oc1) FROM %s GROUP BY oc1 ORDER BY oc1 asc",
                                                                           itWatcher.toString(), otWatcher.toString(), otWatcher.toString()))) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals(2, resultSet.getInt(2));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E2", resultSet.getString(1));Assert.assertEquals(4, resultSet.getInt(2));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E3", resultSet.getString(1));Assert.assertEquals(5, resultSet.getInt(2));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E4", resultSet.getString(1));resultSet.getInt(2); Assert.assertTrue(resultSet.wasNull());
            Assert.assertFalse(resultSet.next());
        }
        try(ResultSet resultSet = methodWatcher.executeQuery(String.format("SELECT oc1, oc2, (SELECT MAX(ic1) FROM %s WHERE ic2 = %s.oc1) FROM %s GROUP BY oc1, oc2 order by oc1 asc, oc2 asc",
                                                                           itWatcher.toString(), otWatcher.toString(), otWatcher.toString()))) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P1", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P2", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P3", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P4", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P5", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E1", resultSet.getString(1));Assert.assertEquals("P6", resultSet.getString(2));Assert.assertEquals(2, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E2", resultSet.getString(1));Assert.assertEquals("P1", resultSet.getString(2));Assert.assertEquals(4, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E2", resultSet.getString(1));Assert.assertEquals("P2", resultSet.getString(2));Assert.assertEquals(4, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E3", resultSet.getString(1));Assert.assertEquals("P2", resultSet.getString(2));Assert.assertEquals(5, resultSet.getInt(3));
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E4", resultSet.getString(1));Assert.assertEquals("P2", resultSet.getString(2));resultSet.getInt(3); Assert.assertTrue(resultSet.wasNull());
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E4", resultSet.getString(1));Assert.assertEquals("P4", resultSet.getString(2));resultSet.getInt(3); Assert.assertTrue(resultSet.wasNull());
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals("E4", resultSet.getString(1));Assert.assertEquals("P5", resultSet.getString(2));resultSet.getInt(3); Assert.assertTrue(resultSet.wasNull());
            Assert.assertFalse(resultSet.next());
        }
    }

    @Test
    public void ssqWithoutCorrelationOnGroupByColumnThrows() throws Exception {
        try(ResultSet resultSet = methodWatcher.executeQuery(String.format("SELECT oc1, (SELECT MAX(ic1) FROM %s WHERE ic2 = %s.oc2) FROM %s GROUP BY oc1 order by oc1 asc",
                                                             itWatcher.toString(), otWatcher.toString(), otWatcher.toString()))) {
            Assert.fail("expected exception containing message: The SELECT list of a grouped query contains at least one invalid expression. " +
                                "If a SELECT list has a GROUP BY, the list may only contain valid grouping expressions and valid aggregate expressions.");
        } catch(Exception se) {
            Assert.assertTrue(se instanceof SQLException);
            Assert.assertEquals("42Y30", ((SQLException)se).getSQLState());
            Assert.assertTrue(se.getMessage().contains("The SELECT list of a grouped query contains at least one invalid expression. If a SELECT list has a GROUP BY, " +
                                                               "the list may only contain valid grouping expressions and valid aggregate expressions."));
        }
    }

    @Test
    public void subqueryWithCorrelationOnGroupByColumnThrows() throws Exception {
        try(ResultSet resultSet = methodWatcher.executeQuery(String.format("SELECT oc1, (SELECT ic2 FROM %s WHERE ic2 = %s.oc1) FROM %s GROUP BY oc1 order by oc1 asc",
                                                                           itWatcher.toString(), otWatcher.toString(), otWatcher.toString()))) {
            Assert.fail("expected exception containing message: Scalar subquery is only allowed to return a single row.");
        } catch(Exception se) {
            Assert.assertTrue(se instanceof SQLException);
            Assert.assertEquals("21000", ((SQLException) se).getSQLState());
            Assert.assertTrue(se.getMessage().contains("Scalar subquery is only allowed to return a single row."));
        }
    }
}
