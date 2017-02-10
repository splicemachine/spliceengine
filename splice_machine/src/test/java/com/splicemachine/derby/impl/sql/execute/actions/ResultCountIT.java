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

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Test that we return the correct number of rows for updates
 * (PreparedStatement.executeUpdate() : int)
 *
 * @author Jeff Cunningham
 *         Date: 6/11/13
 */
public class ResultCountIT { 
    private static final String CLASS_NAME = ResultCountIT.class.getSimpleName().toUpperCase();

    private static final List<String> tasksVals = Arrays.asList(
            "('001', 100, 0100, 0200)",
            "('002', 110, 0100, 0200)",
            "('003', 120, 0100, 0200)",
            "('004', 130, 0100, 0200)",
            "('005', 140, 0100, 0200)");

    private static final List<String> empVals = Arrays.asList(
            "(100, 'jcunningham', 'Jeff', 'Cunningham')",
            "(110, 'jblow', 'Joe', 'Blow')",
            "(120, 'fziffle', 'Fred', 'Ziffle')",
            "(130, 'mstuart', 'Martha', 'Stuart')",
            "(140, 'fkruger', 'Freddy', 'Kruger')");

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher tableSchema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final String TASK_TABLE_NAME = "Tasks";
    private static final String TASK_TABLE_DEF = "(TaskId varchar(5), empId int, StartedAt INT, FinishedAt INT)";
    protected static SpliceTableWatcher taskTable = new SpliceTableWatcher(TASK_TABLE_NAME,CLASS_NAME, TASK_TABLE_DEF);

    private static final String EMP_TABLE_NAME = "Emp";
    private static final String EMP_TABLE_DEF = "(empId int, userId varchar(15), fname varchar(10), lname varchar(20))";
    protected static SpliceTableWatcher empTable = new SpliceTableWatcher(EMP_TABLE_NAME,CLASS_NAME, EMP_TABLE_DEF);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(tableSchema)
            .around(empTable)
            .around(taskTable)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        Connection conn = spliceClassWatcher.getOrCreateConnection();

                        //  load tasks table
                        for (String rowVal : tasksVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + taskTable.toString() + " values " + rowVal);
                        }

                        //  load emp table
                        for (String rowVal : empVals) {
                            spliceClassWatcher.getStatement().executeUpdate("insert into " + empTable.toString() + " values " + rowVal);
                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    /**
     * Test prepared statement insert returns correct number of inserted rows.
     * @throws Exception
     */
    @Test
    public void testInsert() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);

        // insert good data
        PreparedStatement ps = methodWatcher.prepareStatement(
                String.format("insert into %s.%s (taskId, empId, startedAt, finishedAt) values (?,?,?,?)",
                        tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setString(1, "1011");
        ps.setInt(2, 101);
        ps.setInt(3, 0600);
        ps.setInt(4, 0700);
        int rows = ps.executeUpdate();

        Assert.assertEquals(1, rows);
    }

    /**
     * Test prepared statement update returns correct number of updated rows.
     * @throws Exception
     */
    @Test
    public void testUpdate() throws Exception {
        String theTaskId = "999";
        String query = String.format("select empId from %s.%s where taskId = '%s'",
                tableSchema.schemaName, TASK_TABLE_NAME, theTaskId);

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        // insert good data
        PreparedStatement ps = methodWatcher.prepareStatement(
                String.format("insert into %s.%s (taskId, empId, startedAt, finishedAt) values (?,?,?,?)",
                        tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setString(1, theTaskId);
        ps.setInt(2, 101);
        ps.setInt(3, 0600);
        ps.setInt(4, 0700);
        int rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);

        ResultSet rs = connection.createStatement().executeQuery(query);
        rs.next();
        Assert.assertEquals(101, rs.getInt(1));
        Assert.assertFalse("Only one row expected.", rs.next());

        ps = methodWatcher.prepareStatement(
                String.format("update %s.%s set empId = ? where taskId = ?", tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setInt(1, 102);
        ps.setString(2, theTaskId);
        rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);

        rs = connection.createStatement().executeQuery(query);
        rs.next();
        Assert.assertEquals(102, rs.getInt(1));
        Assert.assertFalse("Only one row expected.", rs.next());
    }

    /**
     * Test prepared statement delete returns correct number of deleted rows.
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        String theTaskId = "222";
        String query = String.format("select empId from %s.%s where taskId = '%s'",
                tableSchema.schemaName, TASK_TABLE_NAME, theTaskId);

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        // insert good data
        PreparedStatement ps = methodWatcher.prepareStatement(
                String.format("insert into %s.%s (taskId, empId, startedAt, finishedAt) values (?,?,?,?)",
                        tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setString(1, theTaskId);
        ps.setInt(2, 101);
        ps.setInt(3, 0600);
        ps.setInt(4, 0700);
        int rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);
        connection.commit();

        ResultSet rs = connection.createStatement().executeQuery(query);
        rs.next();
        Assert.assertEquals(101, rs.getInt(1));
        Assert.assertFalse("Only one row expected.", rs.next());
        connection.commit();

        ps = methodWatcher.prepareStatement(
                String.format("delete from %s.%s where taskId = ?", tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setString(1, theTaskId);
        rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);
        connection.commit();

        rs = connection.createStatement().executeQuery(query);
        Assert.assertFalse("No rows expected.", rs.next());
        connection.commit();
    }

    /**
     * Test prepared statement delete returns correct number of deleted rows.
     * @throws Exception
     */
    @Test
    public void testDeleteSeveral() throws Exception {
        int theTaskId = 440;
        String query = String.format("select * from %s.%s where taskId like '%s'",
                tableSchema.schemaName, TASK_TABLE_NAME, "4%");

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        PreparedStatement ps;
        int rows;
        int total = 10;
        for (int i=0; i<total; i++) {
            // insert good data
            ps = methodWatcher.prepareStatement(
                    String.format("insert into %s.%s (taskId, empId, startedAt, finishedAt) values (?,?,?,?)",
                            tableSchema.schemaName, TASK_TABLE_NAME));
            ps.setString(1, Integer.toString(theTaskId + i));
            ps.setInt(2, 100+i);
            ps.setInt(3, 0600+i);
            ps.setInt(4, 0700+i);
            rows = ps.executeUpdate();
            Assert.assertEquals(1, rows);
        }
        connection.commit();

        ResultSet rs = connection.createStatement().executeQuery(query);
        Assert.assertEquals(10, SpliceUnitTest.resultSetSize(rs));
        connection.commit();

        ps = methodWatcher.prepareStatement(
                String.format("delete from %s.%s where taskId like ?", tableSchema.schemaName, TASK_TABLE_NAME));
        ps.setString(1, "4%");
        rows = ps.executeUpdate();
        Assert.assertEquals(10, rows);
        connection.commit();

        rs = connection.createStatement().executeQuery(query);
        Assert.assertFalse("No rows expected.", rs.next());
        connection.commit();
    }

    @Test
    public void testDeleteInSubselect() throws Exception {
        String query = String.format("select * from %s.%s where empId = 130",
                tableSchema.schemaName, TASK_TABLE_NAME);

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        int rows;
        PreparedStatement ps = methodWatcher.prepareStatement(
                // delete from tasks where tasks.empId in (select emp.empId from emp, tasks where emp.empId=tasks.empId and emp.userId='fkruger');
                String.format("delete from %1$s.%2$s where %1$s.%2$s.empId in (select %1$s.%3$s.empId from %1$s.%3$s, %1$s.%2$s where %1$s.%3$s.empId=%1$s.%2$s.empId and %1$s.%3$s.userId=?)",
                        tableSchema.schemaName, TASK_TABLE_NAME, EMP_TABLE_NAME));
        ps.setString(1, "mstuart");
        rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);

        ResultSet rs = connection.createStatement().executeQuery(query);
        Assert.assertFalse("No rows expected.", rs.next());
    }

    @Test
    public void testDeleteInSimplifiedSubselect() throws Exception {
        TestUtils.tableLookupByNumber(spliceClassWatcher);
        String query = String.format("select * from %s.%s where empId = 130",
                tableSchema.schemaName, TASK_TABLE_NAME);

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        int rows;
        PreparedStatement ps = methodWatcher.prepareStatement(
            // delete from tasks where tasks.empId in (select emp.empId from emp where emp.userId='fkruger');
            String.format("delete from %1$s.%2$s where %1$s.%2$s.empId in (select %1$s.%3$s.empId from %1$s.%3$s where %1$s.%3$s.userId=?)",
                tableSchema.schemaName, TASK_TABLE_NAME, EMP_TABLE_NAME));
        ps.setString(1, "mstuart");
        rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);

        ResultSet rs = connection.createStatement().executeQuery(query);
        Assert.assertFalse("No rows expected.", rs.next());
    }

    @Test
    public void testDeleteInValue() throws Exception {
        String query = String.format("select * from %s.%s where empId = 140",
                tableSchema.schemaName, TASK_TABLE_NAME);

        TestConnection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);

        // do select
        PreparedStatement ps = methodWatcher.prepareStatement(
                String.format("select %1$s.%3$s.empId from %1$s.%3$s, %1$s.%2$s where %1$s.%3$s.empId=%1$s.%2$s.empId and %1$s.%3$s.userId=?",
                        tableSchema.schemaName, TASK_TABLE_NAME, EMP_TABLE_NAME));
        ps.setString(1, "fkruger");
        ResultSet rs = ps.executeQuery();
        connection.commit();
        Integer val = (Integer)((Map.Entry)TestUtils.resultSetToMaps(rs).get(0).entrySet().iterator().next()).getValue();
        Assert.assertEquals(new Integer(140), val);

        // do delete using value from select
        int rows;
        ps = methodWatcher.prepareStatement(
                String.format("delete from %1$s.%2$s where %1$s.%2$s.empId in (%3$d)",
                        tableSchema.schemaName, TASK_TABLE_NAME, val));
        rows = ps.executeUpdate();
        Assert.assertEquals(1, rows);
        connection.commit();

        rs = connection.createStatement().executeQuery(query);
        Assert.assertFalse("No rows expected.", rs.next());
        connection.commit();
    }

    private void printMap(List<Map> mapRs) {
        for(Map map : mapRs) {
            // System.out.println();
            for (Object entryObj : map.entrySet()) {
                Map.Entry entry = (Map.Entry) entryObj;
                // System.out.println(entry.getKey() + ": " + entry.getValue());
            }
        }
    }
}
