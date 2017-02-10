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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.*;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.RowId;

/**
 * Created by jyuan on 9/28/14.
 */
public class RowIdIT extends SpliceUnitTest {
    public static final String CLASS_NAME = RowIdIT.class.getSimpleName().toUpperCase();
    public static int nRows = 3;
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1_NAME = "A";
    public static final String TABLE2_NAME = "B";
    public static final String TABLE3_NAME = "C";
    public static final String TABLE4_NAME = "D";
    public static final String TABLE5_NAME = "E";

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    private static String tableDef2 = "(I INT, J INT)";
    private static String tableDef3 = "(I INT, J INT, primary key(i))";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher(TABLE3_NAME,CLASS_NAME, tableDef2);
    protected static SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher(TABLE4_NAME,CLASS_NAME, tableDef3);
    protected static SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher(TABLE5_NAME,CLASS_NAME, tableDef3);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher1));
                        for (int i = 0; i < nRows; i++) {
                            ps.setInt(1, i);
                            ps.execute();
                        }
                    }  catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher2).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher2));
                        for (int i = 0; i < nRows; i++) {
                            ps.setInt(1, i);
                            ps.execute();
                        }
                    }  catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher3).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s (i) values (?)", spliceTableWatcher3));
                        ps.setInt(1, 1);
                        ps.execute();

                        ps = spliceClassWatcher.prepareStatement(
                                String.format("create index ti on  %s (i)", spliceTableWatcher3));
                        ps.execute();
                    }  catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            })
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(new SpliceDataWatcher() {

                private void populateTable(SpliceTableWatcher spliceTableWatcher) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(
                                String.format("insert into %s values (?,?)", spliceTableWatcher));
                        for (int i = 0; i < nRows; ++i) {
                            ps.setInt(1, i);
                            ps.setInt(2, i);
                            ps.addBatch();
                        }
                        ps.executeBatch();

                    }  catch (Exception e) {
                    throw new RuntimeException(e);
                }
                }
                @Override
                protected void starting(Description description) {
                    populateTable(spliceTableWatcher4);
                    populateTable(spliceTableWatcher5);
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testRowIdForOneTable() throws Exception {

        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s", this.getTableReference(TABLE1_NAME)));

        while (rs.next()) {
            RowId rowId = rs.getRowId("rowid");
            String s = rs.getString(1);
            Assert.assertTrue(s.compareToIgnoreCase(rowId.toString()) == 0);
        }
    }

    @Test
    public void testUpdateWithSubquery() throws Exception {

        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i = 0", this.getTableReference(TABLE1_NAME)));

        RowId rowId1 = null;
        while (rs.next()) {
            rowId1 = rs.getRowId("rowid");
        }
        rs.close();

        methodWatcher.executeUpdate(
                String.format("update %s set i=1000 where rowid = (select rowid from %s where i = 0)",
                        this.getTableReference(TABLE1_NAME), this.getTableReference(TABLE1_NAME)));

        rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i = 1000", this.getTableReference(TABLE1_NAME)));
        RowId rowId2 = null;
        while (rs.next()) {
            rowId2 = rs.getRowId("rowid");
            int i = rs.getInt("i");

            Assert.assertEquals(rowId1, rowId2);
            Assert.assertEquals(i, 1000);
        }
    }

    @Test
    public void testRowIdForJoin() throws Exception {

        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select a.rowid, a.i, b.rowid, b.i from %s a, %s b where a.i=b.i",
                        this.getTableReference(TABLE1_NAME), this.getTableReference(TABLE2_NAME)));

        while (rs.next()) {
            RowId rowId = rs.getRowId(1);
            String s = rs.getString(1);
            Assert.assertTrue(s.compareToIgnoreCase(rowId.toString()) == 0);

            rowId = rs.getRowId(3);
            s = rs.getString(3);
            Assert.assertTrue(s.compareToIgnoreCase(rowId.toString()) == 0);
        }
    }

    @Test
    @Ignore
    public void testStringConversion() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select t1.rowid, t1.i from %s t1, %s t2 where cast(t1.rowid as varchar(128))=cast(t2.rowid as varchar(128))",
                        this.getTableReference(TABLE1_NAME), this.getTableReference(TABLE1_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(nRows, count);
    }

    @Test
    public void testRowIdAsPredicate() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select t1.rowid, t1.i from %s t1, %s t2 where t1.rowid = t2.rowid",
                        this.getTableReference(TABLE1_NAME), this.getTableReference(TABLE1_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertEquals(nRows, count);
    }

    @Test
    public void testUpdate() throws Exception {

        // Get rowid for a row
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i=1", this.getTableReference(TABLE1_NAME)));

        String rowId = null;
        while (rs.next()) {
            rowId = rs.getString(1);
        }
        rs.close();

        // Change its column value according rowid
        methodWatcher.executeUpdate(String.format("update %s set i=10 where rowid =\'%s\'",
                this.getTableReference(TABLE1_NAME), rowId));

        // verify column value changed for the specified row
        rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i=10", this.getTableReference(TABLE1_NAME)));

        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();

        Assert.assertEquals(1, count);
    }

    @Test
    public void testDelete() throws Exception {
        PreparedStatement ps = spliceClassWatcher.prepareStatement(
                String.format("insert into %s (i) values (?)", spliceTableWatcher1));
        ps.setInt(1, 100);
        ps.execute();

        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i=100", this.getTableReference(TABLE1_NAME)));

        String rowId = null;
        while (rs.next()) {
            rowId = rs.getString(1);
        }
        rs.close();

        // delete the row
        methodWatcher.executeUpdate(String.format("delete from %s where rowid =\'%s\'",
                this.getTableReference(TABLE1_NAME), rowId));

        // verify column value changed for the specified row
        rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i=100", this.getTableReference(TABLE1_NAME)));

        int count = 0;
        while (rs.next()) {
            count++;
        }
        rs.close();

        Assert.assertEquals(0, count);
    }



    @Test
    public void testPreparedStatement() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s where i=0", this.getTableReference(TABLE1_NAME)));

        RowId rId = null;
        while (rs.next()) {
            rId = rs.getRowId("rowid");
            String s = rs.getString(1);
            Assert.assertTrue(s.compareToIgnoreCase(rId.toString()) == 0);
        }

        com.splicemachine.db.client.am.RowId rowId = new com.splicemachine.db.client.am.RowId(rId.getBytes());

        PreparedStatement ps = spliceClassWatcher.prepareStatement(
                String.format("select i, rowid from %s where rowid = ?", spliceTableWatcher1));
        ps.setRowId(1, rowId);
        rs = ps.executeQuery();

        while (rs.next()) {
            rId = rs.getRowId(2);
            String s = rs.getString(2);
            Assert.assertTrue(s.compareToIgnoreCase(rId.toString()) == 0);
        }
    }

    @Test
    @Ignore("DB-3169")
    public void testCoveringIndex() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("select rowid, i, j from %s --SPLICE-PROPERTIES index=ti \n where i=1", this.getTableReference(TABLE3_NAME)));
        Assert.assertTrue(rs.next());
        RowId rowId1 = rs.getRowId(1);

        rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s --SPLICE-PROPERTIES index=ti \n where i=1", this.getTableReference(TABLE3_NAME)));
        Assert.assertTrue(rs.next());
        RowId rowId2 = rs.getRowId(1);

        Assert.assertEquals(rowId1.toString(), rowId2.toString());

        rs  = methodWatcher.executeQuery(
                String.format("select rowid, i from %s --SPLICE-PROPERTIES index=null \n where i=1", this.getTableReference(TABLE3_NAME)));
        Assert.assertTrue(rs.next());
        RowId rowId3 = rs.getRowId(1);

        Assert.assertEquals(rowId1.toString(), rowId3.toString());
    }

    @Test
    public void testSubquery() throws Exception {
        String sqlText = String.format("update %s set j=j+3 where rowid in (select rowid from %s)",
                spliceTableWatcher4, spliceTableWatcher5);
        int n = methodWatcher.executeUpdate(sqlText);
        Assert.assertEquals("Wrong number of rows updated", n, nRows);

        sqlText = String.format("delete from %s where rowid in (select rowid from %s)",
                spliceTableWatcher4, spliceTableWatcher5);
        n = methodWatcher.executeUpdate(sqlText);
        Assert.assertEquals("Wrong number of rows updated", n, nRows);
    }

    @Test
    public void testAggregateNotAllowed() throws Exception {
        String sqlText = String.format("select min(rowid) from %s", spliceTableWatcher4);
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.assertTrue("An exception is expected to be thrown", false);
        }
        catch (Exception e) {
            Assert.assertTrue(e.getLocalizedMessage().contains("A REF column cannot be aggregated"));
        }
    }
}
