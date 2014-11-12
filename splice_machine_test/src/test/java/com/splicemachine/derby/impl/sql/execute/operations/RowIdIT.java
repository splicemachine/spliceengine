package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.apache.derby.iapi.types.SQLRef;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.PreparedStatement;
import java.sql.RowId;
//import org.apache.derby.client.am.RowId;
/**
 * Created by jyuan on 9/28/14.
 */
public class RowIdIT extends SpliceUnitTest {
    public static final String CLASS_NAME = RowIdIT.class.getSimpleName().toUpperCase();
    public static int nRows = 3;
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE1_NAME = "A";
    public static final String TABLE2_NAME = "B";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE1_NAME,CLASS_NAME, tableDef);
    protected static SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher(TABLE2_NAME,CLASS_NAME, tableDef);

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

        org.apache.derby.client.am.RowId rowId = new org.apache.derby.client.am.RowId(rId.getBytes());

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
}
