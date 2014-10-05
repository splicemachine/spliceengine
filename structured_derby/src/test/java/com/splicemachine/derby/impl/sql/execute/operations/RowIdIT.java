package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.apache.derby.iapi.types.SQLRef;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
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
                        for (int i = 0; i < 3; i++) {
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
                        for (int i = 0; i < 3; i++) {
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

}
