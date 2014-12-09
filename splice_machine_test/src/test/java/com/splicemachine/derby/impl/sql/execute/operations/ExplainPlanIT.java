package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.RowId;

/**
 * Created by jyuan on 10/7/14.
 */
public class ExplainPlanIT extends SpliceUnitTest  {

    public static final String CLASS_NAME = ExplainPlanIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String TABLE_NAME = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static String tableDef = "(I INT)";
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testExplainSelect() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain select * from %s", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainUpdate() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain update %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainDelete() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("explain delete from %s where i = 1", this.getTableReference(TABLE_NAME)));

        int count = 0;
        while (rs.next()) {
            ++count;
        }
        Assert.assertTrue(count>0);
    }

    @Test
    public void testExplainTwice() throws Exception {
        ResultSet rs  = methodWatcher.executeQuery(
                String.format("-- some comments \n explain\nupdate %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));
        int count1 = 0;
        while (rs.next()) {
            ++count1;
        }
        rs.close();
        rs  = methodWatcher.executeQuery(
                String.format("-- some comments \n explain\nupdate %s set i = 0 where i = 1", this.getTableReference(TABLE_NAME)));
        int count2 = 0;
        while (rs.next()) {
            ++count2;
        }
        Assert.assertTrue(count1 == count2);
    }
}
