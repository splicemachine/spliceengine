package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;

/**
 * Test we get unsupported operation exception when trying to set
 * a Savepoint.
 *
 * @author Jeff Cunningham
 *         Date: 7/17/13
 */
public class SavepointConstantOperationIT { 
    public static final String CLASS_NAME = SavepointConstantOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static final String TABLE_NAME_1 = "B";

    private static String tableDef = "(TaskId INT NOT NULL)";
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, tableDef);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testSavepointNotSupported() throws Exception {
        Connection conn = methodWatcher.createConnection();
        conn.setAutoCommit(false);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate(String.format("INSERT INTO %s.%s (TaskId) VALUES(%d)", CLASS_NAME, TABLE_NAME_1, 1));
        // set savepoint
        Savepoint svpt1 = null;
        try {
            svpt1 = conn.setSavepoint("S1");
            Assert.fail("Expected unsupported operation exception");
        } catch (SQLException e) {
           // expected
            Assert.assertEquals("Expected unsupported operation exception", "Splice Engine operation unsupported exception: SavepointConstantOperation is unsupported.", e.getMessage());
        }
        Assert.assertNull(svpt1);
    }
}
