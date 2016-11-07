package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 * IT's for external table functionality
 *
 */
public class PinTableIT extends SpliceUnitTest{

    private static final String SCHEMA_NAME = PinTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("t1",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("t2",SCHEMA_NAME,"(col1 int)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
    .around(spliceTableWatcher2);
    @Test
    public void testPinTableDoesNotExist() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("create pin table foo");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","X0X05",e.getSQLState());
        }
    }

    @Test
    public void testPinTable() throws Exception {
            methodWatcher.executeUpdate("insert into t1 values (1)");
            methodWatcher.executeUpdate("create pin table t1");
            ResultSet rs = methodWatcher.executeQuery("select * from t1 --splice-properties pin=true");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void selectFromPinThatDoesNotExist() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into t2 values (1)");
            ResultSet rs = methodWatcher.executeQuery("select * from t2 --splice-properties pin=true");
            Assert.assertEquals("foo", TestUtils.FormattedResult.ResultFactory.toString(rs));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT16",e.getSQLState());
        }
    }

}