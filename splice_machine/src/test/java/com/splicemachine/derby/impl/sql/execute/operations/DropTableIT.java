package com.splicemachine.derby.impl.sql.execute.operations;


import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import com.splicemachine.homeless.TestUtils;
import java.sql.*;

/**
 * Created by tgildersleeve on 6/29/17.
 * SPLICE-1729
 *
 * Tests that the following queries execute successfully, both when a table exists and does not exist:
 *
 * drop table table_name
 * drop table if exists table_name
 * drop table table_name if exists
 *
 * _true tests test existing tables; _false tests test nonexistent tables
 */
public class DropTableIT {

    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testDropTable_true() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE DropTable_true(COL1 INT, COL2 VARCHAR(10))");
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE", "DROPTABLE_TRUE",null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTable_true has not been created", s.contains("DROPTABLE_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE DROPTABLE_TRUE"));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE", "DROPTABLE_TRUE",null);
        Assert.assertTrue("DropTable_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLE_TRUE"));
    }

    @Test
    public void testDropTable_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, null, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("Nonexistent_table should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE NONEXISTENT_TABLE"));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42Y55",e.getSQLState());
        }
    }

    @Test
    public void testDropTableIfExists_name_true() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE DropTableIfExists_name_true(COL1 INT, COL2 VARCHAR(10))");
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE","DROPTABLEIFEXISTS_NAME_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTableIfExists_name_true has not been created", s.contains("DROPTABLEIFEXISTS_NAME_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE IF EXISTS DROPTABLEIFEXISTS_NAME_TRUE"));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE","DROPTABLEIFEXISTS_NAME_TRUE", null);
        Assert.assertTrue("DropTableIfExists_name_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLEIFEXISTS_NAME_TRUE"));
    }

    @Test
    public void testDropTableIfExists_name_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, null, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE IF EXISTS NONEXISTENT_TABLE"));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown");
        }
    }

    @Test
    public void testDropTable_name_ifExists_true() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE DropTable_name_ifExists_true(COL1 INT, COL2 VARCHAR(10))");
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE","DROPTABLE_NAME_IFEXISTS_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTable_name_ifExists_true has not been created", s.contains("DROPTABLE_NAME_IFEXISTS_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE DROPTABLE_NAME_IFEXISTS_TRUE IF EXISTS"));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, "SPLICE","DROPTABLE_NAME_IFEXISTS_TRUE", null);
        Assert.assertTrue("DropTable_name_ifExists_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLE_NAME_IFEXISTS_TRUE"));
    }

    @Test
    public void testDropTable_name_ifExists_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, null, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE NONEXISTENT_TABLE IF EXISTS"));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown");
        }
    }

    @Test
    public void testDropTableIfExists_syntaxErr() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, null, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE EXISTS NONEXISTENT_TABLE"));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X01",e.getSQLState());
            Assert.assertEquals("Wrong Warning Message", "Syntax error: Encountered \"EXISTS\" at line 1, column 12.", e.getMessage());
        }
    }


}
