/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

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

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = DropTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testDropTable_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE TABLE %s.DropTable_true(COL1 INT, COL2 VARCHAR(10))", schema.schemaName));

        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "DROPTABLE_TRUE",null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTable_true has not been created", s.contains("DROPTABLE_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE %s.DROPTABLE_TRUE", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "DROPTABLE_TRUE",null);
        Assert.assertTrue("DropTable_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLE_TRUE"));
    }

    @Test
    public void testDropTable_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("Nonexistent_table should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE %s.NONEXISTENT_TABLE", schema.schemaName));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42Y55",e.getSQLState());
        }
    }

    @Test
    public void testDropTableIfExists_name_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE TABLE %s.DropTableIfExists_name_true(COL1 INT, COL2 VARCHAR(10))",schema.schemaName));
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPTABLEIFEXISTS_NAME_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTableIfExists_name_true has not been created", s.contains("DROPTABLEIFEXISTS_NAME_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE IF EXISTS %s.DROPTABLEIFEXISTS_NAME_TRUE", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPTABLEIFEXISTS_NAME_TRUE", null);
        Assert.assertTrue("DropTableIfExists_name_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLEIFEXISTS_NAME_TRUE"));
    }

    @Test
    public void testDropTableIfExists_name_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE IF EXISTS %s.NONEXISTENT_TABLE", schema.schemaName));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown");
        }
    }

    @Test
    public void testDropTable_name_ifExists_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE TABLE %s.DropTable_name_ifExists_true(COL1 INT, COL2 VARCHAR(10))",schema.schemaName));
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPTABLE_NAME_IFEXISTS_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropTable_name_ifExists_true has not been created", s.contains("DROPTABLE_NAME_IFEXISTS_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP TABLE %s.DROPTABLE_NAME_IFEXISTS_TRUE IF EXISTS", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPTABLE_NAME_IFEXISTS_TRUE", null);
        Assert.assertTrue("DropTable_name_ifExists_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPTABLE_NAME_IFEXISTS_TRUE"));
    }

    @Test
    public void testDropTable_name_ifExists_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE %s.NONEXISTENT_TABLE IF EXISTS", schema.schemaName));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown");
        }
    }

    @Test
    public void testDropTableIfExists_syntaxErr() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_TABLE",null);
            Assert.assertTrue("NONEXISTENT_TABLE should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_TABLE"));

            methodWatcher.executeUpdate(String.format("DROP TABLE EXISTS %s.NONEXISTENT_TABLE", schema.schemaName));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X01",e.getSQLState());
            Assert.assertEquals("Wrong Warning Message", "Syntax error: Encountered \"EXISTS\" at line 1, column 12.", e.getMessage());
        }
    }


}
