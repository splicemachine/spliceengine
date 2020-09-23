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
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

/**
 * Created by tgildersleeve on 6/29/17.
 * SPLICE-398
 *
 * Tests that the following view drops execute successfully, both when a view exists and does not exist:
 *
 * drop view view_name
 * drop view if exists view_name
 * drop view view_name if exists
 *
 * _true tests test existing views; _false tests test nonexistent views
 */
public class DropViewIT {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = DropViewIT.class.getSimpleName().toUpperCase();
    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    protected static SpliceTableWatcher TABLE_1 = new SpliceTableWatcher("TABLE_1", schema.schemaName, "(COL1 int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema)
            .around(TABLE_1);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testDropView_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE VIEW %s.DROPVIEW_TRUE AS SELECT COL1 FROM %s.TABLE_1", schema.schemaName, schema.schemaName));

        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, null, "DROPVIEW_TRUE",null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropView_true has not been created", s.contains("DROPVIEW_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP VIEW %s.DROPVIEW_TRUE", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "DROPVIEW_TRUE",null);
        Assert.assertTrue("DropView_true was not dropped", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPVIEW_TRUE"));
    }

    @Test
    public void testDropView_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_VIEW",null);
            Assert.assertTrue("Nonexistent_view should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_VIEW"));

            methodWatcher.executeUpdate(String.format("DROP VIEW %s.NONEXISTENT_VIEW", schema.schemaName));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","X0X05",e.getSQLState());
        }
    }

    @Test
    public void testDropViewIfExists_name_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE VIEW %s.DROPVIEWIFEXISTS_NAME_TRUE AS SELECT COL1 FROM %s.TABLE_1",schema.schemaName,schema.schemaName));
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPVIEWIFEXISTS_NAME_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropViewIfExists_name_true has not been created", s.contains("DROPVIEWIFEXISTS_NAME_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP VIEW IF EXISTS %s.DROPVIEWIFEXISTS_NAME_TRUE", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPVIEWIFEXISTS_NAME_TRUE", null);
        Assert.assertTrue("DropViewIfExists_name_true was not dropped", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPVIEWIFEXISTS_NAME_TRUE"));
    }

    @Test
    public void testDropViewIfExists_name_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_VIEW",null);
            Assert.assertTrue("NONEXISTENT_VIEW should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_VIEW"));

            methodWatcher.executeUpdate(String.format("DROP VIEW IF EXISTS %s.NONEXISTENT_VIEW", schema.schemaName));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown. Only a warning should be thrown.");
        }
    }

    @Test
    public void testDropView_name_ifExists_true() throws Exception {
        methodWatcher.executeUpdate(String.format("CREATE VIEW %s.DROPVIEW_NAME_IFEXISTS_TRUE AS SELECT COL1 FROM %s.TABLE_1 ",schema.schemaName, schema.schemaName));
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPVIEW_NAME_IFEXISTS_TRUE", null);
        String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("DropView_name_ifExists_true has not been created", s.contains("DROPVIEW_NAME_IFEXISTS_TRUE"));

        methodWatcher.executeUpdate(String.format("DROP VIEW %s.DROPVIEW_NAME_IFEXISTS_TRUE IF EXISTS", schema.schemaName));
        ResultSet rs2 = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName,"DROPVIEW_NAME_IFEXISTS_TRUE", null);
        Assert.assertTrue("DropView_name_ifExists_true was not deleted", !TestUtils.FormattedResult.ResultFactory.toString(rs2).contains("DROPVIEW_NAME_IFEXISTS_TRUE"));
    }

    @Test
    public void testDropView_name_ifExists_false() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_VIEW",null);
            Assert.assertTrue("NONEXISTENT_VIEW should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_VIEW"));

            methodWatcher.executeUpdate(String.format("DROP VIEW %s.NONEXISTENT_VIEW IF EXISTS", schema.schemaName));
        }
        catch (SQLException e) {
            Assert.fail("No exception should be thrown. Only a warning should be thrown.");
        }
    }

    @Test
    public void testDropViewIfExists_syntaxErr() throws Exception {
        try {
            ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, schema.schemaName, "NONEXISTENT_VIEW",null);
            Assert.assertTrue("NONEXISTENT_VIEW should not exist yet", !TestUtils.FormattedResult.ResultFactory.toString(rs).contains("NONEXISTENT_VIEW"));

            methodWatcher.executeUpdate(String.format("DROP VIEW EXISTS %s.NONEXISTENT_VIEW", schema.schemaName));
            Assert.fail("Exception not thrown");
        }
        catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42X01",e.getSQLState());
            Assert.assertEquals("Wrong Warning Message", "Syntax error: Encountered \"EXISTS\" at line 1, column 11.", e.getMessage());
        }
    }

}
