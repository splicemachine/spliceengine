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

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Tests around creating schemas
 */
public class CreateSchemaIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(CreateSchemaIT.class);

    protected static SpliceSchemaWatcher sullivan1SchemaWatcher = new SpliceSchemaWatcher("SULLIVAN1");
    protected static SpliceSchemaWatcher sullivanSchemaWatcher = new SpliceSchemaWatcher("SULLIVAN");
    protected static SpliceSchemaWatcher cmprod = new SpliceSchemaWatcher("cmprod");
    protected static SpliceUserWatcher cmprodUser = new SpliceUserWatcher("cmprod","bigdata4u");


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(sullivan1SchemaWatcher)
            .around(sullivanSchemaWatcher)
            .around(cmprod)
            .around(cmprodUser);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testCreateSchemasWitSimilarName() throws Exception {
        PreparedStatement ps = methodWatcher.prepareStatement("select * from sys.sysschemas where schemaName like 'SULLIVAN%'");
        ResultSet rs = ps.executeQuery();
        int count = 0;
        while (rs.next()) {
            count++;
        }
        Assert.assertEquals("Incorrect row count", 2, count);
    }

    @Test
    //SPLICE-1739
    public void testCreateSchemaIfNotExists() throws Exception {
        methodWatcher.executeUpdate("CREATE SCHEMA TESTFOO");
        methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS TESTFOO1");
        methodWatcher.executeUpdate("CREATE SCHEMA TESTFOO2 IF NOT EXISTS");
        ResultSet rs = methodWatcher.executeQuery("SELECT SCHEMANAME FROM SYS.SYSSCHEMAS WHERE SCHEMANAME LIKE 'TEST%'");
        rs.next();
        Assert.assertTrue("Schema should now exist", rs.getString(1).equals("TESTFOO"));
        rs.next();
        Assert.assertTrue("Schema should now exist", rs.getString(1).equals("TESTFOO1"));
        rs.next();
        Assert.assertTrue("Schema should now exist", rs.getString(1).equals("TESTFOO2"));
    }

    @Test
    //SPLICE-1739
    public void testCreateSchemaIfNotExistsWhenExists() throws Exception {
        methodWatcher.executeUpdate("CREATE SCHEMA TESTFOO");
        try {
            methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS TESTFOO");
        }
        catch(SQLException e){
            Assert.fail("Shouldn't have thrown an error. CREATE SCHEMA IF NOT EXISTS <schemaname> SHOULD BE HANDLED");
        }
        try{
            methodWatcher.executeUpdate("CREATE SCHEMA TESTFOO IF NOT EXISTS");
        }
        catch (SQLException e){
            Assert.fail("Shouldn't have thrown an error. CREATE SCHEMA <schemaname> IF NOT EXISTS SHOULD BE HANDLED");
        }
        ResultSet rs = methodWatcher.executeQuery("SELECT SCHEMANAME FROM SYS.SYSSCHEMAS WHERE SCHEMANAME LIKE 'TEST%'");
        rs.next();
        Assert.assertTrue("Schema should now exist", rs.getString(1).equals("TESTFOO"));
    }

    @Test
    // DB-5988
    public void testSchemaAuthorizationCreation() throws Exception {
        Connection connection = null;
        try {
            methodWatcher.execute("call SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER('cmprod','cmprod')");
            methodWatcher.execute("create table cmprod.table1 (col1 int)");
            connection = SpliceNetConnection.getConnectionAs("cmprod", "bigdata4u");
            ResultSet rs = connection.createStatement().executeQuery("select * from cmprod.table1");
            rs.next();
            rs.close();
        } finally {
            if (connection != null)
                connection.close();
        }
    }

}
