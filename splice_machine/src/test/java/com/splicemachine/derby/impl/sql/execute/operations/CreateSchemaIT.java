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

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static org.junit.Assert.assertEquals;

/**
 * Tests around creating schemas
 */
public class CreateSchemaIT {
    protected static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(CreateSchemaIT.class);

    protected static final SpliceSchemaWatcher sullivan1SchemaWatcher = new SpliceSchemaWatcher("SULLIVAN1");
    protected static final SpliceSchemaWatcher sullivanSchemaWatcher = new SpliceSchemaWatcher("SULLIVAN");
    protected static final SpliceSchemaWatcher cmprod = new SpliceSchemaWatcher("cmprod");
    protected static final SpliceUserWatcher cmprodUser = new SpliceUserWatcher("cmprod","bigdata4u");


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
        try( ResultSet rs = ps.executeQuery() ) {
            int count = 0;
            while (rs.next()) {
                count++;
            }
            Assert.assertEquals("Incorrect row count", 2, count);
        }
    }

    @Test
    //SPLICE-1739
    public void testCreateSchemaIfNotExists() throws Exception {
        methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS TESTFOO");
        methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS TESTFOO1");
        methodWatcher.executeUpdate("CREATE SCHEMA TESTFOO2 IF NOT EXISTS");
        ResultSet rs = methodWatcher.executeQuery("SELECT SCHEMANAME FROM SYS.SYSSCHEMAS WHERE SCHEMANAME in ('TESTFOO', 'TESTFOO1', 'TESTFOO2')");
        String expected = "SCHEMANAME |\n" +
                "------------\n" +
                "  TESTFOO  |\n" +
                " TESTFOO1  |\n" +
                " TESTFOO2  |";
        assertEquals("list of schemas does not match", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    //SPLICE-1739
    public void testCreateSchemaIfNotExistsWhenExists() throws Exception {
        methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS EXISTINGSCHEMA");
        try {
            methodWatcher.executeUpdate("CREATE SCHEMA IF NOT EXISTS EXISTINGSCHEMA");
        }
        catch(SQLException e){
            Assert.fail("Shouldn't have thrown an error. CREATE SCHEMA IF NOT EXISTS <schemaname> SHOULD BE HANDLED");
        }
        try{
            methodWatcher.executeUpdate("CREATE SCHEMA EXISTINGSCHEMA IF NOT EXISTS");
        }
        catch (SQLException e){
            Assert.fail("Shouldn't have thrown an error. CREATE SCHEMA <schemaname> IF NOT EXISTS SHOULD BE HANDLED");
        }
        ResultSet rs = methodWatcher.executeQuery("SELECT SCHEMANAME FROM SYS.SYSSCHEMAS WHERE SCHEMANAME LIKE 'EXISTINGSCHEMA%'");
        rs.next();
        Assert.assertTrue("Schema should now exist", rs.getString(1).equals("EXISTINGSCHEMA"));
    }

    @Test
    // DB-5988
    public void testSchemaAuthorizationCreation() throws Exception {
        Connection connection = null;
        ResultSet rs = null;
        try {
            methodWatcher.execute("call SYSCS_UTIL.SYSCS_UPDATE_SCHEMA_OWNER('cmprod','cmprod')");
            methodWatcher.execute("create table cmprod.table1 (col1 int)");
            connection = SpliceNetConnection.getConnectionAs("cmprod", "bigdata4u");
            try (Statement s = connection.createStatement()) {
                rs = s.executeQuery("select * from cmprod.table1");
                rs.next();
                rs.close();
                rs = null;
            }
        } finally {
            if( rs != null )
                rs.close();
            if (connection != null)
                connection.close();
        }
    }

    // DB-8357 : Create a table on a not existing schema should not implicitly create the schema.
    @Test
    public void testNoImplicitSchemaCreate() throws Exception {
        try {
            methodWatcher.execute("create table NOT_EXISTING_SCHEMA.AAA (col1 int)");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception", "42Y07", e.getSQLState());
        }
    }
}
