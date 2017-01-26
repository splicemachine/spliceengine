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

package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

public class DatabaseMetaDataTestIT {
    private static final String CLASS_NAME = DatabaseMetaDataTestIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher("t1test", spliceSchemaWatcher.schemaName, "(t1n numeric(10,2) default null, t1c char(10))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1);


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testVersionAndProductName() throws Exception {
        DatabaseMetaData dmd = methodWatcher.getOrCreateConnection().getMetaData();
        Assert.assertEquals("Splice Machine", dmd.getDatabaseProductName());
        Assert.assertEquals("10.9.2.2 - (1)", dmd.getDatabaseProductVersion());
    }

    @Test
    public void testGetSchemasCorrect() throws Exception{
        String schemaName = "TEST_SCHEMA123456";
        TestConnection conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
        try{
            try(Statement s=conn.createStatement()){
                s.execute("create schema "+schemaName);
                conn.setSchema(schemaName);
                s.execute("create table t (a int, b int)");
            }

            DatabaseMetaData dmd=conn.getMetaData();
            try(ResultSet rs=dmd.getSchemas(null,schemaName)){
                Assert.assertTrue("Did not find sys schema!",rs.next());
                String tableSchem=rs.getString("TABLE_SCHEM");
                Assert.assertEquals("Incorrect table schema!",schemaName,tableSchem);
                Assert.assertNull("Incorrect catalog!",rs.getString("TABLE_CATALOG"));
                Assert.assertFalse("Found more than one schema for specified schema value!",rs.next());
            }
        }finally{
            conn.rollback();
        }
    }

    @Test
    public void testNullDecimalInUserTypeInDictionaryMeta() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("select * from sys.syscolumns where columnname = 'T1N'");
        Assert.assertTrue("Query Did not return, decimal serde issue",rs.next());
    }

}