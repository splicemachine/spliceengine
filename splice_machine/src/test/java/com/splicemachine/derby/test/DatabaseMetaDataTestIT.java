/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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