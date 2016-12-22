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

package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.splicemachine.test_dao.SchemaDAO;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Schema creation / drop tests.
 */
//@Category(SerialTest.class)
public class SchemaConstantIT extends SpliceUnitTest { 

    private static final String CLASS_NAME = SchemaConstantIT.class.getSimpleName().toUpperCase();
    private static final String SCHEMA1_NAME = CLASS_NAME + "_1";
    private static final String SCHEMA2_NAME = CLASS_NAME + "_2";
    private static final String SCHEMA3_NAME = CLASS_NAME + "_3";
    private static final String SCHEMA4_NAME = CLASS_NAME + "_4";

    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    @Override
    public String getSchemaName() {
        return CLASS_NAME;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher);

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private SchemaDAO schemaDAO;

    @Before
    public void initSchemaDAO() throws Exception {
        schemaDAO = new SchemaDAO(methodWatcher.getOrCreateConnection());
    }

    @Test
    public void testSchemaCreation() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        schemaDAO.drop(SCHEMA1_NAME);
        connection1.createStatement().execute(String.format("create schema %s",SCHEMA1_NAME));
        ResultSet resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
        connection1.commit();
        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
        Assert.assertTrue("New Txn cannot see created schema",resultSet.next());
    }

    @Test(expected=SQLException.class)
    public void testSchemaCreationTwice() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        connection1.setAutoCommit(false);
        schemaDAO.drop(SCHEMA1_NAME);
        connection1.createStatement().execute(String.format("create schema %s",SCHEMA1_NAME));
        ResultSet resultSet = connection1.getMetaData().getSchemas(null, SCHEMA1_NAME);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
        connection1.createStatement().execute(String.format("create schema %s",SCHEMA1_NAME));
        connection1.commit();
    }

    @Test
    public void testSchemaCreationIsolation() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        Connection connection2 = methodWatcher.createConnection();
        schemaDAO.drop(SCHEMA2_NAME);
        connection1.setAutoCommit(false);
        connection2.setAutoCommit(false);
        connection1.createStatement().execute(String.format("create schema %s",SCHEMA2_NAME));
        ResultSet resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
        Assert.assertFalse("Read Committed Violated",resultSet.next());
        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA2_NAME);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
        connection1.commit();
        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
        Assert.assertFalse("Read Timestamp Violated",resultSet.next());
        connection2.commit();
        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA2_NAME);
        Assert.assertTrue("New Txn cannot see created schema",resultSet.next());
    }

    @Test
    public void testSchemaRollbackIsolation() throws Exception{
        Connection connection1 = methodWatcher.createConnection();
        Connection connection2 = methodWatcher.createConnection();
        schemaDAO.drop(SCHEMA3_NAME);
        connection1.setAutoCommit(false);
        connection2.setAutoCommit(false);
        connection1.createStatement().execute(String.format("create schema %s",SCHEMA3_NAME));
        ResultSet resultSet = connection2.getMetaData().getSchemas(null, SCHEMA3_NAME);
        Assert.assertFalse("Read Committed Violated",resultSet.next());
        resultSet = connection1.getMetaData().getSchemas(null, SCHEMA3_NAME);
        Assert.assertTrue("Connection should see its own writes",resultSet.next());
        connection1.rollback();
        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA3_NAME);
        Assert.assertFalse("Read Timestamp Violated",resultSet.next());
        connection2.commit();
        resultSet = connection2.getMetaData().getSchemas(null, SCHEMA3_NAME);
        Assert.assertFalse("New Txn cannot see rollbacked schema",resultSet.next());
    }

    @Test
    public void testSchemaCreationShowsInMetadata() throws Exception{
        Connection conn = methodWatcher.getOrCreateConnection();
        String schemaName = SCHEMA4_NAME;
        boolean drop = false;
        try(ResultSet schemas=conn.getMetaData().getSchemas()){
            while(schemas.next()){
                if(schemaName.equalsIgnoreCase(schemas.getString(1))){
                    //schema exists, so try and drop it
                    drop=true;
                    break;
                }
            }
        }
        if(drop){
            //try dropping the schema then try finding it again to make sure it's dropped
            try(Statement s = conn.createStatement()){
                s.executeUpdate("drop schema "+schemaName+" restrict");
            }
            //validate that it isn't in the metadata any longer
            try(ResultSet schemas=conn.getMetaData().getSchemas()){
                while(schemas.next()){
                    Assert.assertNotEquals("Schema still found after dropping!",schemaName.toUpperCase(),schemas.getString(1));
                }
            }
        }

        //create the schema and make sure it's present
        try(Statement s = conn.createStatement()){
            s.executeUpdate("create schema "+schemaName);
        }

        boolean found = false;
        try(ResultSet schemas=conn.getMetaData().getSchemas()){
            while(schemas.next()){
                if(schemaName.equalsIgnoreCase(schemas.getString(1))){
                    //schema exists!
                    found=true;
                    break;
                }
            }
        }
        Assert.assertTrue("Did not find schema after creation!",found);

        //now drop it and make sure it's not there any more
        try(Statement s = conn.createStatement()){
            s.executeUpdate("drop schema "+schemaName+" restrict");
        }
        //validate that it isn't in the metadata any longer
        try(ResultSet schemas=conn.getMetaData().getSchemas()){
            while(schemas.next()){
                Assert.assertNotEquals("Schema still found after dropping!",schemaName.toUpperCase(),schemas.getString(1));
            }
        }
    }

    @Test
    public void doubleTestSchemaCreationShowsInMetadata() throws Exception{
        testSchemaCreationShowsInMetadata();
        testSchemaCreationShowsInMetadata();
    }
}
