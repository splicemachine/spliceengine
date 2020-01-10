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
        Assert.assertTrue("New Transaction cannot see created schema",resultSet.next());
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
        Assert.assertTrue("New Transaction cannot see created schema",resultSet.next());
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
        Assert.assertFalse("New Transaction cannot see rollbacked schema",resultSet.next());
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
