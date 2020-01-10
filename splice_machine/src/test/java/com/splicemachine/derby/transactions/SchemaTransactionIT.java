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

package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;

import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Scott Fines
 * Date: 9/25/14
 */
@Category({Transactions.class})
public class SchemaTransactionIT {
    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher);

    private static TestConnection conn1;
    private static TestConnection conn2;


    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception {
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
    }

    @Test
    public void testCreateSchemaNotRecognizedUntilCommit() throws Exception {
        String schemaName = createSchema(1);
        System.out.println("Schema created");

        ResultSet schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("schema is visible outside of transaction!",schemas.next());
        System.out.println("Schema checked 1");

        conn1.commit();
        conn2.commit();
        System.out.println("Committed");
        try{
            //the data should be clear
            schemas = conn2.getMetaData().getSchemas(null, schemaName);
            Assert.assertTrue("schema is visible outside of transaction!", schemas.next());
        }finally{
            /*
             * Try and keep the schemaspace clear. Of course, if DropSchema doesn't work
             * properly, then this won't work and you may get contamination between different
             * test runs.
             */
            conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
            conn1.commit();
        }
    }

    @Test
    public void testCreateSchemaGetSchemaCommit() throws Exception {
        String schemaName=createSchema(11);

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible within the same transaction!", schemas.next());

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after commit!",schemas.next());

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after commit!",schemas.next());
        //commit the txn
        conn1.commit();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible within the same transaction!", schemas.next());

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after commit!",schemas.next());
    }


    @Test
    public void testCreateSchemaRollback() throws Exception {
        String schemaName = createSchema(16);

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible within the same transaction!", schemas.next());
        conn1.rollback();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("schema is visible after rollback!",schemas.next());
    }

    @Test
    public void testCreateSchemaRollbackAfterCreateTable() throws Exception {
        String schemaName = createSchema(10);

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible within the same transaction!", schemas.next());

        //do something so that schemas has the option of ending up in the dictionary cache
        try(Statement s = conn1.createStatement()){
            s.execute("create table "+schemaName+".t (a int)");
        }
        //now  rollback and make sure the schema doesn't bleed over
        conn1.rollback();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("schema is visible after rollback!",schemas.next());
    }

    @Test
    public void testCreateSchemaWithSameNameCausesWriteConflict() throws Exception {
        String schemaName = createSchema(3);

        try{
            conn2.createStatement().execute(String.format("create schema %s",schemaName));
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message:"+se.getMessage(), "SE014",se.getSQLState()); //WWConflict
        }
    }

    @Test
    public void testDropSchemaNotRecognizedUntilCommit() throws Exception {
        String schemaName = createSchema(4);
        conn1.commit();
        conn2.commit();

        ResultSet schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after commit!", schemas.next());
        schemas.close();

        //now drop the schema
        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));

        schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("drop schema is visible outside of transaction!",schemas.next());
        schemas.close();

        conn1.commit();
        conn2.commit();

        schemas = conn2.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("drop schema is still visible after commit!", schemas.next());
        schemas.close();
    }

    @Test
    public void testDropSchemaRollback() throws Exception {
        String schemaName = createSchema(5);
        conn1.commit();

        ResultSet schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible before drop!", schemas.next());

        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("drop schema is not visible inside same transaction!",schemas.next());

        conn1.rollback();

        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertTrue("schema is not visible after rollback!",schemas.next());
        //actually drop it to make sure it clears
        conn1.createStatement().execute(String.format("drop schema %s restrict",schemaName));
        conn1.commit();
        schemas = conn1.getMetaData().getSchemas(null, schemaName);
        Assert.assertFalse("drop schema is not visible inside same transaction!",schemas.next());
    }

    @Test
    public void testDropSchemaCreateSchemaSameTransaction() throws Exception{
        //create a schema
        String schemaName = createSchema(conn1,7);

        //make sure it shows up
        try(ResultSet rs = conn1.getMetaData().getSchemas(null,schemaName)){
            Assert.assertTrue("Schema is missing!",rs.next());
        }

        try(ResultSet rs = conn1.getMetaData().getSchemas(null,schemaName)){
            Assert.assertTrue("Schema is missing!",rs.next());
        }

        //drop the schema
        try(Statement s = conn1.createStatement()){
            s.execute("drop schema "+ schemaName+" restrict");
        }

        //make sure it's absent
        try(ResultSet rs = conn1.getMetaData().getSchemas(null,schemaName)){
            Assert.assertFalse("Schema is still present after drop!",rs.next());
        }

        //create it again
        try(Statement s = conn1.createStatement()){
            s.execute("create schema "+ schemaName);
        }

        //make sure it shows up
        try(ResultSet rs = conn1.getMetaData().getSchemas(null,schemaName)){
            Assert.assertTrue("Schema is missing!",rs.next());
        }
    }

    private String createSchema(int initialSchemaId) throws SQLException{
        return createSchema(conn1,initialSchemaId);
    }
    private String createSchema(Connection conn,int initialSchemaId) throws SQLException{
        String schemaName;
        boolean found;
        do{
            schemaName=("schema"+initialSchemaId).toUpperCase();
            try(Statement s = conn.createStatement()){
                s.execute(String.format("create schema %s",schemaName));
                found = true;
            }catch(SQLException se){
                if(!"X0Y68".equals(se.getSQLState()))
                    throw se;
                else initialSchemaId+=4;
                found = false;
            }
        }while(!found);
        return schemaName;
    }
}
