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

package com.splicemachine.derby.transactions;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;

import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * @author Scott Fines
 *         Date: 9/15/14
 */
@Category({Transactions.class})
public class CreateTableTransactionIT {
    private static final String SCHEMA_NAME=CreateTableTransactionIT.class.getSimpleName().toUpperCase();
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);


    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);

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
    public void testCreateTableNotRecognizedUntilCommit() throws Exception {
        conn1.createStatement().execute(String.format("create table %s.t (a int,b int)",schemaWatcher.schemaName));

        try{
            conn2.createStatement().executeQuery(String.format("select * from %s.t", schemaWatcher.schemaName));
            Assert.fail("Did not receive error!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message:"+se.getMessage(),
                    SQLState.LANG_TABLE_NOT_FOUND,se.getSQLState());
        }

        conn1.commit();
        conn2.commit();
        try{
            long count = conn2.count(String.format("select * from %s.t",schemaWatcher.schemaName));
            Assert.assertEquals("Incorrect count!",0,count);
        }finally{
            //drop the table that we just committed to keep things clear
            conn1.createStatement().execute(String.format("drop table %s.t",schemaWatcher.schemaName));
            conn1.commit();
        }
    }

    @Test
    public void testCreateTableRollback() throws Exception {
        conn1.createStatement().execute(String.format("create table %s.tr (a int,b int)",schemaWatcher.schemaName));

        try(ResultSet rs = conn1.createStatement().executeQuery(String.format("select * from %s.tr", schemaWatcher.schemaName))){
           Assert.assertFalse("No data should be present!",rs.next()); //just to make sure that selects work
        }

        conn1.rollback();

        try{
            conn1.createStatement().executeQuery(String.format("select * from %s.tr", schemaWatcher.schemaName));
            Assert.fail("Did not receive error!");
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message:"+se.getMessage(), SQLState.LANG_TABLE_NOT_FOUND,se.getSQLState());
        }
    }

    @Test
    public void testCreateTableWithSameNameCausesWriteConflict() throws Exception {
        conn1.createStatement().execute(String.format("create table %s.t (a int,b int)",schemaWatcher.schemaName));
        try{
            conn2.createStatement().execute(String.format("create table %s.t (a int,b int)",schemaWatcher.schemaName));
        }catch(SQLException se){
            Assert.assertEquals("Incorrect error message:"+se.getMessage(),
                    "SE014",se.getSQLState());
        }
    }

    @Test
    public void testCreateTableCommitAllowsOtherTransactionsToInsert() throws Exception{
        String table = schemaWatcher.schemaName+".t10";
        try(Statement s = conn1.createStatement()){
            s.executeUpdate("create table "+table+" (a int, b int)");
        }
        conn1.commit();
        conn2.commit(); //advance to ensure visibility

        try{
            try(Statement s=conn1.createStatement()){
                Assert.assertEquals("Data was mysteriously present!",0l,conn1.count(s,"select * from "+table));
            }

            try(Statement s=conn2.createStatement()){
                Assert.assertEquals("Data was mysteriously present!",0l,conn2.count(s,"select * from "+table));
                s.execute("insert into "+table+" (a, b) values (1,1)");
                Assert.assertEquals("Data was not inserted properly!",1l,conn2.count(s,"select * from "+table));
            }
        }finally{
            try(Statement s = conn1.createStatement()){
                s.executeUpdate("drop table "+table);
            }
        }
    }
}
