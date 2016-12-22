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

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;
import java.sql.Statement;

/**
 * Tests around the maintenance of TransactionAdmin tools.
 *
 * @author Scott Fines
 * Date: 9/4/14
 */
@Category({Transactions.class})
public class TransactionAdminIT {

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

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
//        conn1Txn = conn1.getCurrentTransactionId();
//        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test
    @Ignore("Doesn't test killing properly")
    public void testKillTransactionKillsTransaction() throws Exception {
        conn1.createStatement().execute("call SYSCS_UTIL.SYSCS_KILL_TRANSACTION("+conn2Txn+")");
        //vacuum must wait for conn2 to complete, unless it's been killed
        conn1.createStatement().execute("call SYSCS_UTIL.VACUUM()");
    }

    @Test
    public void testGetCurrentTransactionWorks() throws Exception{
        /*
         * Sequence:
         * 1. create table
         * 2. commit
         * 3. drop table
         * 4. call SYSCS_UTIL.GET_CURRENT_TRANSACTION();
         * 5. rollback;
         * 6. call SYSCS_UTIL.GET_CURRENT_TRANSACTION();
         *
         * and make sure that there is no error.
         */
        System.out.println("Creating table");
        String table; //= createTable(conn1,9);
        try(Statement s = conn1.createStatement()){
            try{
                s.execute("create table t9 (a int, b int)");
            }catch(SQLException se){
                if(!se.getSQLState().equals("X0Y32"))
                    throw se; //ignore LANG_OBJECT_ALREADY_EXISTS errors
            }
            table = "t9";
        }
        System.out.println("Committing");
        conn1.commit();

        try(Statement s =conn1.createStatement()){
            System.out.println("inserting data");
            s.execute("insert into "+ table+" (a,b) values (1,1)");
        }

        conn1.commit();
        System.out.println("getCurrId");
        long txnId2 = conn1.getCurrentTransactionId();

//        Assert.assertNotEquals("Txn id did not advance!",txnId,txnId2);
//
//        try(Statement s = conn1.createStatement()){
//            s.execute("insert into "+table+" (a,b) values (1,1)");
//        }
    }
}
