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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
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

    public static final String SCHEMA = TransactionAdminIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    
    @ClassRule
    public static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);


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
    public void testKilledTransactionRaisesException() throws Exception {
        conn1.createStatement().execute("create table a (i int)");
        long id = conn1.getCurrentTransactionId();
        conn1.createStatement().execute("call SYSCS_UTIL.SYSCS_KILL_TRANSACTION(" + id + ")");
        try {
            conn1.commit();
            Assert.fail("Should have raised exception");
        }catch (SQLException se) {
            Assert.assertEquals("SE015", se.getSQLState());
        }
        // connection shouldn't have closed
        conn1.createStatement().execute("create table a (i int)");
        conn1.rollback();
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

//        Assert.assertNotEquals("Transaction id did not advance!",txnId,txnId2);
//
//        try(Statement s = conn1.createStatement()){
//            s.execute("insert into "+table+" (a,b) values (1,1)");
//        }
    }
}
