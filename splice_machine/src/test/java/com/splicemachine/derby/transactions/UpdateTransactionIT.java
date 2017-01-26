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

package com.splicemachine.derby.transactions;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * ITs relating to transactional behavior of UPDATE calls.
 *
 * @author Scott Fines
 *         Date: 11/10/14
 */
@Category({Transactions.class})
public class UpdateTransactionIT {
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(UpdateTransactionIT.class.getSimpleName());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int)");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        Statement statement = classWatcher.getStatement();
                        statement.execute("insert into "+table+" (a,b) values (1,1)");
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }
                }
            });

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
        conn1Txn = conn1.getCurrentTransactionId();
        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test
    public void testUpdateAndSelectPreparedStatementsWorkWithCommit() throws Exception {
        /*Regression test for DB-2199*/
        PreparedStatement select = conn1.prepareStatement("select a,b from "+table+" where b = ?");
        conn1.commit();
        PreparedStatement update = conn1.prepareStatement("update "+ table+" set a = a+1 where b = ?");
        conn1.commit();

        //now make sure that the order is correct
        select.setInt(1,1);
        ResultSet selectRs = select.executeQuery();
        Assert.assertTrue("Did not find any rows!",selectRs.next());
        Assert.assertEquals("Incorrect value for a!",1,selectRs.getInt(1));

        //issue the update
        update.setInt(1,1);
        update.execute();

        //make sure the value changed within this transaction
        select.setInt(1,1);
        selectRs = select.executeQuery();
        Assert.assertTrue("Did not find any rows!",selectRs.next());
        Assert.assertEquals("Incorrect value for a!",2,selectRs.getInt(1));

        //now commit, and make sure that the change has been propagated
        conn1.commit();

        select.setInt(1,1);
        selectRs = select.executeQuery();
        Assert.assertTrue("Did not find any rows!",selectRs.next());
        Assert.assertEquals("Incorrect value for a!",2,selectRs.getInt(1));
    }
}
