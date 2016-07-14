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

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.SQLException;

/**
 * @author alex_stybaev
 * Date 5/28/15.
 */
@Category({Transactions.class})
public class SetRoleTransactionIT {

    public static final SpliceSchemaWatcher schemaWatcher =
            new SpliceSchemaWatcher(SetRoleTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceWatcher classWatcher = new SpliceWatcher();

    public static final SpliceTableWatcher table =
            new SpliceTableWatcher("tableA",schemaWatcher.schemaName,"(a int, b int, primary key (a))");

    private static final String ROLE_NAME = "TS_ROLE";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn1;
    private static TestConnection conn2;

//    private long conn1Txn;
//    private long conn2Txn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.createConnection();
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
        conn2.rollback();
        conn1.close();
        conn2.close();

        conn1 = classWatcher.createConnection();
        conn2 = classWatcher.createConnection();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
    }


    @Test
    public void testCommitedTransactionRoleNotExist() throws Exception {
        conn1.createStatement().execute(String.format("insert into %s values (1,1)", table));
        //conn1.commit();
        try {
            conn1.createStatement().execute(String.format("set role %s", ROLE_NAME));
            Assert.fail("Exception expected");
        } catch (SQLException ex) {
            Assert.assertEquals("Wrong Exception!", SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION, ex.getSQLState());
            Assert.assertNotEquals(SQLState.ROLE_INVALID_SPECIFICATION, ex.getSQLState());
        }
    }

    @Test
    public void testNotCommitedTransactionRoleNotExist() throws Exception {
        try {
            conn1.createStatement().execute(String.format("set role %s", ROLE_NAME));
            Assert.fail("Exception expected");
        } catch (SQLException ex) {
            Assert.assertEquals(SQLState.ROLE_INVALID_SPECIFICATION, ex.getSQLState());
        }
    }

    @Test
    public void testNotCommitedTransactionRoleExist() throws Exception {
        conn1.createStatement().execute(String.format("CREATE ROLE %s", ROLE_NAME));
        conn1.commit();
        try {
            conn2.createStatement().execute(String.format("SET ROLE %s", ROLE_NAME));
        } catch (SQLException ex) {
            Assert.fail(String.format("Exception is thrown: %s", ex.getMessage()));
        } finally {
            conn1.createStatement().execute(String.format("DROP ROLE %s", ROLE_NAME));
            conn1.commit();
        }
    }

    @Test
    public void testCommitedTransactionRoleExist() throws Exception {
        conn1.createStatement().execute(String.format("CREATE ROLE %s", ROLE_NAME));
        conn1.commit();
        try {
            conn2.createStatement().execute(String.format("SET ROLE %s", ROLE_NAME));
        } catch (SQLException ex) {
            Assert.fail(String.format("Exception is thrown: %s", ex.getMessage()));
        } finally {
            conn1.createStatement().execute(String.format("DROP ROLE %s", ROLE_NAME));
            conn1.commit();
        }
    }


}
