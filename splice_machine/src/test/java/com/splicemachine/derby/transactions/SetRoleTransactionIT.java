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
