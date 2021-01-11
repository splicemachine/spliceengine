/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.api.ConflictResolutionStrategy;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.HBaseTest;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;

import static com.splicemachine.test_tools.Rows.row;

// this test suite relies on calling SYSCS_GET_REGION_SERVER_CONFIG_INFO to check whether the cluster is running
// with IMMEDIATE or DEFERRED conflict resolution. This procedure relies on some methods that are not implemented
// yet in Mem profile such as com.splicemachine.access.util.ReflectingConfigurationSource.prefixMatch
// therefor, we run this suite only in HBase profile.
@Category({HBaseTest.class, SerialTest.class})
public class ConflictResolutionStrategyIT extends SpliceUnitTest {

    public static final String CLASS_NAME = ConflictResolutionStrategyIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    private static ConflictResolutionStrategy conflictResolutionStrategy = ConflictResolutionStrategy.IMMEDIATE;

    @BeforeClass
    public static void getConflictResolutionStrategy() throws Exception {
        try(Connection conn = spliceClassWatcher.createConnection()) {
            try(Statement statement = conn.createStatement()) {
                try(ResultSet rs = statement.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_REGION_SERVER_CONFIG_INFO(null, 0)")) {
                    while(rs.next()) { // there is a bug in SYSCS_GET_REGION_SERVER_CONFIG_INFO that prevents filtering with first input parameter.
                        if(rs.getString(2).equals("conflictResolutionStrategy")) {
                            if(rs.getString(3).equals(ConflictResolutionStrategy.DEFERRED.toString())) {
                                conflictResolutionStrategy = ConflictResolutionStrategy.DEFERRED;
                            } else {
                                conflictResolutionStrategy = ConflictResolutionStrategy.IMMEDIATE;
                            }
                        }
                    }
                }
            }
        }
    }

    public boolean conflictResolutionStrategyIsDeferred() throws Exception {
        return conflictResolutionStrategy == ConflictResolutionStrategy.DEFERRED;
    }

    public boolean conflictResolutionStrategyIsImmediate() throws Exception {
        return !conflictResolutionStrategyIsDeferred();
    }

    static class Harness {

        private final Map<String, Transaction> txns;

        Harness() {
            txns = new HashMap<>();
        }

        private Harness createTable(String name, Integer... values) throws Exception {
            List<Iterable<Object>> rows = new ArrayList<>();
            for(Integer value : values) {
                rows.add(row(value));
            }
            try(Connection conn = spliceClassWatcher.createConnection()) {
                try(Statement statement = conn.createStatement()) {
                    statement.executeUpdate(String.format("drop table if exists %s", name));
                }
                new TableCreator(conn)
                        .withCreate(String.format("create table %s(col1 int)", name))
                        .withInsert(String.format("insert into %s values(?)", name))
                        .withRows(rows)
                        .create();
            }
            return this;
        }

        Harness tableShouldContain(String name, Integer... values) throws Exception {
            try(Connection conn = spliceClassWatcher.createConnection()) {
                try(Statement statement = conn.createStatement()) {
                    try(ResultSet resultSet = statement.executeQuery(String.format("select * from %s order by col1 asc", name))) {
                        Arrays.sort(values);
                        for(int value : values) {
                            Assert.assertTrue(resultSet.next());
                            Assert.assertEquals(value, resultSet.getInt(1));
                        }
                        Assert.assertFalse(resultSet.next());
                    }
                }
            }
            return this;
        }

        private void finish() throws SQLException {
            for(Transaction tx : txns.values()) {
                tx.connection.close();
            }
        }

        private Harness when() { return this; } // lexical sugar
        private Harness and() { return this; } // lexical sugar

        private Harness createTxn(String name) throws Exception {
            Transaction txn = new Transaction(this);
            txns.put(name, txn);
            return this;
        }

        private Transaction and(String name) throws Exception {
            return txns.get(name);
        }

        private Transaction then(String name) throws Exception {
            return txns.get(name);
        }

        static class Transaction {

            final Harness harness;
            final TestConnection connection;
            final long txnId;

            private long getTransactionId() throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    try(ResultSet resultSet = statement.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_CURRENT_TRANSACTION()")) {
                        resultSet.next();
                        return resultSet.getLong(1);
                    }
                }
            }

            Transaction(Harness harness) throws Exception {
                this.connection = spliceClassWatcher.createConnection();
                this.connection.setAutoCommit(false);
                this.harness = harness;
                this.txnId = getTransactionId();
            }

            Harness commits() throws Exception {
                connection.commit();
                return harness;
            }

            Harness shouldFailToCommitWithMessage(String errorMessage) throws Exception {
                try {
                    commits();
                    Assert.fail(String.format("expected 'commit' to fail with error '%s', but it did not", errorMessage));
                } catch(Exception e) {
                    checkErrorMessage(errorMessage, e.getMessage());
                }
                return harness;
            }

            Harness shouldFailToUpdateWithMessage(String tableName, int row, int newValue, String errorMessage) throws Exception {
                try {
                    updates(tableName, row, newValue);
                    Assert.fail(String.format("expected 'update' to fail with error '%s', but it did not", errorMessage));
                } catch (Exception e) {
                    checkErrorMessage(errorMessage, e.getMessage());
                }
                return harness;
            }

            private void checkErrorMessage(String expectedErrorMessage, String actualErrorMessage) {
                for(Map.Entry<String, Transaction> entry : harness.txns.entrySet()) {
                    expectedErrorMessage = expectedErrorMessage.replaceAll(String.format("\\!%s\\!", entry.getKey()), String.valueOf(entry.getValue().txnId));
                }
                for(String errorMessagePart : expectedErrorMessage.split("\\!\\?\\!")) {
                    Assert.assertTrue(actualErrorMessage.contains(errorMessagePart));
                }
            }

            Harness rollbacks() throws Exception {
                connection.rollback();
                return harness;
            }

            Harness updates(String tableName, int row, int newValue) throws SQLException {
                try(Statement statement = connection.createStatement()) {
                    statement.executeUpdate(String.format("update %s set col1 = %s where col1 = %s", tableName, newValue, row));
                }
                return harness;
            }
        }
    }

    @Test
    public void testConflictResolutionAtCommitTime() throws Exception {
        Assume.assumeTrue("test ignored because cluster is running with IMMEDIATE conflict resolution strategy", conflictResolutionStrategyIsDeferred());
        Harness harness = new Harness();
        harness.when().createTable("t1", 1, 2, 3, 4)
                .and().createTxn("txn1")
                .and().createTxn("txn2")
                .and("txn1").updates("t1", 1, 200)
                .and("txn2").updates("t1", 1, 300)
                .and("txn1").commits()
                .then("txn2").shouldFailToCommitWithMessage("Committing transaction !txn2! is not possible since it conflicts with transaction !txn1! which is committed already")
                .and().tableShouldContain("t1", 200, 2, 3, 4)
                .and().finish();
    }

    @Test
    public void testConflictResolutionAtCommitTimeMultipleConflicts() throws Exception {
        Assume.assumeTrue("test ignored because cluster is running with IMMEDIATE conflict resolution strategy", conflictResolutionStrategyIsDeferred());
        Harness harness = new Harness();
        harness.when().createTable("t1", 1, 2, 3, 4)
                .and().createTxn("txn1")
                .and().createTxn("txn2")
                .and().createTxn("txn3")

                .and("txn1").updates("t1", 1, 100)
                .and("txn2").updates("t1", 1, 200)

                .and("txn1").updates("t1", 2, 110)
                .and("txn3").updates("t1", 2, 300)

                .and("txn3").updates("t1", 3, 310)
                .and("txn1").commits()
                .then("txn2").shouldFailToCommitWithMessage("Committing transaction !txn2! is not possible since it conflicts with transaction !txn1! which is committed already")
                .then("txn3").shouldFailToCommitWithMessage("Committing transaction !txn3! is not possible since it conflicts with transaction !txn1! which is committed already")
                .and().tableShouldContain("t1", 100, 110, 3, 4)
                .and().finish();
    }

    @Test
    public void testConflictResolutionAtCommitTimeMultipleConflictsWithRollbackInbetween() throws Exception {
        Assume.assumeTrue("test ignored because cluster is running with IMMEDIATE conflict resolution strategy", conflictResolutionStrategyIsDeferred());
        Harness harness = new Harness();
        harness.when().createTable("t1", 1, 2, 3, 4)
                .and().createTxn("txn1")
                .and().createTxn("txn2")
                .and().createTxn("txn3")

                .and("txn1").updates("t1", 1, 100)
                .and("txn2").updates("t1", 1, 200)

                .and("txn1").updates("t1", 2, 110)
                .and("txn3").updates("t1", 2, 300)

                .and("txn2").rollbacks()

                .and("txn3").updates("t1", 3, 310)
                .and("txn1").commits()
                .then("txn3").shouldFailToCommitWithMessage("Committing transaction !txn3! is not possible since it conflicts with transaction !txn1! which is committed already")
                .and().tableShouldContain("t1", 100, 110, 3, 4)
                .and().finish();
    }

    @Test
    public void testConflictResolutionAtCommitTimeMultipleConflictsWithLatestConflictingTxnCommit() throws Exception {
        Assume.assumeTrue("test ignored because cluster is running with IMMEDIATE conflict resolution strategy", conflictResolutionStrategyIsDeferred());
        Harness harness = new Harness();
        harness.when().createTable("t1", 1, 2, 3, 4)
                .and().createTxn("txn1")
                .and().createTxn("txn2")
                .and().createTxn("txn3")

                .and("txn1").updates("t1", 1, 100)

                .and("txn2").updates("t1", 2, 200)

                .and("txn3").updates("t1", 1, 300)
                .and("txn3").updates("t1", 2, 310)
                .and("txn3").updates("t1", 3, 320)

                .and("txn3").commits()
                .then("txn2").shouldFailToCommitWithMessage("Unable to commit transaction !txn2!. It either timed out, or was rolled back by an administrative action or indirectly by a committed conflicting transaction.")
                .and("txn1").shouldFailToCommitWithMessage("Unable to commit transaction !txn1!. It either timed out, or was rolled back by an administrative action or indirectly by a committed conflicting transaction.")
                .and().tableShouldContain("t1", 300, 310, 320, 4)
                .and().finish();
    }
}
