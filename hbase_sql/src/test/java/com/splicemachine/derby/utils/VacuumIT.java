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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.apache.hadoop.hbase.client.*;
import org.apache.log4j.Logger;
import org.junit.*;

import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Scott Fines
 * Date: 3/19/14
 */
@Category({SerialTest.class})
public class VacuumIT extends SpliceUnitTest{
    private static Logger LOG=Logger.getLogger(VacuumIT.class);
    public static final String CLASS_NAME = VacuumIT.class.getSimpleName().toUpperCase();
    final protected static String TABLE = "T";
    final protected static String TABLEA = "A";
    final protected static String TABLED = "D";
    final protected static String TABLEE = "E";
    final protected static String TABLEG = "G";
    final protected static String TABLEH = "H";
    final protected static String TABLEI = "I";
    final protected static String TABLEJ = "J";

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule classRule = spliceClassWatcher;

    @Rule
    public SpliceWatcher methodRule = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    final protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    final protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableAWatcher = new SpliceTableWatcher(TABLEA, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableDWatcher = new SpliceTableWatcher(TABLED, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableEWatcher = new SpliceTableWatcher(TABLEE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableGWatcher = new SpliceTableWatcher(TABLEG, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableHWatcher = new SpliceTableWatcher(TABLEH, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableIWatcher = new SpliceTableWatcher(TABLEI, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    final protected static SpliceTableWatcher spliceTableJWatcher = new SpliceTableWatcher(TABLEJ, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableAWatcher)
            .around(spliceTableDWatcher)
            .around(spliceTableEWatcher)
            .around(spliceTableGWatcher)
            .around(spliceTableHWatcher)
            .around(spliceTableIWatcher)
            .around(spliceTableJWatcher);

    @BeforeClass
    public static void setUpClass() throws Exception {
        try(SpliceWatcher methodWatcher = new SpliceWatcher(null)) {
            LOG.info("check other tests didn't leave transactions open, which is bad for Vacuum tests.");
            String name = "A test before VacuumIT"; // ... failed to close transactions
            SpliceUnitTest.waitForStaleTransactions(methodWatcher, name, 10, true);
            LOG.info("done. vacuum leftovers from other tests..");
            methodWatcher.execute("call SYSCS_UTIL.VACUUM()");
            LOG.info("done.");
        }
    }

    @Test
    public void testVacuumDoesNotBreakStuff() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        long[] conglomerateNumber = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, TABLE);
        String conglomerateString = Long.toString(conglomerateNumber[0]);
        try (PreparedStatement ps = methodWatcher.prepareStatement(String.format("drop table %s.%s", CLASS_NAME, TABLE))) {
            ps.execute();
        }

        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            Set<String> beforeTables = getConglomerateSet(admin.listTables());
            try (CallableStatement callableStatement = methodRule.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }
            Set<String> afterTables = getConglomerateSet(admin.listTables());
            assertTrue(beforeTables.contains(conglomerateString));
            Assert.assertFalse(afterTables.contains(conglomerateString));
            Set<String> deletedTables = getDeletedTables(beforeTables, afterTables);
            for (String t : deletedTables) {
                long conglom = Long.parseLong(t);
                assertTrue(conglom >= DataDictionary.FIRST_USER_TABLE_NUMBER);
            }
        }
    }

    void execute(Connection connection, String sql) throws SQLException {
        try(Statement s = connection.createStatement()) {
            s.execute(sql);
        }

    }

    void executeOneRs(Connection connection, String sql, boolean rsNextReturn) throws SQLException {
        try( Statement s = connection.createStatement() ) {
            try (ResultSet rs = s.executeQuery(sql)) {
                assertEquals(rs.next(), rsNextReturn);
            }
        }
    }

    @Test
    public void testVacuumDoesNotDeleteConcurrentCreatedTable() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        execute(connection, String.format("drop table %s.b if exists", CLASS_NAME));
        connection.commit();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            executeOneRs( connection, "select * from sys.systables", true);

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                execute(connection2, String.format("create table %s.b (i int)", CLASS_NAME));
                long[] conglomerates = SpliceAdmin.getConglomNumbers(connection2, CLASS_NAME, "B");


                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }
                    for (long congId : conglomerates) {
                        // make sure the table exists in HBase and hasn't been dropped by VACUUM
                        admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                    }
                }
            }
        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }

    void assertCleaned(Admin admin, long[] conglomerates) throws Exception {
        for (long congId : conglomerates) {
            // make sure the table has been dropped by VACUUM
            assertFalse("Dropped table splice:" + congId + " not in use hasn't been vaccumed",
                admin.tableExists(TableName.valueOf("splice:" + congId)));
        }
    }

    @Test
    public void testVacuumDoesNotDeleteTablePossiblyInUse() throws Exception {
        int oldest = SpliceUnitTest.getOldestActiveTransaction(methodWatcher);
        LOG.info("VacuumIT: oldest: " + oldest + "\n");
        Connection connection = spliceClassWatcher.getOrCreateConnection();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            executeOneRs( connection, String.format("select * from %s.e", CLASS_NAME), false);

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "E");

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                execute(connection2, String.format("drop table %s.e", CLASS_NAME));

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }
                    for (long congId : conglomerates) {
                        // make sure the table exists in HBase and hasn't been dropped by VACUUM
                        admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                    }

                    // Now commit
                    connection.commit();

                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    assertCleaned(admin, conglomerates);

                }
            }

        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }

    @Test
    public void testVacuumDoesNotDeleteTableWithoutTransactionIdPossiblyInUse() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            executeOneRs(connection, String.format("select * from %s.g", CLASS_NAME), false);

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "G");

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    TableName tn = TableName.valueOf("splice:" + congId);
                    HTableDescriptor td = admin.getTableDescriptor(tn);
                    admin.disableTable(tn);
                    admin.deleteTable(tn);
                    td.setValue(SIConstants.TRANSACTION_ID_ATTR, null);
                    admin.createTable(td);
                }
            }

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                execute(connection2, String.format("drop table %s.g", CLASS_NAME));

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }
                    for (long congId : conglomerates) {
                        // make sure the table exists in HBase and hasn't been dropped by VACUUM
                        admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                    }

                    // Now commit
                    connection.commit();

                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    assertCleaned(admin, conglomerates);
                }
            }

        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }

    @Test
    public void testVacuumRemovesTableFromRolledbackTransaction() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            execute(connection, String.format("create table %s.F(i int)", CLASS_NAME));

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "F");

            connection.rollback();

            try (Connection connection2 = spliceClassWatcher.createConnection()) {

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    assertCleaned(admin, conglomerates);
                }
            }


            execute(connection, String.format("create table %s.F(i int)", CLASS_NAME));

            conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "F");

            execute(connection, String.format("drop table %s.F", CLASS_NAME));

            connection.rollback();

            try (Connection connection2 = spliceClassWatcher.createConnection()) {

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    assertCleaned(admin, conglomerates);
                }
            }
        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }


    @Test
    public void testVacuumDoesNotBlockOnExistingTransactions() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        execute(connection, String.format("drop table %s.b if exists", CLASS_NAME));
        connection.commit();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            execute(connection, String.format("create table %s.b (i int)", CLASS_NAME));

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "B");

                try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                    callableStatement.execute();
                }

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    for (long congId : conglomerates) {
                        // make sure the table exists in HBase and hasn't been dropped by VACUUM
                        admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                    }
                }
            }
        } finally {
            connection.commit();
            connection.setAutoCommit(autoCommit);
        }
    }

    @Test
    public void testVacuumDoesNotRemoveTablesWithoutTransactionId() throws Exception {

        Connection connection = spliceClassWatcher.getOrCreateConnection();

        long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "D");

        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            for (long congId : conglomerates) {
                // make sure the table exists in HBase and hasn't been dropped by VACUUM
                TableName tn = TableName.valueOf("splice:" + congId);
                HTableDescriptor td = admin.getTableDescriptor(tn);
                admin.disableTable(tn);
                admin.deleteTable(tn);
                td.setValue(SIConstants.TRANSACTION_ID_ATTR, null);
                admin.createTable(td);
            }
        }

        try {
            try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table exists in HBase and hasn't been dropped by VACUUM
                    admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                }
            }

            // now drop the table
            try (CallableStatement callableStatement = connection.prepareCall(String.format("drop table %s.D", CLASS_NAME))) {
                callableStatement.execute();
            }

            try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table doesn't exists in HBase anymore
                    assertFalse("Dropped table didnt get vacuumed",admin.tableExists(TableName.valueOf("splice:" + congId)));
                }
            }

        } finally {
            connection.commit();
        }
    }

    @Test
    public void testVacuumRemovesTablesWithDroppedTransactionIdInDescriptor() throws Exception {

        Connection connection = spliceClassWatcher.getOrCreateConnection();

        long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "H");

        try {
            try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table exists in HBase and hasn't been dropped by VACUUM
                    admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                }
            }

            // now drop the table
            try (CallableStatement callableStatement = connection.prepareCall(String.format("drop table %s.H", CLASS_NAME))) {
                callableStatement.execute();
            }

            // Get transaction Id and remove the row from splice:DROPPED_CONGLOMERATES
            long txnId = -1;
            String name = "splice:"+ com.splicemachine.access.configuration.HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME;
            try (Table droppedConglomerates = ConnectionFactory.createConnection(new Configuration()).getTable(TableName.valueOf(name))) {
                List<Delete> deletes = new ArrayList<>();
                Get get;
                for (long congId : conglomerates) {
                    deletes.add(new Delete(Bytes.toBytes(congId)));
                    Result result = droppedConglomerates.get(new Get(Bytes.toBytes(congId)));
                    byte[] value = result.getValue(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES);
                    txnId = Bytes.toLong(value);
                }
                droppedConglomerates.delete(deletes);
            }

            // Mark them as dropped on the table descriptor
            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table exists in HBase and hasn't been dropped by VACUUM
                    TableName tn = TableName.valueOf("splice:" + congId);
                    HTableDescriptor td = admin.getTableDescriptor(tn);
                    admin.disableTable(tn);
                    admin.deleteTable(tn);
                    td.setValue(SIConstants.DROPPED_TRANSACTION_ID_ATTR, Long.toString(txnId));
                    admin.createTable(td);
                }
            }


            try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table doesn't exists in HBase anymore
                    assertFalse("Dropped table didnt get vacuumed",admin.tableExists(TableName.valueOf("splice:" + congId)));
                }
            }

        } finally {
            connection.commit();
        }
    }


    @Test
    public void testVacuumRemovesConglomeratesAfterTruncateTable() throws Exception {

        Connection connection = spliceClassWatcher.getOrCreateConnection();
        try {
            execute(connection, String.format("create index %s.iname on %s.i (name)", CLASS_NAME, CLASS_NAME));
            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "I");

            assertEquals(2, conglomerates.length);

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table exists in HBase and hasn't been dropped by VACUUM
                    admin.getTableDescriptor(TableName.valueOf("splice:" + congId));
                }
            }

            // now truncate the table
            try (CallableStatement callableStatement = connection.prepareCall(String.format("truncate table %s.I", CLASS_NAME))) {
                callableStatement.execute();
            }

            try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    // make sure the table doesn't exists in HBase anymore
                    assertFalse("Truncated table didnt get vacuumed", admin.tableExists(TableName.valueOf("splice:" + congId)));
                }
            }

        } finally {
            connection.commit();
        }
    }


    @Test
    public void testVacuumDisabledTable() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        long[] conglomerateNumber = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, TABLEA);
        String conglomerateString = Long.toString(conglomerateNumber[0]);

        try (PreparedStatement ps = methodWatcher.prepareStatement(String.format("drop table %s.%s", CLASS_NAME, TABLEA))) {
            ps.execute();
        }

        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            admin.disableTable(TableName.valueOf("splice:" + conglomerateString));
            Set<String> beforeTables = getConglomerateSet(admin.listTables());
            try (CallableStatement callableStatement = methodRule.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                callableStatement.execute();
            }
            Set<String> afterTables = getConglomerateSet(admin.listTables());
            assertTrue(beforeTables.contains(conglomerateString));
            Assert.assertFalse(afterTables.contains(conglomerateString));
            Set<String> deletedTables = getDeletedTables(beforeTables, afterTables);
            for (String t : deletedTables) {
                long conglom = Long.parseLong(t);
                assertTrue(conglom >= DataDictionary.FIRST_USER_TABLE_NUMBER);
            }
        }
    }

    @Test
    public void testVacuumRemoveDroppedConglomerateEntryIfTableAlreadyRemoved() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        long[] conglomerateNumbers = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, TABLEJ);
        try (CallableStatement callableStatement = connection.prepareCall(String.format("drop table %s.J", CLASS_NAME))) {
            callableStatement.execute();
        }
        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            for (long congId : conglomerateNumbers) {
                TableName tableName = TableName.valueOf("splice:" + congId);
                admin.disableTable(tableName);
                admin.deleteTable(tableName);
                assertFalse(admin.tableExists(tableName));
            }
        }
        String name = "splice:" + com.splicemachine.access.configuration.HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME;
        try (Table droppedConglomerates = ConnectionFactory.createConnection(new Configuration()).getTable(TableName.valueOf(name))) {
            for (long congId : conglomerateNumbers) {
                Result result = droppedConglomerates.get(new Get(Bytes.toBytes(congId)));
                assertFalse(result.isEmpty());
            }
        }
        try (CallableStatement callableStatement = connection.prepareCall("call SYSCS_UTIL.VACUUM()")) {
            callableStatement.execute();
        }
        try (Table droppedConglomerates = ConnectionFactory.createConnection(new Configuration()).getTable(TableName.valueOf(name))) {
            for (long congId : conglomerateNumbers) {
                Result result = droppedConglomerates.get(new Get(Bytes.toBytes(congId)));
                assertTrue(result.isEmpty());
            }
        }
    }

    private Set<String> getConglomerateSet(HTableDescriptor[] tableDescriptors) {
        Set<String> conglomerates = new HashSet<>();
        for (HTableDescriptor descriptor:tableDescriptors) {
            String tableName = descriptor.getNameAsString();
            String[] s = tableName.split(":");
            if (s.length < 2) continue;
            conglomerates.add(s[1]);
        }

        return conglomerates;
    }

    private Set<String> getDeletedTables(Set<String> beforeTables, Set<String> afterTables) {
        for (String t : afterTables) {
            beforeTables.remove(t);
        }
        return beforeTables;
    }
}
