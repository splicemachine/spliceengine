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
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
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
@Category({SerialTest.class, LongerThanTwoMinutes.class})
public class VacuumIT extends SpliceUnitTest{
    public static final String CLASS_NAME = VacuumIT.class.getSimpleName().toUpperCase();
    protected static String TABLE = "T";
    protected static String TABLEA = "A";
    protected static String TABLED = "D";
    protected static String TABLEE = "E";
    protected static String TABLEG = "G";
    protected static String TABLEH = "H";
    protected static String TABLEI = "I";
    protected static String TABLEJ = "J";

    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule classRule = spliceClassWatcher;

    @Rule
    public SpliceWatcher methodRule = new SpliceWatcher();

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableAWatcher = new SpliceTableWatcher(TABLEA, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableDWatcher = new SpliceTableWatcher(TABLED, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableEWatcher = new SpliceTableWatcher(TABLEE, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableGWatcher = new SpliceTableWatcher(TABLEG, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableHWatcher = new SpliceTableWatcher(TABLEH, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableIWatcher = new SpliceTableWatcher(TABLEI, spliceSchemaWatcher
            .schemaName, "(name varchar(40), title varchar(40), age int)");
    protected static SpliceTableWatcher spliceTableJWatcher = new SpliceTableWatcher(TABLEJ, spliceSchemaWatcher
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

    @Test
    public void testVacuumDoesNotBreakStuff() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        long[] conglomerateNumber = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, TABLE);
        String conglomerateString = Long.toString(conglomerateNumber[0]);
        try(PreparedStatement ps = methodWatcher.prepareStatement(String.format("drop table %s.%s", CLASS_NAME, TABLE))){
            ps.execute();
        }

        try(Admin admin=ConnectionFactory.createConnection(new Configuration()).getAdmin()){
            Set<String> beforeTables=getConglomerateSet(admin.listTables());
            try(CallableStatement callableStatement=methodRule.prepareCall("call SYSCS_UTIL.VACUUM()")){
                callableStatement.execute();
            }
            Set<String> afterTables=getConglomerateSet(admin.listTables());
            assertTrue(beforeTables.contains(conglomerateString));
            Assert.assertFalse(afterTables.contains(conglomerateString));
            Set<String> deletedTables=getDeletedTables(beforeTables,afterTables);
            for(String t : deletedTables){
                long conglom=new Long(t);
                assertTrue(conglom>=DataDictionary.FIRST_USER_TABLE_NUMBER);
            }
        }
    }

    @Test
    public void testVacuumDoesNotDeleteConcurrentCreatedTable() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();
        connection.createStatement().execute(String.format("drop table %s.b if exists", CLASS_NAME));
        connection.commit();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            try (ResultSet rs = connection.createStatement().executeQuery("select * from sys.systables")) {
                assertTrue(rs.next());
            }

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                connection2.createStatement().execute(String.format("create table %s.b (i int)", CLASS_NAME));
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

    @Test
    public void testVacuumDoesNotDeleteTablePossiblyInUse() throws Exception {
        Connection connection = spliceClassWatcher.getOrCreateConnection();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            try (ResultSet rs = connection.createStatement().executeQuery(String.format("select * from %s.e", CLASS_NAME))) {
                rs.next();
            }

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "E");

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                connection2.createStatement().execute(String.format("drop table %s.e", CLASS_NAME));

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
                    
                    for (long congId : conglomerates) {
                        // make sure the table has been dropped by VACUUM
                        assertFalse("Dropped table not in use hasn't been vaccumed", admin.tableExists(TableName.valueOf("splice:" + congId)));
                    }
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
            try (ResultSet rs = connection.createStatement().executeQuery(String.format("select * from %s.g", CLASS_NAME))) {
                rs.next();
            }

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "G");

            try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                for (long congId : conglomerates) {
                    TableName tn = TableName.valueOf("splice:" + congId);
                    TableDescriptorBuilder.ModifyableTableDescriptor td = (TableDescriptorBuilder.ModifyableTableDescriptor)TableDescriptorBuilder.copy(admin.getTableDescriptor(tn));
                    admin.disableTable(tn);
                    admin.deleteTable(tn);
                    td.setValue(SIConstants.TRANSACTION_ID_ATTR, null);
                    admin.createTable(td);
                }
            }

            try (Connection connection2 = spliceClassWatcher.createConnection()) {
                connection2.createStatement().execute(String.format("drop table %s.g", CLASS_NAME));

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

                    for (long congId : conglomerates) {
                        // make sure the table has been dropped by VACUUM
                        assertFalse("Dropped table not in use hasn't been vaccumed", admin.tableExists(TableName.valueOf("splice:" + congId)));
                    }
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
            connection.createStatement().execute(String.format("create table %s.F(i int)", CLASS_NAME));

            long[] conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "F");

            connection.rollback();

            try (Connection connection2 = spliceClassWatcher.createConnection()) {

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    for (long congId : conglomerates) {
                        // make sure the table has been dropped by VACUUM
                        assertFalse("Dropped table not in use hasn't been vaccumed", admin.tableExists(TableName.valueOf("splice:" + congId)));
                    }
                }
            }


            connection.createStatement().execute(String.format("create table %s.F(i int)", CLASS_NAME));

            conglomerates = SpliceAdmin.getConglomNumbers(connection, CLASS_NAME, "F");

            connection.createStatement().execute(String.format("drop table %s.F", CLASS_NAME));

            connection.rollback();

            try (Connection connection2 = spliceClassWatcher.createConnection()) {

                try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
                    try (CallableStatement callableStatement = connection2.prepareCall("call SYSCS_UTIL.VACUUM()")) {
                        callableStatement.execute();
                    }

                    for (long congId : conglomerates) {
                        // make sure the table has been dropped by VACUUM
                        assertFalse("Dropped table not in use hasn't been vaccumed", admin.tableExists(TableName.valueOf("splice:" + congId)));
                    }
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
        connection.createStatement().execute(String.format("drop table %s.b if exists", CLASS_NAME));
        connection.commit();

        boolean autoCommit = connection.getAutoCommit();
        connection.setAutoCommit(false);
        try {
            connection.createStatement().execute(String.format("create table %s.b (i int)", CLASS_NAME));

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
                TableDescriptorBuilder.ModifyableTableDescriptor td = (TableDescriptorBuilder.ModifyableTableDescriptor)TableDescriptorBuilder.copy(admin.getTableDescriptor(tn));
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
                    TableDescriptorBuilder.ModifyableTableDescriptor td = (TableDescriptorBuilder.ModifyableTableDescriptor)TableDescriptorBuilder.copy(admin.getTableDescriptor(tn));
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
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("create index %s.iname on %s.i (name)", CLASS_NAME, CLASS_NAME));
            }
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
                    assertFalse("Truncated table didnt get vacuumed",admin.tableExists(TableName.valueOf("splice:" + congId)));
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

        try(PreparedStatement ps = methodWatcher.prepareStatement(String.format("drop table %s.%s", CLASS_NAME, TABLEA))){
            ps.execute();
        }

        try(Admin admin=ConnectionFactory.createConnection(new Configuration()).getAdmin()){
            admin.disableTable(TableName.valueOf("splice:"+conglomerateString));
            Set<String> beforeTables=getConglomerateSet(admin.listTables());
            try(CallableStatement callableStatement=methodRule.prepareCall("call SYSCS_UTIL.VACUUM()")){
                callableStatement.execute();
            }
            Set<String> afterTables=getConglomerateSet(admin.listTables());
            assertTrue(beforeTables.contains(conglomerateString));
            Assert.assertFalse(afterTables.contains(conglomerateString));
            Set<String> deletedTables=getDeletedTables(beforeTables,afterTables);
            for(String t : deletedTables){
                long conglom=new Long(t);
                assertTrue(conglom>=DataDictionary.FIRST_USER_TABLE_NUMBER);
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
        String name = "splice:"+ com.splicemachine.access.configuration.HBaseConfiguration.DROPPED_CONGLOMERATES_TABLE_NAME;
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
