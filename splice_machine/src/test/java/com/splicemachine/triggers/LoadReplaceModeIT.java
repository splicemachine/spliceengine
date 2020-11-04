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

package com.splicemachine.triggers;

import com.splicemachine.db.iapi.reference.MessageId;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.compile.DeleteNode;
import com.splicemachine.db.impl.sql.compile.InsertNode;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Properties;

/**
 * Tests creating/defining triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class LoadReplaceModeIT {

    private static final String SCHEMA = LoadReplaceModeIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin"});
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public LoadReplaceModeIT(String connectionString) {
        this.connectionString = connectionString;
    }

    @Before
    public void createTables() throws Exception {
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    void createSignalTrigger(String table, String condition, String type, String signalId) throws Exception {
        String sql = "CREATE TRIGGER " + signalId + " " + condition + " ON " + table +" FOR EACH " + type + "\n" +
                "BEGIN ATOMIC\n" +
                "   SIGNAL SQLSTATE '" + signalId + "' SET MESSAGE_TEXT ='" + condition + " fired.';\n" +
                "END\n";
        methodWatcher.executeUpdate(sql);
    }

    void assureFails(boolean update, String exceptionType, String query) throws Exception {
        SpliceUnitTest.sqlExpectException(methodWatcher, query, exceptionType, update);
    }

    // check that INSERT with LOAD_REPLACE mode still adheres to CONSTRAINT
    @Test
    public void testConstraintsStillWork() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE constraintTable (\n" +
                "c1 INTEGER PRIMARY KEY,\n" +
                "c2 INTEGER,\n" +
                "CONSTRAINT myconstraint CHECK (c1 > c2) )" );

        assureFails( true, SQLState.LANG_CHECK_CONSTRAINT_VIOLATED, "INSERT INTO constraintTable VALUES (9,99)");
        assureFails( true, SQLState.LANG_CHECK_CONSTRAINT_VIOLATED, "INSERT INTO constraintTable " + InsertNode.LOAD_REPLACE_PROPERTY + " VALUES (9,99)");
        methodWatcher.execute("DROP TABLE constraintTable");
    }

    @Test
    public void testLoadReplace() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE ri1 (c1 INTEGER PRIMARY KEY)");

        methodWatcher.executeUpdate("CREATE TABLE ri2 (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER"
                + " REFERENCES ri1(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("INSERT INTO ri1 VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO ri2 VALUES (100,11), (200, 12), (300, 13)");

       assureFails( true, SQLState.LANG_FK_VIOLATION, "INSERT INTO ri2 VALUES (99,9)");

        String  signalIdInsert = "B_I_R", signalIdInsertStatement = "B_I_S",
                signalIdDelete = "B_D_R", signalIdDeleteStatement = "B_D_S",
                signalIdDeleteAfter = "A_D_R", signalIdDeleteAfterStatement = "A_D_S",
                signalIdInsertAfter = "A_I_R", signalIdInsertAfterStatement = "A_I_S";
        createSignalTrigger("ri1", "BEFORE DELETE", "ROW", signalIdDelete);
        createSignalTrigger("ri1", "AFTER DELETE",  "ROW", signalIdDeleteAfter);
        createSignalTrigger("ri1", "BEFORE INSERT", "ROW", signalIdInsert);
        createSignalTrigger("ri1", "AFTER INSERT",  "ROW", signalIdInsertAfter);

        createSignalTrigger("ri1", "BEFORE DELETE", "STATEMENT", signalIdDeleteStatement);
        createSignalTrigger("ri1", "AFTER DELETE",  "STATEMENT", signalIdDeleteAfterStatement);
        createSignalTrigger("ri1", "BEFORE INSERT", "STATEMENT", signalIdInsertStatement);
        createSignalTrigger("ri1", "AFTER INSERT",  "STATEMENT", signalIdInsertAfterStatement);

        assureFails( true, signalIdDeleteStatement, "DELETE FROM ri1");

        String load_replace1 = SpliceUnitTest.getResourceDirectory()+"load_replace1.csv";
        String load_replace2 = SpliceUnitTest.getResourceDirectory()+"load_replace2.csv";

        // this would normally fail for 3 reasons
        // 1a.) we delete data, so the TRIG_DELETE_BEFORE would abort the transaction
        // 1a.) ri2 table is referncing rows in ri1, so we can't delete them (integrity check)
        // 2a.) when importing, can't insert because TRIG_INSERT_BEFORE would abort
        String sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri1', null, '" + load_replace1 + "', '|', null, null, null, null, 0, '/tmp', true, null)";

        // IMPORT_DATA: fails
        assureFails( false, signalIdInsertStatement, String.format(sql, "IMPORT_DATA") );

        // LOAD_REPLACE: ok
        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );

        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from ri1 order by c1",
                "C1 |\n----\n 1 |\n 2 |\n 3 |\n 4 |\n 5 |", false);

        // have file 1999|199
        // this would normally fail because
        // 2b.) ri2.c2 is referencing ri1.c1, and there's no 9 in ri1.c1 (integrity check)

        sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri2', null, '" + load_replace2 + "', '|', null, null, null, null, 0, '/tmp', true, null)";
        assureFails( false,MessageId.LANG_IMPORT_TOO_MANY_BAD_RECORDS, String.format(sql, "IMPORT_DATA") );

        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );

        // using INSERT --splice-properties insertMode=LOAD_REPLACE
        assureFails( true, SQLState.LANG_FK_VIOLATION, "INSERT INTO ri2 VALUES (777, 77)" );

        methodWatcher.executeUpdate("INSERT INTO ri2 " + InsertNode.LOAD_REPLACE_PROPERTY + "\nVALUES (777, 77)"); // should not fail
        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from ri2 order by c1",
                "C1  |C2  |\n-----------\n 777 |77  |\n1999 |199 |", false);


        // using DELETE --splice-properties noTriggerRI=1
        assureFails( true, signalIdDeleteStatement, "DELETE FROM ri1");
        methodWatcher.executeUpdate("DELETE FROM ri1 " + DeleteNode.NO_TRIGGER_RI_PROPERTY); // should not fail

        methodWatcher.execute("DROP TRIGGER " + signalIdDelete);
        methodWatcher.execute("DROP TRIGGER " + signalIdDeleteAfter);
        methodWatcher.execute("DROP TRIGGER " + signalIdInsert);
        methodWatcher.execute("DROP TRIGGER " + signalIdInsertAfter);
        methodWatcher.execute("DROP TRIGGER " + signalIdDeleteStatement);
        methodWatcher.execute("DROP TRIGGER " + signalIdDeleteAfterStatement);
        methodWatcher.execute("DROP TRIGGER " + signalIdInsertStatement);
        methodWatcher.execute("DROP TRIGGER " + signalIdInsertAfterStatement);

        methodWatcher.execute("DROP TABLE ri2");
        methodWatcher.execute("DROP TABLE ri1");
    }

    @Test
    public void testIndex() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE riA (c1 INTEGER PRIMARY KEY)");

        methodWatcher.executeUpdate("CREATE TABLE riB (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES riA(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("CREATE TABLE riC (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES riB(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (100,11), (200, 12), (300, 13)");
        methodWatcher.executeUpdate("INSERT INTO riC VALUES (1,100), (2, 200), (3, 300)");

        methodWatcher.executeUpdate("DELETE FROM riB " + DeleteNode.NO_TRIGGER_RI_PROPERTY);
        methodWatcher.executeUpdate("DELETE FROM riA"); // shouldn't fail, no indices anymore

        // make sure indices are correct:
        // we shouldn't be able to use reference 11, as it was deleted
        assureFails( true, SQLState.LANG_FK_VIOLATION, "INSERT INTO riB VALUES (333,11)");

        methodWatcher.executeUpdate("INSERT INTO riB " + InsertNode.LOAD_REPLACE_PROPERTY +
                "\nVALUES (233,21)");

        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from riB order by c1",
                "C1  |C2 |\n" +
                        "---------\n" +
                        "233 |21 |", false);

        // should be OK: riC has an index on values 100, 200 and 300 in riB, but since riB doesn't contain
        // these values anymore, we only check if the value to be delete (21) is in the index 100, 200, 300,
        // and it isn't, so we can delete.
        methodWatcher.executeUpdate("DELETE FROM riB");

        methodWatcher.executeUpdate("INSERT INTO riB " + InsertNode.LOAD_REPLACE_PROPERTY +
                "\nVALUES (999,99)");
        // should be able to refernce these values even when inserted with LOAD REPLACE
        methodWatcher.executeUpdate("INSERT INTO riC VALUES (9,999)");


        methodWatcher.executeUpdate("INSERT INTO riB " + InsertNode.LOAD_REPLACE_PROPERTY +
                "\nVALUES (100,11), (200, 12), (300, 13) ");

        assureFails( true, SQLState.LANG_FK_VIOLATION, "DELETE FROM riB");

        methodWatcher.execute("DROP TABLE riC");
        methodWatcher.execute("DROP TABLE riB");
        methodWatcher.execute("DROP TABLE riA");
    }


    // check that INSERT with LOAD_REPLACE mode still adheres to PK constraints
    @Test
    public void testPK() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE riA (c1 INTEGER PRIMARY KEY)");
        methodWatcher.executeUpdate( "INSERT INTO riA " + InsertNode.LOAD_REPLACE_PROPERTY + " VALUES 9");
        assureFails( true, SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                "INSERT INTO riA " + InsertNode.LOAD_REPLACE_PROPERTY + " VALUES 9");
        methodWatcher.executeUpdate( "DELETE from riA");

        assureFails( true, SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,
                "INSERT INTO riA " + InsertNode.LOAD_REPLACE_PROPERTY + " VALUES 9, 9");
        methodWatcher.execute("DELETE from riA");
        methodWatcher.execute("DROP TABLE riA");
    }

    // since LOAD_REPLACE consists of the 2 steps DELETE+INSERT,
    // make sure if the INSERT part fails that the DELETE is rolled back
    @Test
    public void testLoadReplaceRollback() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE riA (c1 INTEGER PRIMARY KEY)");
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 1, 3, 7, 8");

        String load_replace1 = SpliceUnitTest.getResourceDirectory()+"load_replace1.csv";
        String sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'riA', null, 'not/existing/file', " +
                "'|', null, null, null, null, 0, '/tmp', true, null)";

        String exception = com.splicemachine.db.shared.common.reference.SQLState.LANG_FILE_DOES_NOT_EXIST.split("\\.")[0];
        assureFails( false, exception, String.format(sql, "LOAD_REPLACE") );

        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from riA order by c1",
                "C1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 3 |\n" +
                        " 7 |\n" +
                        " 8 |", false);

        methodWatcher.execute("DROP TABLE riA");
    }

    @Test
    public void TestTruncateLater() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE riA (c1 INTEGER PRIMARY KEY)");

        methodWatcher.executeUpdate("CREATE TABLE riB (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES riA(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("CREATE TABLE riC (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES riB(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (100,11), (200, 12), (300, 13)");

        // when removing this with TRUNCATE, this needs to truncate FK indices
        methodWatcher.executeUpdate("DELETE FROM riB --splice-properties noTriggerRI=1");
        methodWatcher.executeUpdate("DELETE FROM riA");

        methodWatcher.execute("DROP TABLE riC");
        methodWatcher.execute("DROP TABLE riB");
        methodWatcher.execute("DROP TABLE riA");
    }
}
