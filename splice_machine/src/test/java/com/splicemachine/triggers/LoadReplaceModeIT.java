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
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Collection;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

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
        //params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
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

    void createSignalTrigger(String name, String table, String condition, String signalId) throws Exception {
        String sql = "CREATE TRIGGER " + name + " " + condition + " ON " + table +" FOR EACH ROW\n" +
                "BEGIN ATOMIC\n" +
                "   SIGNAL SQLSTATE '" + signalId + "' SET MESSAGE_TEXT ='" + condition + " fired.';\n" +
                "END\n";
        methodWatcher.executeUpdate(sql);
    }

    void assureFails(boolean update, String exceptionType, String query) throws Exception {
        SpliceUnitTest.sqlExpectException(methodWatcher, query, exceptionType, update);
    }

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
    public void test1() throws Exception {
        methodWatcher.executeUpdate("CREATE TABLE ri1 (c1 INTEGER PRIMARY KEY)");

        methodWatcher.executeUpdate("CREATE TABLE ri2 (\n" +
                "   c1 INTEGER PRIMARY KEY,\n" +
                "   c2 INTEGER REFERENCES ri1(c1)"
                + ")"
        );
        methodWatcher.executeUpdate("INSERT INTO ri1 VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO ri2 VALUES (100,11), (200, 12), (300, 13)");

        assureFails( true, SQLState.LANG_FK_VIOLATION, "INSERT INTO ri2 VALUES (99,9)");

        String signalIdInsert = "87101", signalIdDelete = "87102", signalIdDeleteAfter = "87103", signalIdInsertAfter = "87104";
        createSignalTrigger("TRIG_BEFORE_DELETE", "ri1", "BEFORE DELETE", signalIdDelete);
        createSignalTrigger("TRIG_AFTER_DELETE",  "ri1", "AFTER DELETE",  signalIdDeleteAfter);
        createSignalTrigger("TRIG_BEFORE_INSERT", "ri1", "BEFORE INSERT", signalIdInsert);
        createSignalTrigger("TRIG_AFTER_INSERT",  "ri1", "AFTER INSERT",  signalIdInsertAfter);
        assureFails( true, signalIdDelete, "DELETE FROM ri1");

        String load_replace1 = SpliceUnitTest.getResourceDirectory()+"load_replace1.csv";
        String load_replace2 = SpliceUnitTest.getResourceDirectory()+"load_replace2.csv";

        // this would normally fail for 3 reasons
        // 1a.) we delete data, so the TRIG_DELETE_BEFORE would abort the transaction
        // 1a.) ri2 table is referncing rows in ri1, so we can't delete them (integrity check)
        // 2a.) when importing, can't insert because TRIG_INSERT_BEFORE would abort
        String sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri1', null, '" + load_replace1 + "', '|', null, null, null, null, 0, '/tmp', true, null)";

        // IMPORT_DATA: fails
        assureFails( false,MessageId.LANG_IMPORT_TOO_MANY_BAD_RECORDS, String.format(sql, "IMPORT_DATA") );

        // LOAD_REPLACE: ok
        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );
        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from ri1 order by c1",
                "C1 |\n----\n 1 |\n 2 |\n 3 |\n 4 |\n 5 |", false);

        // have file 1999|199
        // this would normally fail because
        // 2b.) ri2.c2 is referencing ri1.c1, and there's no 9 in ri1.c1 (integrity check)

        sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri2', null, '" + load_replace2 + "', '|', null, null, null, null, 0, '/tmp', true, null)";
        assureFails( false,MessageId.LANG_IMPORT_TOO_MANY_BAD_RECORDS, String.format(sql, "IMPORT_DATA") );

        // INSERT INTO "LOADREPLACEMODEIT"."RI2"("C1", "C2") --splice-properties useSpark=true ,
        // insertMode=LOAD_REPLACE, statusDirectory=/tmp, badRecordsAllowed=0,
        // bulkImportDirectory=null, samplingOnly=false, outputKeysOnly=false, skipSampling=false SELECT "C1","C2" from new
        // com.splicemachine.derby.vti.SpliceFileVTI('/Users/martinrupp/spliceengine/splice_machine/src/test/test-data/load_replace2.csv',NULL,'|',NULL,'HH:mm:ss','yyyy-MM-dd','yyyy-MM-dd HH:mm:ss','true','UTF-8','/tmp',0 )
        // AS importVTI ("C1" INTEGER, "C2" INTEGER)
        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );
//        methodWatcher.executeUpdate("DELETE FROM ri2 " + DeleteNode.NO_TRIGGER_RI_PROPERTY); // should not fail
//        methodWatcher.executeUpdate("INSERT INTO ri2 " + InsertNode.LOAD_REPLACE_PROPERTY + "\nVALUES (1999,199)"); // should not fail

//        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from ri2 order by c1",
//                "C1  |C2  |\n-----------\n1999 |199 |", false);

        // using INSERT --splice-properties insertMode=LOAD_REPLACE
        assureFails( true, SQLState.LANG_FK_VIOLATION, "INSERT INTO ri2 VALUES (777, 77)" );

        methodWatcher.executeUpdate("INSERT INTO ri2 " + InsertNode.LOAD_REPLACE_PROPERTY + "\nVALUES (777, 77)"); // should not fail
        SpliceUnitTest.sqlExpectToString( methodWatcher, "select * from ri2 order by c1",
                "C1  |C2  |\n-----------\n 777 |77  |\n1999 |199 |", false);


        // using DELET --splice-properties noTriggerRI=1
        assureFails( true, signalIdDelete, "DELETE FROM ri1");
        methodWatcher.executeUpdate("DELETE FROM ri1 " + DeleteNode.NO_TRIGGER_RI_PROPERTY); // should not fail

        methodWatcher.execute("DROP TRIGGER TRIG_BEFORE_DELETE");
        methodWatcher.execute("DROP TRIGGER TRIG_AFTER_DELETE");
        methodWatcher.execute("DROP TRIGGER TRIG_BEFORE_INSERT");
        methodWatcher.execute("DROP TRIGGER TRIG_AFTER_INSERT");

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
        methodWatcher.executeUpdate("INSERT INTO riB " + InsertNode.LOAD_REPLACE_PROPERTY +
                "\nVALUES (100,11), (200, 12), (300, 13) ");

        assureFails( true, "FK", "DELETE FROM riC");
    }
}
