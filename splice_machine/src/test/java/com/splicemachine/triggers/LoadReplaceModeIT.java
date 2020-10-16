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
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.TriggerBuilder;
import com.splicemachine.test_dao.TriggerDAO;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
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
        params.add(new Object[]{"jdbc:splice://localhost:1527/splicedb;user=splice;password=admin;useSpark=true"});
        return params;
    }

    private String connectionString;

    public LoadReplaceModeIT(String connecitonString) {
        this.connectionString = connecitonString;
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

    void AssureFails(boolean update, String exceptionType, String query) throws Exception {
        try {
            if( update )
                methodWatcher.executeUpdate(query);
            else
                methodWatcher.executeQuery(query);
            Assert.fail(query + ":\nException not thrown");
        } catch( java.sql.SQLIntegrityConstraintViolationException e)
        {
            Assert.assertEquals(exceptionType, "SQLIntegrityConstraintViolationException");
        } catch (SQLException e) {
            Assert.assertEquals(query + ":\nWrong Exception (" + e.getMessage() + ")", exceptionType, e.getSQLState());
        }
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

        AssureFails( true, "SQLIntegrityConstraintViolationException", "INSERT INTO ri2 VALUES (99,9)");

        String signalIdInsert = "87101", signalIdDelete = "87102", signalIdDeleteAfter = "87103", signalIdInsertAfter = "87104";
        createSignalTrigger("TRIG_BEFORE_DELETE", "ri1", "BEFORE DELETE", signalIdDelete);
        createSignalTrigger("TRIG_AFTER_DELETE",  "ri1", "AFTER DELETE",  signalIdDeleteAfter);
        createSignalTrigger("TRIG_BEFORE_INSERT", "ri1", "BEFORE INSERT", signalIdInsert);
        createSignalTrigger("TRIG_AFTER_INSERT",  "ri1", "AFTER INSERT",  signalIdInsertAfter);
        AssureFails( true, signalIdDelete, "DELETE FROM ri1");

        String load_replace1 = SpliceUnitTest.getResourceDirectory()+"load_replace1.csv";
        String load_replace2 = SpliceUnitTest.getResourceDirectory()+"load_replace2.csv";

        // this would normally fail for 3 reasons
        // 1a.) we delete data, so the TRIG_DELETE_BEFORE would abort the transaction
        // 1a.) ri2 table is referncing rows in ri1, so we can't delete them (integrity check)
        // 2a.) when importing, can't insert because TRIG_INSERT_BEFORE would abort
        String sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri1', null, '" + load_replace1 + "', '|', null, null, null, null, 0, '/tmp', true, null)";

        // IMPORT_DATA: fails
        AssureFails( false,MessageId.LANG_IMPORT_TOO_MANY_BAD_RECORDS, String.format(sql, "IMPORT_DATA") );

        // LOAD_REPLACE: ok
        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );
        try(ResultSet rs = methodWatcher.executeQuery("select * from ri1 order by c1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected = "C1 |\n----\n 1 |\n 2 |\n 3 |\n 4 |\n 5 |";
            Assert.assertEquals(actual, expected, actual);
        }

        // have file 1999|199
        // this would normally fail because
        // 2b.) ri2.c2 is referencing ri1.c1, and there's no 9 in ri1.c1 (integrity check)

        sql = "call SYSCS_UTIL.%s ('" + SCHEMA + "', 'ri2', null, '" + load_replace2 + "', '|', null, null, null, null, 0, '/tmp', true, null)";
        AssureFails( false,MessageId.LANG_IMPORT_TOO_MANY_BAD_RECORDS, String.format(sql, "IMPORT_DATA") );
        methodWatcher.executeQuery( String.format(sql, "LOAD_REPLACE") );

        try(ResultSet rs2 = methodWatcher.executeQuery("select * from ri2 order by c1")) {
            String actual = TestUtils.FormattedResult.ResultFactory.toString(rs2);
            String expected = "C1  |C2  |\n-----------\n1999 |199 |";
            Assert.assertEquals(actual, expected, actual);
        }

        methodWatcher.execute("DROP TRIGGER TRIG_BEFORE_DELETE");
        methodWatcher.execute("DROP TRIGGER TRIG_AFTER_DELETE");
        methodWatcher.execute("DROP TRIGGER TRIG_BEFORE_INSERT");
        methodWatcher.execute("DROP TRIGGER TRIG_AFTER_INSERT");

        methodWatcher.execute("DROP TABLE ri2");
        methodWatcher.execute("DROP TABLE ri1");
    }
}
