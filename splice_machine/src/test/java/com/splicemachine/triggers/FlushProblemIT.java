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

import com.splicemachine.db.iapi.reference.SQLState;
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

/**
 * Tests creating/defining triggers.
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class FlushProblemIT {

    private static final String SCHEMA = FlushProblemIT.class.getSimpleName();

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

    public FlushProblemIT(String connectionString) {
        this.connectionString = connectionString;
    }

    @Before
    public void createTables() throws Exception {
        Connection conn = new TestConnection(DriverManager.getConnection(connectionString, new Properties()));
        conn.setSchema(SCHEMA.toUpperCase());
        methodWatcher.setConnection(conn);
    }

    void assureFails(boolean update, String exceptionType, String query) throws Exception {
        SpliceUnitTest.sqlExpectException(methodWatcher, query, exceptionType, update);
    }

    @Test
    public void testIndex2() throws Exception {
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
        methodWatcher.executeUpdate("DELETE FROM riC");
        methodWatcher.executeUpdate("DELETE FROM riB");
        methodWatcher.executeUpdate("DELETE FROM riA");

        methodWatcher.executeUpdate("DELETE FROM riB");
        methodWatcher.executeUpdate("INSERT INTO riA VALUES 11, 12, 13");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (999,11)");
        methodWatcher.executeUpdate("INSERT INTO riC VALUES (9,999)");
        methodWatcher.executeUpdate("INSERT INTO riB VALUES (100,11), (200, 12), (300, 13) ");
        assureFails(true, SQLState.LANG_FK_VIOLATION, "DELETE FROM riB");
        methodWatcher.executeUpdate("call SYSCS_UTIL.SYSCS_FLUSH_TABLE('" + SCHEMA + "', 'RIA')");
        methodWatcher.executeUpdate("call SYSCS_UTIL.SYSCS_FLUSH_TABLE('" + SCHEMA + "', 'RIB')");
        methodWatcher.executeUpdate("call SYSCS_UTIL.SYSCS_FLUSH_TABLE('" + SCHEMA + "', 'RIC')");
        methodWatcher.execute("DROP TABLE riC");
        methodWatcher.execute("DROP TABLE riB");
        methodWatcher.execute("DROP TABLE riA");
    }
}
