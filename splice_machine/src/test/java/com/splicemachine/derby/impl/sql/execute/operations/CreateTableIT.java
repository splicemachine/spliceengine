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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.File;
import java.sql.*;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class CreateTableIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CreateTableIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schema);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    static File tempDir;

    @BeforeClass
    public static void createTempDirectory() throws Exception
    {
        tempDir = createTempDirectory(schema.schemaName);
    }

    @AfterClass
    public static void deleteTempDirectory() throws Exception
    {
        deleteTempDirectory(tempDir);
    }

    static int counter = 0;
    private static String generateTableName() {
        return "T" + counter++;
    }

    private void shouldContain(String tableName, List<List<Integer>> rows) throws SQLException {
        ResultSet rs =  spliceClassWatcher.executeQuery("SELECT * FROM " + tableName + " ORDER BY c1 ASC");
        for(List<Integer> row : rows) {
            Assert.assertTrue(rs.next());
            for(int i=0; i<row.size(); ++i) {
                Assert.assertEquals((int)row.get(i), rs.getInt(i+1));
            }
        }
        Assert.assertFalse(rs.next());
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "OBL_UNSATISFIED_OBLIGATION"}, justification = "Test not related to business logic")
    private String idOf(final String tableName) throws SQLException {
        PreparedStatement ps = spliceClassWatcher.prepareStatement("SELECT tableid FROM sys.systables WHERE tablename = ?");
        ps.setString(1, tableName);
        ResultSet rs = ps.executeQuery();
        rs.next();
        return rs.getString(1);
    }

    @SuppressFBWarnings(value = {"OBL_UNSATISFIED_OBLIGATION_EXCEPTION_EDGE", "OBL_UNSATISFIED_OBLIGATION"}, justification = "Test not related to business logic")
    private List<String> constraintsOf(final String tableName) throws SQLException {
        String id = idOf(tableName);
        PreparedStatement ps = spliceClassWatcher.prepareStatement("SELECT constraintname FROM sys.sysconstraints WHERE tableid = ?");
        ps.setString(1, id);
        List<String> result = new ArrayList<>();
        ResultSet rs = ps.executeQuery();
        while(rs.next()) {
            result.add(rs.getString(1));
        }
        return result;
    }

    @Test
    public void createTableWithRedundantUniqueConstraintIsIgnored() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        spliceClassWatcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2), CONSTRAINT " + uqCon + " UNIQUE(c1, c2))");
        spliceClassWatcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        // check that data exists
        shouldContain(tbl, Arrays.asList(Arrays.asList(1,2), Arrays.asList(3,4)));

        // check that the unique constraint is NOT created
        Assert.assertFalse(constraintsOf(tbl).contains(uqCon));

        // todo: try to also further verify this by dropping the PK and add duplicate rows once DB-9809 is fixed
    }

    @Test
    public void createTableWithRedundantUniqueConstraintIsIgnoredDifferentColumnOrder() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        spliceClassWatcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2), CONSTRAINT " + uqCon + " UNIQUE(c2, c1))");
        spliceClassWatcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        // check that data exists
        shouldContain(tbl, Arrays.asList(Arrays.asList(1,2), Arrays.asList(3,4)));

        // check that the unique constraint is NOT created
        Assert.assertFalse(constraintsOf(tbl).contains(uqCon));

        // todo: try to also further verify this by dropping the PK and add duplicate rows once DB-9809 is fixed
    }

    @Test
    public void alterTableWithRedundantUniqueConstraintFails() throws Exception {
        String tbl = generateTableName();
        String pkCon = tbl + "PK", uqCon = tbl + "UQ";
        spliceClassWatcher.executeUpdate("CREATE TABLE " + tbl + "(c1 INT, c2 INT, CONSTRAINT " + pkCon + " PRIMARY KEY(c1, c2))");
        spliceClassWatcher.executeUpdate("INSERT INTO " + tbl + " VALUES(1, 2), (3, 4)");

        try {
            spliceClassWatcher.executeUpdate("ALTER TABLE " + tbl + " ADD CONSTRAINT " + uqCon + " UNIQUE(c1, c2)");
            fail("Expected: ERROR 42Z93: Constraints 'CCC1' and 'CCC' have the same set of columns, which is not allowed");
        } catch (SQLException e) {
            Assert.assertEquals("42Z93", e.getSQLState());
            return;
        }
        fail("Expected: ERROR 42Z93: Constraints 'CCC1' and 'CCC' have the same set of columns, which is not allowed");
    }

    /**
     * Tests that the following queries execute successfully, both when a table exists and does not exist:
     *
     * create [external | (global | local) temporary] table table_name ...
     * create ...                                     table if not exists table_name ...
     * create ...                                     table table_name if not exists ...
     *
     */

    private String getExternalResourceDirectory()
    {
        return tempDir.toString() + "/";
    }

    private String appendStorage(String sqlText, String tableType) {
        if (tableType.equals("external")) {
            String path = getExternalResourceDirectory() + "native-spark-table";
            return sqlText + " stored as TEXTFILE location '" + path + "'";
        }
        return sqlText;
    }

    private void testCreateTableIfNotExists(String tableType) throws Exception {
        String tableName = String.format("TEST_CREATE_%s_TABLE_IF_NOT_EXISTS_07061143", tableType.replaceAll(" ", "_").toUpperCase());

        try {
            methodWatcher.executeUpdate(String.format("drop table if exists %s.%s", schema.schemaName, tableName));
            methodWatcher.executeUpdate(appendStorage(String.format("create %s table if not exists %s.%s (c int)", tableType, schema.schemaName, tableName), tableType));

            ResultSet rs = methodWatcher.executeQuery(String.format("select tablename from sys.systables where tablename = '%s'", tableName));
            String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
            Assert.assertTrue(tableName + " has not been created", s.contains(tableName));

            // creating a table with an existing name using "if not exists" returns a warning
            String sqlTexts[] = {
                    "create %s table if not exists %s.%s (c int)",
                    "create %s table %s.%s if not exists (c int)",
                    "create %s table if not exists %s.%s if not exists (c int)"
            };
            try {
                for (String sqlText : sqlTexts) {
                    methodWatcher.executeUpdate(appendStorage(String.format(sqlText, tableType, schema.schemaName, tableName), tableType));
                }
            } catch (SQLException e) {
                Assert.fail("No exception should be thrown");
            }

            // otherwise it's an error
            try {
                methodWatcher.executeUpdate(appendStorage(String.format("create %s table %s.%s (c int)", tableType, schema.schemaName, tableName), tableType));
            } catch (SQLException e) {
                Assert.assertEquals("Wrong Exception", "X0Y32", e.getSQLState());
            }
        }
        finally {
            methodWatcher.executeUpdate(String.format("drop table if exists %s.%s", schema.schemaName, tableName));
        }
    }

    @Test
    public void testCreateTableIfNotExists_BaseTable() throws Exception {
        testCreateTableIfNotExists("");
    }

    @Test
    public void testCreateTableIfNotExists_LocalTempTable() throws Exception {
        testCreateTableIfNotExists("local temporary");
    }

    @Test
    public void testCreateTableIfNotExists_GlobalTempTable() throws Exception {
        testCreateTableIfNotExists("global temporary");
    }

    @Test
    public void testCreateTableIfNotExists_ExternalTable() throws Exception {
        testCreateTableIfNotExists("external");
    }

    @Test
    public void testCommentBeforeCreateTableAs() throws Exception {
        methodWatcher.executeUpdate("create table test_comment_before_create_as_dep_tbl (col int)");

        String createStmt = "create table %s as select * from test_comment_before_create_as_dep_tbl";
        methodWatcher.executeUpdate("-- some SQL-style comment here\n" + String.format(createStmt, generateTableName()));
        methodWatcher.executeUpdate("/* some C-style comment here */\n" + String.format(createStmt, generateTableName()));
        methodWatcher.executeUpdate("/* some mixed */\n-- comments\n" + String.format(createStmt, generateTableName()));
    }

    @Test
    public void testCreateTableWithDefault() throws Exception {
        methodWatcher.executeUpdate("create table test_create_table_with_default (col int not null"
            + ",a boolean not null with default"
            + ",b char(5) not null with default"
            + ",c decimal(15,2) not null with default"
            + ",d double not null with default"
            + ",e int not null with default"
            + ",f bigint not null with default"
            + ",g long varchar not null with default"
            + ",h real not null with default"
            + ",i smallint not null with default"
            + ",j tinyint not null with default"
            + ",k varchar(5) not null with default"
            + ",l float not null with default"
            + ",m numeric not null with default"
            + ",n date not null with default"
            + ",n_fixed date not null"
            + ",o time not null with default"
            + ",o_fixed time not null"
            + ",p timestamp not null with default"
            + ",p_fixed timestamp not null"
            + ",q char(5) for bit data not null with default"
            + ",r long varchar for bit data not null with default"
            + ",s blob not null with default"
            + ",t clob not null with default"
            + ",u varchar(5) for bit data not null with default"
        + ")");
        methodWatcher.executeUpdate("insert into test_create_table_with_default (col, n_fixed, o_fixed, p_fixed) values (42, current date, current time, current timestamp)");
        try (ResultSet rs = methodWatcher.executeQuery("select a,b,c,d,e,f,g,h,i,j,k,l,m from test_create_table_with_default")) {
            String s = TestUtils.FormattedResult.ResultFactory.toString(rs);
            String expected = "A   | B |  C  | D  | E | F | G | H  | I | J | K | L  | M |\n" +
                    "------------------------------------------------------------\n" +
                    "false |   |0.00 |0.0 | 0 | 0 |   |0.0 | 0 | 0 |   |0.0 | 0 |";
            Assert.assertEquals(expected, s);
        }
        try (ResultSet rs = methodWatcher.executeQuery("select n, n_fixed, o, o_fixed, p, p_fixed from test_create_table_with_default")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(rs.getDate(1), rs.getDate(2));
            Assert.assertEquals(rs.getTime(3), rs.getTime(4));
            Assert.assertEquals(rs.getTimestamp(5), rs.getTimestamp(6));
        }
        try (ResultSet rs = methodWatcher.executeQuery("select q,r,s,t,u from test_create_table_with_default")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals("0000000000", rs.getString(1));
            Assert.assertEquals(-1, rs.getBinaryStream(2).read());
            Assert.assertEquals(0, rs.getBlob(3).length());
            Assert.assertEquals(0, rs.getClob(4).length());
            Assert.assertEquals(-1, rs.getBinaryStream(5).read());
        }
    }
}
