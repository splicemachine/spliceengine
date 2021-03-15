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

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class CurrentFunctionPathIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CurrentFunctionPathIT.class.getSimpleName().toUpperCase();
    private static final String TABLE_NAME="T1";

    protected static SpliceWatcher       spliceClassWatcher  = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    protected static String              functionSchema      = CLASS_NAME + "_FS";
    protected static String              functionName        = functionSchema + ".YHADD";

    protected static String              testSchema1          = CLASS_NAME + "_TEST_SCHEMA1";
    protected static String              testSchema2          = CLASS_NAME + "_TEST_SCHEMA2";
    protected static String              testSchema3          = CLASS_NAME + "_TEST_SCHEMA3";
    protected static String              testSchema4          = CLASS_NAME + "_TEST_SCHEMA4";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void setup() throws Exception {
        setup(spliceClassWatcher);
    }

    private static void setup(SpliceWatcher spliceClassWatcher) throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();

        spliceClassWatcher.execute(format("create schema %s", functionSchema));
        spliceClassWatcher.execute(format("create schema %s", testSchema1));
        spliceClassWatcher.execute(format("create schema %s", testSchema2));
        spliceClassWatcher.execute(format("create schema %s", testSchema3));
        spliceClassWatcher.execute(format("create schema %s", testSchema4));

        String location = getResourceDirectory() + "MathFun.jar";
        String sqlText = format("call sqlj.install_jar('%s', '%s',0)", location, functionName);
        spliceClassWatcher.execute(sqlText);
        sqlText = format("CALL SYSCS_UTIL.SYSCS_SET_GLOBAL_DATABASE_PROPERTY('derby.database.classpath', '%s')",
                         functionName);
        spliceClassWatcher.execute(sqlText);

        String sql = format("create function %s(a int, b int) returns integer language java parameter style java no sql " +
                                    "external name 'com.yh.MathFun.add'", functionName);
        spliceClassWatcher.execute(sql);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        spliceClassWatcher.execute(format("drop function %s", functionName));
        spliceClassWatcher.execute(format("call sqlj.remove_jar('%s',0)", functionName));
        spliceClassWatcher.execute(format("drop schema %s cascade", functionSchema));
        spliceClassWatcher.execute(format("drop schema %s cascade", testSchema1));
        spliceClassWatcher.execute(format("drop schema %s cascade", testSchema2));
        spliceClassWatcher.execute(format("drop schema %s cascade", testSchema3));
        spliceClassWatcher.execute(format("drop schema %s cascade", testSchema4));
    }

    private void shouldSucceed(TestConnection connection, String query, int expected) throws SQLException {
        try(ResultSet resultSet = connection.query(query)) {
            Assert.assertTrue(resultSet.next());
            Assert.assertEquals(expected, resultSet.getInt(1));
            Assert.assertFalse(resultSet.next());
        }
    }

    private void shouldFail(TestConnection connection, String query) throws SQLException {
        try {
            connection.query(query);
            Assert.fail("query should have failed");
        } catch(Exception e) {
            Assert.assertTrue(e instanceof SQLException);
            SQLException sqlException = (SQLException)e;
            Assert.assertEquals(SQLState.LANG_CURRENT_FUNCTION_PATH_SCHEMA_DOES_NOT_EXIST, sqlException.getSQLState());
        }
    }

    private String concate(String... schemas) {
        StringBuilder result = new StringBuilder();
        for(String schema : schemas) {
            result.append("\"").append(schema).append("\",");
        }
        result.deleteCharAt(result.length() - 1);
        return result.toString();
    }

    @Test
    public void testSettingFunctionValidSchemas() throws SQLException {
        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(functionSchema).build()) {
            shouldSucceed(connection, "select yhadd(1, 2) from sysibm.sysdummy1", 3);
        }

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(concate(testSchema2, functionSchema, testSchema3)).build()) {
            shouldSucceed(connection, "select yhadd(1, 2) from sysibm.sysdummy1", 3);
        }

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(concate(functionSchema, testSchema3, testSchema2)).build()) {
            shouldSucceed(connection, "select yhadd(1, 2) from sysibm.sysdummy1", 3);
        }

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(concate(testSchema2, testSchema3, functionSchema)).build()) {
            shouldSucceed(connection, "select yhadd(1, 2) from sysibm.sysdummy1", 3);
        }
    }

    @Test
    public void testSettingFunctionInvalidSchemas() throws SQLException {
        String invalidSchema1 = CLASS_NAME + "invalidSchema1";
        String invalidSchema2 = CLASS_NAME + "invalidSchema2";

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(invalidSchema1).build()) {
            shouldFail(connection, "select yhadd(1, 2) from sysibm.sysdummy1");
        }

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(concate(invalidSchema1, functionSchema, invalidSchema2)).build()) {
            shouldFail(connection, "select yhadd(1, 2) from sysibm.sysdummy1");
        }
    }

    @Test
    public void testSettingFunctionLazySchemaExistanceCheck() throws SQLException {
        String invalidSchema1 = CLASS_NAME + "invalidSchema1";

        try(TestConnection connection = spliceClassWatcher.connectionBuilder().schema(testSchema1).setCurrentFunctionPath(concate(testSchema2, functionSchema, testSchema3, invalidSchema1)).build()) {
            shouldSucceed(connection, "select yhadd(1, 2) from sysibm.sysdummy1", 3);
        }
    }
}
