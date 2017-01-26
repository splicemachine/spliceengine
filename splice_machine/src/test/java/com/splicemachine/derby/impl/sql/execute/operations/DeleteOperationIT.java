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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

public class DeleteOperationIT {

    private static final String SCHEMA = DeleteOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table T (v1 int primary key, v2 int, v3 int unique)")
                .withInsert("insert into T values(?,?,?)")
                .withRows(rows(row(1, 10, 100), row(2, 20, 200), row(3, 30, 300))).create();

        new TableCreator(connection)
                .withCreate("create table a_test (c1 smallint)")
                .withInsert("insert into a_test values(?)")
                .withRows(rows(row(32767))).create();

        new TableCreator(connection)
                .withCreate("create table customer1 (cust_id int, status boolean)")
                .withInsert("insert into customer1 values(?,?)")
                .withRows(rows(row(1, true), row(2, true), row(3, true), row(4, true), row(5, true))).create();

        new TableCreator(connection)
                .withCreate("create table customer2 (cust_id int, status boolean)")
                .withInsert("insert into customer2 values(?,?)")
                .withRows(rows(row(1, true), row(2, true), row(3, true), row(4, true), row(5, true))).create();

        new TableCreator(connection)
                .withCreate("create table shipment (cust_id int)")
                .withInsert("insert into shipment values(?)")
                .withRows(rows(row(2), row(4))).create();

        new TableCreator(connection)
                .withCreate("create table tdelete (a smallint)")
                .withInsert("insert into tdelete values(?)")
                .withRows(rows(row((short) 1), row(Short.MAX_VALUE))).create();

        try(CallableStatement cs = connection.prepareCall("call SYSCS_UTIl.COLLECT_SCHEMA_STATISTICS(?,false)")){
            cs.setString(1,spliceSchemaWatcher.schemaName);
            cs.execute();
        }

    }

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testDelete() throws Exception {
        // Delete using primary key col
        assertEquals("Incorrect num rows deleted", 1, methodWatcher.executeUpdate("delete from T where v1 = 1"));
        assertEquals("" +
                        "V1 |V2 |V3  |\n" +
                        "-------------\n" +
                        " 2 |20 |200 |\n" +
                        " 3 |30 |300 |",
                TestUtils.FormattedResult.ResultFactory.toString(methodWatcher.executeQuery("select * from T")));

        // Delete using unique index col
        assertEquals("Incorrect num rows deleted", 1, methodWatcher.executeUpdate("delete from T where v3 = 300"));
        assertEquals("" +
                        "V1 |V2 |V3  |\n" +
                        "-------------\n" +
                        " 2 |20 |200 |",
                TestUtils.FormattedResult.ResultFactory.toString(methodWatcher.executeQuery("select * from T")));

        // Delete using normal col
        assertEquals("Incorrect num rows deleted", 1, methodWatcher.executeUpdate("delete from T where v2 = 20"));
        assertEquals("", TestUtils.FormattedResult.ResultFactory.toString(methodWatcher.executeQuery("select * from T")));
    }

    @Test(expected = SQLException.class, timeout = 10000)
    public void testDeleteWithSumOverflowThrowsError() throws Exception {
        try {
            methodWatcher.getStatement().execute("delete from a_test where c1+c1 > 0");
        } catch (SQLException sql) {
            assertEquals("Incorrect SQLState for message " + sql.getMessage(), SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, sql.getSQLState());
            throw sql;
        }
    }

    // If you change one of the following 'delete over join' tests,
    // you probably need to make a similar change to UpdateOperationIT.

    @Test
    public void testDeleteOverNestedLoopJoin() throws Exception {
        doTestDeleteOverJoin("NESTEDLOOP", "customer1");
    }

    private void doTestDeleteOverJoin(String hint, String customerTable) throws Exception {
        StringBuffer sb = new StringBuffer(200);
        sb.append("delete from %s %s \n");
        sb.append("where not exists ( \n");
        sb.append("  select 1 \n");
        sb.append("  from %s %s --SPLICE-PROPERTIES joinStrategy=%s \n");
        sb.append("  where %s.cust_id = %s.cust_id \n");
        sb.append(") \n");
        String query = String.format(sb.toString(), customerTable, "customer", "shipment", "shipment", hint, "customer", "shipment");
        int rows = methodWatcher.executeUpdate(query);
        assertEquals("Incorrect number of rows deleted.", 3, rows);
    }

    @Test(expected = SQLException.class)
    public void testDeleteThrowsDivideByZero() throws Exception {
        try {
            methodWatcher.executeUpdate("delete from a_test where c1/0 = 1");
        } catch (SQLException se) {
            String sqlState = se.getSQLState();
            assertEquals("incorrect SQL state!", "22012", sqlState);
            assertEquals("Incorrect message!", "Attempt to divide by zero.", se.getMessage());
            throw se;
        }
    }

    @Test
    public void testDeleteThrowsOutOfRange() throws Exception {
        try {
            methodWatcher.executeUpdate(String.format("delete from tdelete where a+a > 0"));
        } catch (SQLException se) {
            assertEquals("Incorrect SQL state!", SQLState.LANG_OUTSIDE_RANGE_FOR_DATATYPE, se.getSQLState());
        }
    }
}
