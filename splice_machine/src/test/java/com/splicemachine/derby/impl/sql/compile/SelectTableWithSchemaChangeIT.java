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
package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.*;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 3/28/18.
 */
public class SelectTableWithSchemaChangeIT extends SpliceUnitTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final String CLASS_NAME = SelectTableWithSchemaChangeIT.class.getSimpleName().toUpperCase();

    protected  static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static SpliceTableWatcher T3 = new SpliceTableWatcher("T3", schemaWatcher.schemaName,"(a3 int, b3 int, c3 int, d3 int)");
    private static SpliceIndexWatcher IX_T3 = new SpliceIndexWatcher(T3.tableName, schemaWatcher.schemaName, "IX_T3", schemaWatcher.schemaName, "(b3)", false, false, false);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(schemaWatcher.schemaName);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(T3)
            .around(IX_T3);

    private Connection conn;

    @Before
    public void setUpTest() throws Exception{
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDownTest() throws Exception{
        try {
            conn.rollback();
        } catch (Exception e) {}
    }

    @Before
    public void setUp() throws Exception {
        methodWatcher.executeUpdate(String.format("INSERT INTO T3 VALUES(1, 1, 1, null)," +
                "(2, 2, 2, null), " +
                "(3, 3, 3, 3)"));
    }

    @Test
    public void testSelectOnTableWithSchemaChangeUsingNonCoveringIndex() throws Exception {
        try (Statement s = conn.createStatement()) {
            /* step1: drop a column c1 in the middle */
            s.execute("alter table t3 drop column c3");

            /* step2: populate a few more rows */
            s.execute("insert into t3(a3, b3, d3) values (4,4,4)");

            /* step3: run queries */
            /* Q1: select columns coming after the dropped columns with non-covering index */
            String sql = "select b3, d3 from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1";

            String expected = "B3 | D3  |\n" +
                    "----------\n" +
                    " 2 |NULL |\n" +
                    " 3 |  3  |\n" +
                    " 4 |  4  |";
            ResultSet rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* Q2: add an outer select on top of Q1 */
            sql = "select * from (select b3, d3 from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1) dt";
            expected = "B3 | D3  |\n" +
                    "----------\n" +
                    " 2 |NULL |\n" +
                    " 3 |  3  |\n" +
                    " 4 |  4  |";
            rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* Q3: add case expression to Q2 */
            sql = "select * from (select b3, case when d3 is null then 999 else d3 end from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1) dt";
            expected = "B3 | 2  |\n" +
                    "---------\n" +
                    " 2 |999 |\n" +
                    " 3 | 3  |\n" +
                    " 4 | 4  |";
            rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* step4: add back column c1 and insert a new row */
            s.execute("alter table t3 add column c3 int");
            s.execute("insert into t3(a3, b3, d3, c3) values (5,5,5,5)");

            /* step 5: run Q1 ~ Q3 using the newly added column */
            /* Q1: select columns coming after the dropped columns with non-covering index */
            sql = "select b3, c3 from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1";

            expected = "B3 | C3  |\n" +
                    "----------\n" +
                    " 2 |NULL |\n" +
                    " 3 |NULL |\n" +
                    " 4 |NULL |\n" +
                    " 5 |  5  |";
            rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* Q2: add an outer select on top of Q1 */
            sql = "select * from (select b3, c3 from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1) dt";
            expected = "B3 | C3  |\n" +
                    "----------\n" +
                    " 2 |NULL |\n" +
                    " 3 |NULL |\n" +
                    " 4 |NULL |\n" +
                    " 5 |  5  |";
            rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* Q3: add case expression to Q2 */
            sql = "select * from (select b3, case when c3 is null then 999 else d3 end from t3 --splice-properties index=ix_t3\n" +
                    "where b3>1) dt";
            expected = "B3 | 2  |\n" +
                    "---------\n" +
                    " 2 |999 |\n" +
                    " 3 |999 |\n" +
                    " 4 |999 |\n" +
                    " 5 | 5  |";
            rs = s.executeQuery(sql);
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }
    }

}
