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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;
import java.util.Properties;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 *
 *
 *
 */
public class PushAggregationIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(PushAggregationIT.class);
    public static final String CLASS_NAME = PushAggregationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 10, 100),
                        row(1, 1, 11, 101),
                        row(1, 1, 12, 102),
                        row(1, 2, 20, 200),
                        row(1, 2, 21, 201),
                        row(2, 1, 10, 100),
                        row(2, 1, 11, 101),
                        row(2, 1, 12, 102),
                        row(2, 2, 20, 200),
                        row(2, 2, 21, 201)))
                .create();


        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int)")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 30, 300),
                        row(1, 1, 31, 301),
                        row(1, 2, 40, 400),
                        row(1, 2, 41, 401)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }


    @Test
    public void testPushAggregationOnSingleTableSingeAggregateColumn() throws Exception {
        String sqlText = "select a1, sum(c1) from t1, t2 where b1=b2 group by a1";

        rowContainsQuery(9,"explain " + sqlText,"GroupBy",methodWatcher);

        String expected =
                "A1 | 2  |\n" +
                        "---------\n" +
                        " 1 |148 |\n" +
                        " 2 |148 |";


        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testPushAggregationOnSingleTableMultipleAggregateColumns() throws Exception {
        String sqlText = "select a1, sum(c1), count(c1) from t1, t2 where b1=b2 group by a1";

        rowContainsQuery(9,"explain " + sqlText,"GroupBy",methodWatcher);

        String expected =
                "A1 | 2  | 3 |\n" +
                        "-------------\n" +
                        " 1 |148 |10 |\n" +
                        " 2 |148 |10 |";


        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }
}
