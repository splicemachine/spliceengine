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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class LogicalProfilePropagationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = LogicalProfilePropagationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table t1 (c int, d int)")
                .withInsert("insert into t1 values (?, ?)")
                .withRows(rows(
                        row(1, 1),
                        row(2, 2),
                        row(3, 3),
                        row(4, 1),
                        row(5, 2),
                        row(6, 3),
                        row(7, 3),
                        row(8, 3),
                        row(9, 4),
                        row(10, 5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table test_empty (c1 int primary key, c2 int, c3 int)")
                .withIndex("create index test_empty_idx on test_empty(c1, c2)")
                .create();

        spliceClassWatcher.execute(format("analyze schema %s", CLASS_NAME));
    }

    /**** single-table cases ****/

    @Test
    public void testSinglePointSelect() throws Exception {
        String query = "explain select * from t1 where d = 3";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 4         | 1   | 10  | 3,6,7,8       |
        // | 2                | 1         | 3   | 3   | 3,3,3,3       |
        rowContainsQuery(3, query, "logicalProfile=[(1,4,1,10),(2,1,3,3)]", CM_V2, methodWatcher);
    }

    @Test
    public void testRangeSelectBothInclusive() throws Exception {
        String query = "explain select * from t1 where c >= 7 and c <= 10";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 4         | 7   | 10  | 7,8,9,10      |
        // | 2                | 3.2       | 1   | 5   | 3,3,4,5       |
        rowContainsQuery(3, query, "logicalProfile=[(1,4,7,10),(2,3.2,1,5)]", CM_V2, methodWatcher);

        query = "explain select * from t1 where c between 7 and 10";
        rowContainsQuery(4, query, "logicalProfile=[(1,4,7,10),(2,3.2,1,5)]", CM_V2, methodWatcher);
    }

    @Test
    public void testRangeSelectNotInclusive() throws Exception {
        String query = "explain select * from t1 where c > 7 and c <= 10";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 3         | 7   | 10  | 8,9,10        |
        // | 2                | 2.5       | 1   | 5   | 3,4,5         |
        rowContainsQuery(3, query, "logicalProfile=[(1,3,7,10),(2,2.5,1,5)]", CM_V2, methodWatcher);

        query = "explain select * from t1 where c >= 7 and c < 10";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 3         | 7   | 10  | 7,8,9         |
        // | 2                | 2.5       | 1   | 5   | 3,3,4         |
        rowContainsQuery(3, query, "logicalProfile=[(1,3,7,10),(2,2.5,1,5)]", CM_V2, methodWatcher);

        query = "explain select * from t1 where c > 7 and c < 10";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 2         | 7   | 10  | 8,9           |
        // | 2                | 1.8       | 1   | 5   | 3,4           |
        rowContainsQuery(3, query, "logicalProfile=[(1,2,7,10),(2,1.8,1,5)]", CM_V2, methodWatcher);
    }

    @Test
    public void testOutOfRangeSelect() throws Exception {
        String query = "explain select * from t1 where d = 15";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 1         | 1   | 10  | -             |
        // | 2                | 1         | 15  | 15  | -             |
        rowContainsQuery(3, query, "logicalProfile=[(1,1,1,10),(2,1,15,15)]", CM_V2, methodWatcher);
    }

    @Test
    public void testNotEqualSelect() throws Exception {
        String query = "explain select * from t1 where c <> 3";

        // | column position  | #distinct | min | max | actual values      |
        // | ---------------- | --------- | --- | --- | -------------      |
        // | 1                | 9         | 1   | 10  | 1,2,4,5,6,7,8,9,10 |
        // | 2                | 5         | 1   | 5   | 1,2,1,2,3,3,3,4,5  |
        rowContainsQuery(3, query, "logicalProfile=[(1,9,1,10),(2,5,1,5)]", CM_V2, methodWatcher);
    }

    @Test
    public void testSingleTableCorrelation() throws Exception {
        String query = "explain select * from t1 where c = d";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 2         | 1   | 10  | 1,2,3         |
        // | 2                | 1.8       | 1   | 5   | 1,2,3         |
        // Note: Current values are obtained using the default formula based on selectivity.
        // Formulae for this specific case should give:
        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 5         | 1   | 5   | 1,2,3         |
        // | 2                | 5         | 1   | 5   | 1,2,3         |
        rowContainsQuery(3, query, "logicalProfile=[(1,2,1,10),(2,1.8,1,5)]", CM_V2, methodWatcher);
    }

    @Test
    public void testGroupBy() throws Exception {
        String query = "explain select count(*), d from t1 where d >= 3 group by d";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | -         | -   | -   | 4,1,1         |
        // | 2                | 4         | 3   | 5   | 3,4,5         |
        // TODO: distinct count should be 3 if numeric calculation in logical profile select() is implemented
        rowContainsQuery(new int[]{3,4}, query, CM_V2, methodWatcher,
                         new String[]{"ProjectRestrict", "logicalProfile=[(2,4,3,5)]"},
                         new String[]{"GroupBy", "logicalProfile=[(2,4,3,5)]"});
    }

    @Test
    public void testDistinct() throws Exception {
        String query = "explain select distinct d from t1 where d >= 3";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 2                | 4         | 3   | 5   | 3,4,5         |
        rowContainsQuery(new int[]{3,4}, query, CM_V2, methodWatcher,
                         new String[]{"Distinct", "logicalProfile=[(2,4,3,5)]"},
                         new String[]{"TableScan", "logicalProfile=[(2,4.2,3,5)]"});
    }

    @Test
    public void testLimitOffset() throws Exception {
        String query = "explain select * from t1 where d = 3 order by c {limit 2}";

        // | column position  | #distinct | min | max | actual values |
        // | ---------------- | --------- | --- | --- | ------------- |
        // | 1                | 2         | 1   | 10  | 3,6           |
        // | 2                | 1         | 3   | 3   | 3,3           |
        rowContainsQuery(new int[]{3,4}, query, CM_V2, methodWatcher,
                         new String[]{"Limit", "logicalProfile=[(1,2,1,10),(2,1,3,3)]"},
                         new String[]{"OrderBy", "logicalProfile=[(1,2,1,10),(2,1,3,3)]"});
    }

    /**** joins ****/


}
