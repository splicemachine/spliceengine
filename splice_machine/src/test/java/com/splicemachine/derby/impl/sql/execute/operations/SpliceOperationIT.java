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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 8/1/20.
 */
@RunWith(Parameterized.class)
public class SpliceOperationIT extends SpliceUnitTest {
    private static final String SCHEMA = SpliceOperationIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private String useSparkString;

    public SpliceOperationIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 numeric(14,4))")
                .withInsert("insert into t1 values(?,?)")
                .withRows(rows(row(1, 11.9900))).create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 numeric(14,4))")
                .withInsert("insert into t2 values(?,?)")
                .withRows(rows(row(1, 22.9900))).create();

        new TableCreator(connection)
                .withCreate("create table t3 (a3 int, b3 numeric(14,4))")
                .withInsert("insert into t3 values(?,?)")
                .withRows(rows(row(1, 33.9900))).create();
    }

    @Test
    public void testJoin() throws Exception {
        String sql = String.format(
                "select a1, X from (select a1, nvl(b1,0) as X from t1) dt, t2 --splice-properties joinStrategy=broadcast,useSpark=%s\n" +
                        "where a1=a2", this.useSparkString
        );

        String expected = "A1 |   X    |\n" +
                "-------------\n" +
                " 1 |11.9900 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testCrossJoin() throws Exception {
        String sql = String.format(
                "select a1, X from (select a1, nvl(b1,0) as X from t1) dt, t2 --splice-properties joinStrategy=cross,useSpark=%s"
                , this.useSparkString
        );

        String expected = "A1 |   X    |\n" +
                "-------------\n" +
                " 1 |11.9900 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void testUnion() throws Exception {
        String sql = String.format(
                "select a1, nvl(b1,0) from t1, t2 union all select a2, b2 from t2 --splice-properties useSpark=%s"
                , this.useSparkString
        );

        String expected = "A1 |   2    |\n" +
                "-------------\n" +
                " 1 |11.9900 |\n" +
                " 1 |22.9900 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

}
