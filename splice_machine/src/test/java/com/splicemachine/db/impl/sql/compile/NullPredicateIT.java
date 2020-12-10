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
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Test predicate with tuples
 */
@RunWith(Parameterized.class)
public class NullPredicateIT extends SpliceUnitTest {

    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = NullPredicateIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    public static void createData(Connection conn, String schemaName) throws Exception {
        new TableCreator(conn)
                .withCreate("create table t(a int, b varchar(5))")
                .withInsert("insert into t values(?,?)")
                .withRows(rows(
                        row(1, "aaa")))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    public NullPredicateIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @Test
    public void testSimpleNull() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as boolean)", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInArithmeticExpression() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as int) + 1 > 3", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInCharacterExpression() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as varchar(3)) || 'test' = 'bar_test'", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInLike() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as varchar(3)) like 'bar_%%'", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInBetween_1() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where a between cast(null as int) and 5", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInBetween_2() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as int) between 1 and 5", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInInList_1() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where a in (cast(null as int), 5)", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInInList_2() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                "where cast(null as int) in (3, 5)", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }

    @Test
    public void testNullInJoinCondition() throws Exception {
        String query = format("select count(*) from T --SPLICE-PROPERTIES useSpark=%s\n" +
                " inner join T on cast(null as boolean)", useSpark);

        String expected = "1 |\n" +
                "----\n" +
                " 0 |";

        try(ResultSet rs = methodWatcher.executeQuery(query)) {
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        }
    }
}
