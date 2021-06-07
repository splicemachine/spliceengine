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
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test constant folding
 */
@RunWith(Parameterized.class)
@Category(SerialTest.class)
public class ConstantFoldingIT extends SpliceUnitTest {

    private Boolean disableConstantFolding;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{false});
        params.add(new Object[]{true});
        return params;
    }

    @Parameterized.BeforeParam
    public static void beforeParam(boolean disableConstantFolding) throws Exception {
        classWatcher.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE()");
        classWatcher.execute("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
        classWatcher.execute(format("call syscs_util.syscs_set_global_database_property('splice.database.disableConstantFolding', '%s')", disableConstantFolding));
    }

    @Parameterized.AfterParam
    public static void afterParam() throws Exception {
        classWatcher.execute("CALL SYSCS_UTIL.SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE()");
        classWatcher.execute("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");
        classWatcher.execute("call syscs_util.syscs_set_global_database_property('splice.database.disableConstantFolding', null)");
    }

    private static final String SCHEMA = ConstantFoldingIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    public ConstantFoldingIT(Boolean disableConstantFolding) {
        this.disableConstantFolding = disableConstantFolding;
    }

    private void assertSkipTableScan(String query) throws Exception {
        testExplainContains(query, methodWatcher, Collections.singletonList("Values"), Collections.singletonList("TableScan"));
    }

    private void assertTableScan(String query) throws Exception {
        testExplainContains(query, methodWatcher, Collections.singletonList("TableScan"), null);
    }

    private String buildCountQuery(String predicate) {
        return format("select count(*) from sysibm.sysdummy1 where %s", predicate);
    }

    private void checkFalsePredicates(List<String> predicates) throws Exception {
        for (String p: predicates) {
            String q = buildCountQuery(p);
            checkIntExpression(q, 0, methodWatcher);
            if (!disableConstantFolding) {
                assertSkipTableScan(q);
            }
        }
    }

    private void checkTruePredicates(List<String> predicates) throws Exception {
        for (String p: predicates) {
            String q = buildCountQuery(p);
            checkIntExpression(q, 1, methodWatcher);
        }
    }

    @Test
    public void testNot() throws Exception {
        checkFalsePredicates(Arrays.asList(
                "(not true) = true",
                "(not false) = false",
                "(not false) = case when 1=0 then true end"
        ));
        checkTruePredicates(Arrays.asList(
                "not true = false",
                "not false = true"
        ));
    }

    @Test
    public void testConditional() throws Exception {
        checkFalsePredicates(Arrays.asList(
                "case when 1 = 0 then true else false end",
                "case when 1 = 1 then false else true end",
                "case when case when 1 = 0 then false end then true else true end"
        ));
        checkTruePredicates(Arrays.asList(
                "case when 1 = 1 then true else false end",
                "case when 1 = 0 then false else true end",
                "case when 1 = 1 then true end"
        ));
    }

    @Test
    public void testBinaryRelationOperator() throws Exception {
        checkFalsePredicates(Arrays.asList(
                "1 = 0",
                "1 < 0",
                "0 > 1",
                "1 <= 0",
                "0 >= 1",
                "1 <> 1",
                "1 = case when 1 = 0 then 1 end",
                "'abc' > 'def'",
                "true = false"

        ));
        checkTruePredicates(Arrays.asList(
                "1 <> 0",
                "1 > 0",
                "0 < 1",
                "1 >= 0",
                "0 <= 1",
                "1 = 1",
                "'abc' = 'abc'",
                "false = false"
        ));
    }

    @Test
    public void testUnaryMinusPlus() throws Exception {
        checkFalsePredicates(Arrays.asList(
                "1 = - (0)",
                "1 = + (0)"
        ));
        checkTruePredicates(Arrays.asList(
                "-1 = - (1)",
                "1 = + (1)"
        ));
    }
}



