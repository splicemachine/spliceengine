/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;


/**
 * Test index access with no predicate on the first index column.
 */
@RunWith(Parameterized.class)
public class IndexPrefixIterationIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    private static boolean isMemPlatform = false;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = IndexPrefixIterationIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        isMemPlatform = isMemPlatform();
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/IndexPrefixIterationTestTables.sql", "");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        try {
            TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/IndexPrefixIterationTestCleanup.sql", "");
        }
        catch (Exception e) {

        }
    }

    public IndexPrefixIterationIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    public static boolean
    isMemPlatform() throws Exception{
        try (ResultSet rs = classWatcher.executeQuery("CALL SYSCS_UTIL.SYSCS_IS_MEM_PLATFORM()")) {
            rs.next();
            return ((Boolean)rs.getObject(1));
        }
    }

    @Test
    public void testSingleTableScan() throws Exception {
        if (isMemPlatform && useSpark)
            return;
        String expected =
            "A1 |B1 |C1 |D1 |E1 |F1 |G1 |            H1             |\n" +
            "--------------------------------------------------------\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.000001 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.321111 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.999999 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |2018-12-31 15:59:59.321111 |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1969-12-31 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |  1969-12-31 15:59:59.001  |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1970-12-31 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1995-01-01 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1999-12-31 15:59:59.0   |";

        String query = format("select * from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "c1  = '1' and\n" +
                                "b1 = 1 and\n" +
                                "d1 in (1,-2,-3)\n", useSpark);

        List<String> containedStrings = Arrays.asList("MultiProbeTableScan", "IndexPrefixIteratorMode(129 values)");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "d1 in (1,-2,-3)\n", useSpark);

        expected =
            "1  |\n" +
            "-----\n" +
            "576 |";

        // Should pick T11_IX2.
        containedStrings = Arrays.asList("MultiProbeIndexScan", "T11_IX2");
        notContainedStrings = Arrays.asList("IndexPrefixIteratorMode");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "b1 < 0\n", useSpark);
        expected =
            "1 |\n" +
            "----\n" +
            " 1 |";

        containedStrings = Arrays.asList("TableScan", "IndexPrefixIteratorMode(129 values)");
        notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
        query = format("select count(*) from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "b1 = 1 and c1 < '2' \n", useSpark);
        expected =
            "1  |\n" +
            "-----\n" +
            "180 |";

        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        methodWatcher.execute("set session_property disablePredsForIndexOrPkAccessPath=true");
        containedStrings = Arrays.asList("TableScan");
        notContainedStrings = Arrays.asList("IndexPrefixIteratorMode");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

    }

    @Test
    public void testIndexOnExpressions() throws Exception {
        if (isMemPlatform && useSpark)
            return;
        String query = format("select count(*) from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "b1+1 = 2 \n", useSpark);

        String expected =
            "1  |\n" +
            "-----\n" +
            "576 |";
        // Should pick T11_IX3.
        List<String> containedStrings = Arrays.asList("IndexScan", "T11_IX3", "IndexPrefixIteratorMode(129 values)");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t11 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "b1+1 between 0 and 2 \n", useSpark);
        expected =
            "1  |\n" +
            "-----\n" +
            "577 |";
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

    }

    @Test
    public void testJoins() throws Exception {
        if (isMemPlatform && useSpark)
            return;
        String query = format("select count(*) from t11 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t11 b\n" +
                                "where\n" +
                                "a.c1  = '1' and\n" +
                                "a.b1 = 1 and\n" +
                                "a.d1 in (1,-2,-3) and\n" +
                                "a.d1 = b.d1", useSpark);

        String expected =
            "1  |\n" +
            "------\n" +
            "5184 |";
        List<String> containedStrings = Arrays.asList("MultiProbeTableScan", "IndexPrefixIteratorMode(129 values)");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t11 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t11 b\n" +
                                "where\n" +
                                "a.c1  = '1' and\n" +
                                "a.b1 = 1 and\n" +
                                "a.d1 in (1,-2,-3) and\n" +
                                "a.b1 = b.b1", useSpark);

        containedStrings = Arrays.asList("MultiProbeTableScan", "IndexPrefixIteratorMode(129 values)", "scannedRows=13", "scannedRows=576");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t11 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                "left outer join t11 b\n" +
                                "on\n" +
                                    "a.b1 = b.b1 and\n" +
                                    "a.c1 = b.c1 and\n" +
                                    "a.d1 = b.d1\n" +
                                "WHERE\n" +
                                    "a.c1  = '-1' and\n" +
                                    "a.e1 = -1 and\n" +
                                    "a.a1 = a.a1", useSpark);
        expected =
            "1 |\n" +
            "----\n" +
            " 1 |";

        containedStrings = Arrays.asList("IndexScan", "T11_IX4", "IndexPrefixIteratorMode(129 values)", "scannedRows=1");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }


    @Test
    public void testParameterized() throws Exception {
        if (isMemPlatform && useSpark)
            return;
        String expected =
            "A1 |B1 |C1 |D1 |E1 |F1 |G1 |            H1             |\n" +
            "--------------------------------------------------------\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.000001 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.321111 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |1969-12-31 15:59:59.999999 |\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 | 1 |2018-12-31 15:59:59.321111 |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1969-12-31 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |  1969-12-31 15:59:59.001  |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1970-12-31 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1995-01-01 15:59:59.0   |\n" +
            " 2 | 1 | 1 | 1 | 1 | 1 | 1 |   1999-12-31 15:59:59.0   |";
        String query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "c1  = ? and\n" +
                                "b1 = ? and\n" +
                                "d1 in (?,?,?)\n", useSpark);

        List<String> containedStrings = Arrays.asList("MultiProbeTableScan", "IndexPrefixIteratorMode(129 values)");
        List<String> notContainedStrings = null;
        List<Integer> paramList = Arrays.asList(1,1,1,-2,-3);
        testPreparedQuery(query, methodWatcher, expected, paramList);
        testParameterizedExplainContains(query, methodWatcher, containedStrings, notContainedStrings, paramList);

        containedStrings = Arrays.asList("T1_IX3", "IndexPrefixIteratorMode(129 values)");
        paramList = Arrays.asList(2);

        query = format("select count(*) from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                        "where\n" +
                        "b1+1 = ? \n", useSpark);
        expected =
            "1  |\n" +
            "-----\n" +
            "576 |";

        testPreparedQuery(query, methodWatcher, expected, paramList);
        testParameterizedExplainContains(query, methodWatcher, containedStrings, notContainedStrings, paramList);

    }


}
