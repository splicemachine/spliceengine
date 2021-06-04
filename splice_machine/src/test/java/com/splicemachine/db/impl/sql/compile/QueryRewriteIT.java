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
 * Test query rewrites
 */
@RunWith(Parameterized.class)
public class QueryRewriteIT extends SpliceUnitTest {
    
    private Boolean useSpark;
    private static boolean isMemPlatform = false;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = QueryRewriteIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        isMemPlatform = isMemPlatform();
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/QueryRewriteIT.sql", "");
    }

    public QueryRewriteIT(Boolean useSpark) {
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
    public void testUNION() throws Exception {

        String expected =
            "A2  | B2  | C2  |\n" +
            "------------------\n" +
            "  0  |  0  |  0  |\n" +
            "  1  | 10  | 100 |\n" +
            " 10  | 100 |1000 |\n" +
            " 11  | 110 |1100 |\n" +
            "  2  | 20  | 200 |\n" +
            "  3  | 30  | 300 |\n" +
            "  5  | 50  | 500 |\n" +
            "  6  | 60  | 600 |\n" +
            "  8  | 80  | 800 |\n" +
            "  9  | 90  | 900 |\n" +
            "NULL |NULL |NULL |";

        String query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union " +
                                "select * from t3", useSpark);

        List<String> containedStrings = Arrays.asList("Subquery", "Limit");
        List<String> notContainedStrings = Arrays.asList("Join");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        String secondSet =
            "1  |  2  |  3  |\n" +
            "------------------\n" +
            "  0  |  0  |  0  |\n" +
            "  1  |  1  |  1  |\n" +
            "  1  | 10  | 100 |\n" +
            " 10  | 100 |1000 |\n" +
            " 11  | 110 |1100 |\n" +
            "  2  | 20  | 200 |\n" +
            "  5  | 50  | 500 |\n" +
            "  8  | 80  | 800 |\n" +
            "NULL |NULL |NULL |";
        List<String> containedStrings2 = Arrays.asList("Limit");
        query = format("select 1,1,1 from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                " union " +
                                "select * from t3", useSpark);
        testQuery(query, secondSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings2, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union " +
                                "select 1,2,3 from t3", useSpark);

        expected =
            "1  |  2  |  3  |\n" +
            "------------------\n" +
            "  0  |  0  |  0  |\n" +
            "  1  | 10  | 100 |\n" +
            "  1  |  2  |  3  |\n" +
            " 11  | 110 |1100 |\n" +
            "  2  | 20  | 200 |\n" +
            "  3  | 30  | 300 |\n" +
            "  5  | 50  | 500 |\n" +
            "  6  | 60  | 600 |\n" +
            "  8  | 80  | 800 |\n" +
            "  9  | 90  | 900 |\n" +
            "NULL |NULL |NULL |";

        testQuery(query, expected, methodWatcher);
        /* The explain should contain 2 limit clauses and 1 subquery:
            Plan
            ----
            Cursor(n=16,rows=1024,updateMode=READ_ONLY (1),engine=OLAP (cost))
              ->  ScrollInsensitive(n=16,totalCost=27.048,outputRows=1024,outputHeapSize=1.894 KB,partitions=1,parallelTasks=1)
                ->  Distinct(n=13,totalCost=5.315,outputRows=1024,outputHeapSize=1.894 KB,partitions=1,parallelTasks=1)
                  ->  Union(n=11,totalCost=5.315,outputRows=1946,outputHeapSize=3.6 KB,partitions=2,parallelTasks=1)
                    ->  Limit(n=10,totalCost=0.005,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                      ->  ProjectRestrict(n=9,totalCost=0.005,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                        ->  TableScan[T2(1904)](n=7,totalCost=0.005,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                    ->  ProjectRestrict(n=5,totalCost=5.315,outputRows=922,outputHeapSize=3.6 KB,partitions=1,parallelTasks=1,preds=[is not null(subq=4)])
                      ->  Subquery(n=4,totalCost=0.011,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,correlated=false,expression=true,invariant=true)
                        ->  Limit(n=4,totalCost=0.001,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                          ->  ProjectRestrict(n=3,totalCost=0.001,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                            ->  TableScan[T1(1888)](n=1,totalCost=0.001,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                      ->  TableScan[T2(1904)](n=0,totalCost=5.106,scannedRows=1024,outputRows=1024,outputHeapSize=3.6 KB,partitions=1,parallelTasks=1)
         */
        rowContainsQuery(new int[]{5,9,10}, "explain " + query, methodWatcher,
                new String[] {"Limit"},                                // 5
                new String[] {"Subquery"},                             // 9
                new String[] {"Limit"}                                 // 10
                );

        query = format("select random() from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union " +
                                "select random() from t3", useSpark);

        containedStrings = null;
        notContainedStrings = Arrays.asList("Subquery", "Limit");
        // A nondeterministic function should disable this rewrite.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);


        query = format("select sum(a2) from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union " +
                                "select max(a3) from t3", useSpark);
        // Aggregate functions should disable this rewrite.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select sum(a2) over (Partition by b2 ORDER BY c2 rows between unbounded preceding and unbounded following) from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union " +
                                "select AVG(a3) OVER(PARTITION BY b3) from t3", useSpark);
        // Window functions should disable this rewrite.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b union all " +
                                "select * from t3", useSpark);
        // The rewrite does not apply to UNION ALL.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);


        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 b, t3 c, t1 d union " +
                                "select 1,2,3 from t3 union " +
                                 "select 4,5,6 from t1"
                                , useSpark);
        expected =
            "1  |  2  |  3  |\n" +
            "------------------\n" +
            "  0  |  0  |  0  |\n" +
            "  1  | 10  | 100 |\n" +
            "  1  |  2  |  3  |\n" +
            " 11  | 110 |1100 |\n" +
            "  2  | 20  | 200 |\n" +
            "  3  | 30  | 300 |\n" +
            "  4  |  5  |  6  |\n" +
            "  5  | 50  | 500 |\n" +
            "  6  | 60  | 600 |\n" +
            "  8  | 80  | 800 |\n" +
            "  9  | 90  | 900 |\n" +
            "NULL |NULL |NULL |";
        testQuery(query, expected, methodWatcher);
        /* Every TableScan but one should have a Limit clause:
            Plan
            ----
            Cursor(n=29,rows=57,updateMode=READ_ONLY (1),engine=OLTP (default))
              ->  ScrollInsensitive(n=29,totalCost=33.098,outputRows=57,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                ->  Distinct(n=28,totalCost=24.698,outputRows=57,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                  ->  Union(n=26,totalCost=24.698,outputRows=57,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                    ->  Limit(n=25,totalCost=4.04,outputRows=20,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                      ->  ProjectRestrict(n=24,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                        ->  TableScan[T1(2352)](n=22,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                    ->  Distinct(n=21,totalCost=12.258,outputRows=37,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                      ->  Union(n=19,totalCost=12.258,outputRows=37,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                        ->  Limit(n=18,totalCost=4.04,outputRows=20,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                          ->  ProjectRestrict(n=17,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                            ->  TableScan[T3(2384)](n=15,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                        ->  ProjectRestrict(n=13,totalCost=4.052,outputRows=17,outputHeapSize=49 B,partitions=1,parallelTasks=1,preds=[is not null(subq=4),is not null(subq=8),is not null(subq=12)])
                          ->  Subquery(n=4,totalCost=0.422,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,correlated=false,expression=true,invariant=true)
                            ->  Limit(n=4,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                              ->  ProjectRestrict(n=3,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                                ->  TableScan[T1(2352)](n=1,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                          ->  Subquery(n=8,totalCost=0.422,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,correlated=false,expression=true,invariant=true)
                            ->  Limit(n=8,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                              ->  ProjectRestrict(n=7,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                                ->  TableScan[T3(2384)](n=5,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                          ->  Subquery(n=12,totalCost=0.422,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,correlated=false,expression=true,invariant=true)
                            ->  Limit(n=12,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1,fetchFirst=1)
                              ->  ProjectRestrict(n=11,totalCost=0.212,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                                ->  TableScan[T1(2352)](n=9,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1,parallelTasks=1)
                          ->  TableScan[T2(2368)](n=0,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=49 B,partitions=1,parallelTasks=1)
         */
        rowContainsQuery(new int[]{5,10,15,19,23}, "explain " + query, methodWatcher,
                new String[] {"Limit"},                                // 5
                new String[] {"Limit"},                                // 10
                new String[] {"Limit"},                                // 15
                new String[] {"Limit"},                                // 19
                new String[] {"Limit"}                                 // 23
                );
    }

    @Test
    public void testDerivedTable() throws Exception {

        String expected =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 10  | 100 |1000 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  3  | 30  | 300 |\n" +
                "  5  | 50  | 500 |\n" +
                "  6  | 60  | 600 |\n" +
                "  8  | 80  | 800 |\n" +
                "  9  | 90  | 900 |\n" +
                "NULL |NULL |NULL |";

        String secondSet =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 10  | 100 |1000 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  5  | 50  | 500 |\n" +
                "  8  | 80  | 800 |\n" +
                "NULL |NULL |NULL |";

        String thirdSet =
                "1  |  2  |  3  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  |  1  |  1  |\n" +
                "  1  | 10  | 100 |\n" +
                " 10  | 100 |1000 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  5  | 50  | 500 |\n" +
                "  8  | 80  | 800 |\n" +
                "NULL |NULL |NULL |";

        String query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1) b union " +
                              "select * from t3", useSpark);

        List<String> containedStrings = Arrays.asList("Subquery", "Limit");
        List<String> notContainedStrings = Arrays.asList("Join");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1 where false) b union " +
                              "select * from t3", useSpark);
        testQuery(query, secondSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1) b, (select * from t1) c union " +
                              "select * from t3", useSpark);
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1 a, t2 b) b union " +
                              "select * from t3", useSpark);
        notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1 a, t2 b) b union " +
                              "select * from t3", useSpark);

        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select 1,1,1 from (select * from t2) b--SPLICE-PROPERTIES useSpark=%s\n" +
                              " union " +
                              "select * from t3", useSpark);

        containedStrings = Arrays.asList("Limit");
        notContainedStrings = Arrays.asList("Join");
        testQuery(query, thirdSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        // Queries which should not use the rewrite.
        containedStrings = null;
        notContainedStrings = Arrays.asList("Subquery", "Limit");
        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", (select * from t1) b where b.a1 in (select a1 from t1) union " +
                              "select * from t3", useSpark);
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select b.* from (select * from t2) b--SPLICE-PROPERTIES useSpark=%s\n" +
                              " union " +
                              "select * from t3", useSpark);

        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }

    @Test
    public void testOuterJoin() throws Exception {

        String expected =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 10  | 100 |1000 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  3  | 30  | 300 |\n" +
                "  5  | 50  | 500 |\n" +
                "  6  | 60  | 600 |\n" +
                "  8  | 80  | 800 |\n" +
                "  9  | 90  | 900 |\n" +
                "NULL |NULL |NULL |";

        String secondSet =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 10  | 100 |1000 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  5  | 50  | 500 |\n" +
                "  8  | 80  | 800 |\n" +
                "NULL |NULL |NULL |";

        String thirdSet =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "NULL |NULL |NULL |";

        String fourthSet =
                "A2  | B2  | C2  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  5  | 50  | 500 |\n" +
                "NULL |NULL |NULL |";

        String query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              "left outer join t3 b on a2 = a3 and b2 = b3 \n" +
                              ", (select * from t1) b union " +
                              "select * from t3", useSpark);

        List<String> containedStrings = Arrays.asList("Subquery", "Limit");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              "right outer join t3 b on a2 = a3 and b2 = b3 \n" +
                              ", (select * from t1) b union " +
                              "select * from t3", useSpark);
        testQuery(query, secondSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from (select * from t1) b, t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              "right outer join t3 b on a2 = a3 and b2 = b3 \n" +
                              " union " +
                              "select * from t3", useSpark);
        testQuery(query, secondSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              "right outer join t3 b on a2 = a3 and b2 = b3 \n" +
                              "right outer join t1 c on false \n" +
                              ", (select * from t1) b union " +
                              "select * from t3 where false", useSpark);
        testQuery(query, thirdSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              "right outer join t3 b on a2 = a3 and b2 = b3 \n" +
                              "right outer join t1 c on a1 = a3 and b1 = b3 \n" +
                              ", (select * from t1) b union " +
                              "select * from t3 where false", useSpark);
        testQuery(query, fourthSet, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }

    @Test
    public void testINTERSECT() throws Exception {

        String expected =
                "1  |  2  |  3  |\n" +
                "------------------\n" +
                "  0  |  0  |  0  |\n" +
                "  1  | 10  | 100 |\n" +
                " 11  | 110 |1100 |\n" +
                "  2  | 20  | 200 |\n" +
                "  5  | 50  | 500 |\n" +
                "  8  | 80  | 800 |\n" +
                "NULL |NULL |NULL |";

        String query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", t1 b intersect " +
                              "select * from t3", useSpark);

        List<String> containedStrings = Arrays.asList("Subquery", "Limit");
        List<String> notContainedStrings = Arrays.asList("Join");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }

    @Test
    public void testEXCEPT() throws Exception {

        String expected =
                "1 | 2 | 3  |\n" +
                "-------------\n" +
                " 3 |30 |300 |\n" +
                " 6 |60 |600 |\n" +
                " 9 |90 |900 |";

        String query = format("select a.* from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", t1 b except " +
                              "select * from t3", useSpark);

        List<String> containedStrings = Arrays.asList("Subquery", "Limit");
        List<String> notContainedStrings = Arrays.asList("Join");
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }
}
