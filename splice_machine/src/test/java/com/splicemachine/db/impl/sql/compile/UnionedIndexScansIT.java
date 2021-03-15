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
 * Test Unioned Index Scans Access Path
 */
@RunWith(Parameterized.class)
public class UnionedIndexScansIT  extends SpliceUnitTest {
    
    private Boolean useSpark;
    private static boolean isMemPlatform = false;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    private static final String SCHEMA = UnionedIndexScansIT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        isMemPlatform = isMemPlatform();
        TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/UnionedIndexScansTestTables.sql", "");
    }

    @AfterClass
    public static void cleanup() throws Exception {
        try {
            TestUtils.executeSqlFile(classWatcher.getOrCreateConnection(), "subquery/UnionedIndexScansTestCleanup.sql", "");
        }
        catch (Exception e) {

        }
    }

    public UnionedIndexScansIT(Boolean useSpark) throws Exception {
        this.useSpark = useSpark;
        try (ResultSet rs = classWatcher.executeQuery("call syscs_util.syscs_set_global_database_property('splice.optimizer.favorUnionedIndexScans', 'true')")) {
        }
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
        String expected =
            "A | B | C |\n" +
            "------------\n" +
            " 1 | 1 | 1 |\n" +
            " 1 | 2 | 1 |\n" +
            " 1 | 3 | 1 |\n" +
            " 1 | 4 | 1 |\n" +
            " 2 | 1 | 2 |\n" +
            " 2 | 2 | 2 |\n" +
            " 2 | 3 | 2 |\n" +
            " 2 | 4 | 2 |\n" +
            " 3 | 1 | 3 |\n" +
            " 3 | 2 | 3 |\n" +
            " 3 | 3 | 3 |\n" +
            " 3 | 4 | 3 |";

        String query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where (a = 1 or a = 2 or c = 3)", useSpark);

        List<String> containedStrings = Arrays.asList("BASEROWID");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

    }

    @Test
    public void testManyORedPredicates() throws Exception {
        String expected =
            "A | B | C |\n" +
            "------------\n" +
            " 1 | 1 | 1 |\n" +
            " 1 | 2 | 1 |\n" +
            " 1 | 3 | 1 |\n" +
            " 1 | 4 | 1 |\n" +
            " 3 | 1 | 3 |\n" +
            " 3 | 2 | 3 |\n" +
            " 3 | 3 | 3 |\n" +
            " 3 | 4 | 3 |";

        String query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where (a = 1 and c = 1) or (a = 2 and c = 1) or (a = 3 and c = 3) or (a = 5 and c = 4)", useSpark);

        List<String> containedStrings = Arrays.asList("BASEROWID");
        List<String> notContainedStrings = null;
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where (a = 1 or c = 1) and (a = 2 or c = 2) and (a = 3 or c = 3) and (a = 4 and c = 4)", useSpark);
        expected =
            "";
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where (a > 1 or c > 1) and (a = 2 or c = 2 or c =3)", useSpark);
        expected =
            "A | B | C |\n" +
            "------------\n" +
            " 2 | 1 | 2 |\n" +
            " 2 | 2 | 2 |\n" +
            " 2 | 3 | 2 |\n" +
            " 2 | 4 | 2 |\n" +
            " 3 | 1 | 3 |\n" +
            " 3 | 2 | 3 |\n" +
            " 3 | 3 | 3 |\n" +
            " 3 | 4 | 3 |";
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
    }

    @Test
    public void testJoins() throws Exception {
        String query = format("select count(*) from t2 --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 where (t2.b = 1 or t2.a = 2 or t2.a = 3) and (t2.a = t1.a or t2.a = t1.c)", useSpark);

        String expected =
            "1 |\n" +
            "----\n" +
            "40 |";
        List<String> containedStrings = Arrays.asList("BASEROWID");
        List<String> notContainedStrings = null;
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);
        testQuery(query, expected, methodWatcher);


        query = format("select count(*) from t2 --SPLICE-PROPERTIES useSpark=%s\n" +
                                ", t1 where (t2.a = 1 or t2.a = 2 or t2.a = 3) and (t2.a = t1.a or t2.a = t1.c)", useSpark);

        expected =
            "1 |\n" +
            "----\n" +
            "48 |";
        notContainedStrings = Arrays.asList("BASEROWID");
        containedStrings = null;
        testQuery(query, expected, methodWatcher);
        // This query cannot currently use UIS access path.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ",t2, t1 where (t2.a = 1 or t2.a = 2 or t2.a = 3) and (t2.a = t1.a or t2.a = t1.c)", useSpark);
        expected =
            "1  |\n" +
            "-----\n" +
            "768 |";
        testQuery(query, expected, methodWatcher);
        // This query cannot currently use UIS access path.
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        query = format("select count(*) from t2 a --SPLICE-PROPERTIES useSpark=%s\n" +
                                ",t2, t1 where (t2.b = 1 or t2.a = 2 or t2.a = 3) and (t2.a = t1.a or t2.a = t1.c)", useSpark);
        expected =
            "1  |\n" +
            "-----\n" +
            "640 |";
        testQuery(query, expected, methodWatcher);
        containedStrings = Arrays.asList("BASEROWID");
        notContainedStrings = null;
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

        // A simplified customer-like query.
        query = format("select * from t1 BH --SPLICE-PROPERTIES useSpark=%s\n" +
                              ", t1 BU where\n" +
                              "( BH.a = 1 AND BH.b = BU.a )\n" +
                              "          OR ( BH.a = BU.a\n" +
                              "               AND BH.b = 1 )", useSpark);
        // The explain should look something like:
        // Plan
        //----
        //Cursor(n=75,rows=36864,updateMode=READ_ONLY (1),engine=OLTP (session hint))
        //  ->  ScrollInsensitive(n=75,totalCost=0,outputRows=36864,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //    ->  ProjectRestrict(n=74,totalCost=0,outputRows=36864,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //      ->  NestedLoopJoin(n=72,totalCost=0,outputRows=36864,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //        ->  ProjectRestrict(n=71,totalCost=4.036,outputRows=32,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1,preds=[((BH.B[30:2] = BU.A[68:1]) or ((BH.B[30:2] = 1) or false)),((BH.A[30:1] = 1) or ((BH.A[30:1] = BU.A[68:1]) or false)),(dnfPathDT_###_BU.BASEROWID2[68:4] = BH.BASEROWID[30:4])])
        //          ->  ProjectRestrict(n=68,totalCost=15481.567,outputRows=36864,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //            ->  NestedLoopJoin(n=66,totalCost=15481.567,outputRows=36864,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //              ->  ProjectRestrict(n=65,totalCost=4.036,outputRows=32,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1)
        //                ->  TableScan[T1(4272)](n=64,totalCost=4.036,scannedRows=32,outputRows=32,outputHeapSize=1.453 MB,partitions=1,parallelTasks=1,keys=[(dnfPathDT_###_BU.BASEROWID[63:1] = BU.BASEROWID[65:4])])
        //              ->  Distinct(n=62,totalCost=1158.935,outputRows=1152,outputHeapSize=28.5 KB,partitions=1,parallelTasks=1)
        //                ->  Union(n=60,totalCost=1158.935,outputRows=1152,outputHeapSize=28.5 KB,partitions=2,parallelTasks=1)
        //                  ->  ProjectRestrict(n=59,totalCost=1158.935,outputRows=576,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                    ->  NestedLoopJoin(n=57,totalCost=1158.935,outputRows=576,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                      ->  ProjectRestrict(n=56,totalCost=4.004,outputRows=4,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                        ->  IndexScan[IDX1(4305)](n=55,totalCost=4.004,scannedRows=4,outputRows=4,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1,baseTable=T1(4272),keys=[(BH.A[54:1] = BU.A[55:1])])
        //                      ->  ProjectRestrict(n=54,totalCost=4.013,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                        ->  ProjectRestrict(n=29,totalCost=150.726,outputRows=144,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                          ->  NestedLoopJoin(n=27,totalCost=150.726,outputRows=144,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                            ->  ProjectRestrict(n=26,totalCost=4.013,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                              ->  TableScan[T1(4272)](n=25,totalCost=4.013,scannedRows=12,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1,keys=[(dnfPathDT_###_BH.BASEROWID[24:1] = BH.BASEROWID[26:3])])
        //                            ->  Distinct(n=23,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=1,parallelTasks=1)
        //                              ->  Union(n=21,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=2,parallelTasks=1)
        //                                ->  ProjectRestrict(n=20,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                  ->  ProjectRestrict(n=19,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                    ->  TableScan[T1(4272)](n=18,totalCost=4.009,scannedRows=8,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1,keys=[(BH.B[18:1] = 1)])
        //                                ->  ProjectRestrict(n=17,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                  ->  ProjectRestrict(n=16,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                    ->  IndexScan[IDX1(4305)](n=15,totalCost=4.004,scannedRows=4,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1,baseTable=T1(4272),keys=[(BH.A[15:1] = 1)])
        //                  ->  ProjectRestrict(n=52,totalCost=1158.935,outputRows=576,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                    ->  NestedLoopJoin(n=50,totalCost=1158.935,outputRows=576,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                      ->  ProjectRestrict(n=49,totalCost=4.004,outputRows=4,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1)
        //                        ->  IndexScan[IDX1(4305)](n=48,totalCost=4.004,scannedRows=4,outputRows=4,outputHeapSize=14.25 KB,partitions=1,parallelTasks=1,baseTable=T1(4272),keys=[(BH.B[47:2] = BU.A[48:1])])
        //                      ->  ProjectRestrict(n=47,totalCost=4.013,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                        ->  ProjectRestrict(n=14,totalCost=150.726,outputRows=144,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                          ->  NestedLoopJoin(n=12,totalCost=150.726,outputRows=144,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                            ->  ProjectRestrict(n=11,totalCost=4.013,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1)
        //                              ->  TableScan[T1(4272)](n=10,totalCost=4.013,scannedRows=12,outputRows=12,outputHeapSize=2.438 KB,partitions=1,parallelTasks=1,keys=[(dnfPathDT_###_BH.BASEROWID[9:1] = BH.BASEROWID[11:3])])
        //                            ->  Distinct(n=8,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=1,parallelTasks=1)
        //                              ->  Union(n=6,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=2,parallelTasks=1)
        //                                ->  ProjectRestrict(n=5,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                  ->  ProjectRestrict(n=4,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                    ->  TableScan[T1(4272)](n=3,totalCost=4.009,scannedRows=8,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1,keys=[(BH.B[3:1] = 1)])
        //                                ->  ProjectRestrict(n=2,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                  ->  ProjectRestrict(n=1,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                                    ->  IndexScan[IDX1(4305)](n=0,totalCost=4.004,scannedRows=4,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1,baseTable=T1(4272),keys=[(BH.A[0:1] = 1)])
        //        ->  ProjectRestrict(n=30,totalCost=4.013,outputRows=12,outputHeapSize=3 KB,partitions=1,parallelTasks=1)
        //          ->  ProjectRestrict(n=45,totalCost=150.726,outputRows=144,outputHeapSize=3 KB,partitions=1,parallelTasks=1)
        //            ->  NestedLoopJoin(n=43,totalCost=150.726,outputRows=144,outputHeapSize=3 KB,partitions=1,parallelTasks=1)
        //              ->  ProjectRestrict(n=42,totalCost=4.013,outputRows=12,outputHeapSize=3 KB,partitions=1,parallelTasks=1)
        //                ->  TableScan[T1(4272)](n=41,totalCost=4.013,scannedRows=12,outputRows=12,outputHeapSize=3 KB,partitions=1,parallelTasks=1,keys=[(dnfPathDT_###_BH.BASEROWID[40:1] = BH.BASEROWID[42:4])])
        //              ->  Distinct(n=39,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=1,parallelTasks=1)
        //                ->  Union(n=37,totalCost=4.009,outputRows=12,outputHeapSize=64 B,partitions=2,parallelTasks=1)
        //                  ->  ProjectRestrict(n=36,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                    ->  ProjectRestrict(n=35,totalCost=4.009,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                      ->  TableScan[T1(4272)](n=34,totalCost=4.009,scannedRows=8,outputRows=8,outputHeapSize=32 B,partitions=1,parallelTasks=1,keys=[(BH.B[34:1] = 1)])
        //                  ->  ProjectRestrict(n=33,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                    ->  ProjectRestrict(n=32,totalCost=4.004,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1)
        //                      ->  IndexScan[IDX1(4305)](n=31,totalCost=4.004,scannedRows=4,outputRows=4,outputHeapSize=32 B,partitions=1,parallelTasks=1,baseTable=T1(4272),keys=[(BH.A[31:1] = 1)])
        expected =
            "A | B | C | A | B | C |\n" +
            "------------------------\n" +
            " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
            " 1 | 1 | 1 | 1 | 2 | 1 |\n" +
            " 1 | 1 | 1 | 1 | 3 | 1 |\n" +
            " 1 | 1 | 1 | 1 | 4 | 1 |\n" +
            " 1 | 2 | 1 | 2 | 1 | 2 |\n" +
            " 1 | 2 | 1 | 2 | 2 | 2 |\n" +
            " 1 | 2 | 1 | 2 | 3 | 2 |\n" +
            " 1 | 2 | 1 | 2 | 4 | 2 |\n" +
            " 1 | 3 | 1 | 3 | 1 | 3 |\n" +
            " 1 | 3 | 1 | 3 | 2 | 3 |\n" +
            " 1 | 3 | 1 | 3 | 3 | 3 |\n" +
            " 1 | 3 | 1 | 3 | 4 | 3 |\n" +
            " 1 | 4 | 1 | 4 | 1 | 4 |\n" +
            " 1 | 4 | 1 | 4 | 2 | 4 |\n" +
            " 1 | 4 | 1 | 4 | 3 | 4 |\n" +
            " 1 | 4 | 1 | 4 | 4 | 4 |\n" +
            " 2 | 1 | 2 | 2 | 1 | 2 |\n" +
            " 2 | 1 | 2 | 2 | 2 | 2 |\n" +
            " 2 | 1 | 2 | 2 | 3 | 2 |\n" +
            " 2 | 1 | 2 | 2 | 4 | 2 |\n" +
            " 3 | 1 | 3 | 3 | 1 | 3 |\n" +
            " 3 | 1 | 3 | 3 | 2 | 3 |\n" +
            " 3 | 1 | 3 | 3 | 3 | 3 |\n" +
            " 3 | 1 | 3 | 3 | 4 | 3 |\n" +
            " 4 | 1 | 4 | 4 | 1 | 4 |\n" +
            " 4 | 1 | 4 | 4 | 2 | 4 |\n" +
            " 4 | 1 | 4 | 4 | 3 | 4 |\n" +
            " 4 | 1 | 4 | 4 | 4 | 4 |\n" +
            " 5 | 1 | 5 | 5 | 1 | 5 |\n" +
            " 5 | 1 | 5 | 5 | 2 | 5 |\n" +
            " 5 | 1 | 5 | 5 | 3 | 5 |\n" +
            " 5 | 1 | 5 | 5 | 4 | 5 |\n" +
            " 6 | 1 | 6 | 6 | 1 | 6 |\n" +
            " 6 | 1 | 6 | 6 | 2 | 6 |\n" +
            " 6 | 1 | 6 | 6 | 3 | 6 |\n" +
            " 6 | 1 | 6 | 6 | 4 | 6 |\n" +
            " 7 | 1 | 7 | 7 | 1 | 7 |\n" +
            " 7 | 1 | 7 | 7 | 2 | 7 |\n" +
            " 7 | 1 | 7 | 7 | 3 | 7 |\n" +
            " 7 | 1 | 7 | 7 | 4 | 7 |\n" +
            " 8 | 1 | 8 | 8 | 1 | 8 |\n" +
            " 8 | 1 | 8 | 8 | 2 | 8 |\n" +
            " 8 | 1 | 8 | 8 | 3 | 8 |\n" +
            " 8 | 1 | 8 | 8 | 4 | 8 |";
        testQuery(query, expected, methodWatcher);
        testExplainContains(query, methodWatcher, containedStrings, notContainedStrings);

    }


    @Test
    public void testParameterized() throws Exception {
        String expected =
            "A | B | C |\n" +
            "------------\n" +
            " 1 | 1 | 1 |\n" +
            " 2 | 1 | 2 |\n" +
            " 3 | 1 | 3 |\n" +
            " 4 | 1 | 4 |\n" +
            " 1 | 2 | 1 |\n" +
            " 2 | 2 | 2 |\n" +
            " 3 | 2 | 3 |\n" +
            " 4 | 2 | 4 |\n" +
            " 1 | 3 | 1 |\n" +
            " 2 | 3 | 2 |\n" +
            " 3 | 3 | 3 |\n" +
            " 4 | 3 | 4 |\n" +
            " 1 | 4 | 1 |\n" +
            " 2 | 4 | 2 |\n" +
            " 3 | 4 | 3 |\n" +
            " 4 | 4 | 4 |";
        String query = format("select * from t1 --SPLICE-PROPERTIES useSpark=%s\n" +
                                "where\n" +
                                "a  = ? or\n" +
                                "a  = ? or\n" +
                                "c  = ? or\n" +
                                "c  = ?\n", useSpark);

        List<String> containedStrings = Arrays.asList("BASEROWID");
        List<String> notContainedStrings = null;
        List<Integer> paramList = Arrays.asList(1,2,3,4);
        testPreparedQuery(query, methodWatcher, expected, paramList);
        testParameterizedExplainContains(query, methodWatcher, containedStrings, notContainedStrings, paramList);

    }

    @Test
    public void testIllegalCreate() throws Exception {

        String query = "create table t3 (baserowid int, a int)";
        List<String> expectedErrors =
        Arrays.asList("'BASEROWID' is a special derived column which may not be defined in DDL statements.  Please rename the column.");

        testUpdateFail(query, expectedErrors, methodWatcher);

        query = "create table t3 (rowid int, a int)";
        expectedErrors =
        Arrays.asList("'ROWID' is a special derived column which may not be defined in DDL statements.  Please rename the column.");
        testUpdateFail(query, expectedErrors, methodWatcher);
    }

    @Test
    public void testIllegalRename() throws Exception {

        String query = "create table t3 (a int, b int)";
        methodWatcher.executeUpdate(query);
        List<String> expectedErrors =
        Arrays.asList("'BASEROWID' is a special derived column which may not be defined in DDL statements.  Please rename the column.");
        query = "RENAME COLUMN T3.a TO baserowid";
        testUpdateFail(query, expectedErrors, methodWatcher);

        query = "RENAME COLUMN T3.a TO rowid";
        expectedErrors =
        Arrays.asList("'ROWID' is a special derived column which may not be defined in DDL statements.  Please rename the column.");
        testUpdateFail(query, expectedErrors, methodWatcher);

        methodWatcher.executeUpdate("drop table t3");
    }
}
