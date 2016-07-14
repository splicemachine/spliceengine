/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import static com.splicemachine.subquery.SubqueryITUtil.*;

/**
 * Tests related to DB-3779.  At one time the existence of a between operator in a given select would prevent any exists
 * subqueries it contained from being flattened.
 */
public class Subquery_Flattening_ExistsBetween_IT {

    private static final String SCHEMA = Subquery_Flattening_ExistsBetween_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table C(name varchar(20), surname varchar(20))");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into C values('Jon', 'Snow'),('Eddard', 'Stark'),('Robb', 'Stark'), ('Jon', 'Arryn')");
    }

    @Test
    public void testBetweenFlattenAndNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 1 and 10 and exists (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testBetweenNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 1 and 4 or exists (select a1 from A ai where ai.a2 > 20)", 1, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testLikeFlattenAndNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'S%' and exists (select surname from C where name = 'Jon')", ZERO_SUBQUERY_NODES, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Snow   |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    public void testLikeNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'A%' or exists (select surname from C where name = 'Robb')", 1, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Arryn  |\n" +
                        "  Jon  |  Snow   |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    public void testSimplePredicateFlattenAndNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 > 0 and a1 <= 10 and exists (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testSimplePredicateNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 >= 1 and a1 <= 4 or exists (select a1 from A ai where ai.a2 > 20)", 1, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }
}
