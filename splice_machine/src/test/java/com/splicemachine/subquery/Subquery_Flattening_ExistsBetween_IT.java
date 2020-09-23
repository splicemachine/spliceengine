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
                "select * from A where a1 between 1 and 10 and exists (select a1 from A ai where ai.a2 > 20)", ONE_SUBQUERY_NODE, "" +
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
                "select * from C where surname like 'S%' and exists (select surname from C where name = 'Jon')", ONE_SUBQUERY_NODE, "" +
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
                "select * from A where a1 > 0 and a1 <= 10 and exists (select a1 from A ai where ai.a2 > 20)", ONE_SUBQUERY_NODE, "" +
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
