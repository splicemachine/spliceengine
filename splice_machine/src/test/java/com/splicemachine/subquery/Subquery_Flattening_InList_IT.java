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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;

import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.*;

public class Subquery_Flattening_InList_IT extends SpliceUnitTest {

    private static final String SCHEMA = Subquery_Flattening_InList_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int)");
        classWatcher.executeUpdate("create table B(b1 int, b2 int)");
        classWatcher.executeUpdate("create table C(name varchar(20), surname varchar(20))");
        classWatcher.executeUpdate("insert into A values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        classWatcher.executeUpdate("insert into B values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(NULL,NULL)");
        classWatcher.executeUpdate("insert into C values('Jon', 'Snow'),('Eddard', 'Stark'),('Robb', 'Stark'), ('Jon', 'Arryn')");
    }

    @Test
    public void simple() throws Exception {
        // subquery reads different table
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select b1 from B where b2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
        // subquery reads same table
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 in (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testBetweenFlattenAndNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 0 and 10 and a1 in (select a1 from A ai where ai.a2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testBetweenNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where a1 between 0 and 4 or a1 in (select a1 from A ai where ai.a2 > 20)", 1, "" +
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
                "select * from C where surname like 'S%' and surname in (select surname from C where name = 'Jon')", ZERO_SUBQUERY_NODES, "" +
                        "NAME | SURNAME |\n" +
                        "----------------\n" +
                        " Jon |  Snow   |"
        );
    }

    @Test
    public void testLikeNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'A%' or surname in (select surname from C where name = 'Robb')", 1, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Arryn  |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    public void testMultiColumnSimpleFlatten() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where (a1, a2) in (select * from B where b2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testMultiColumnLikeFlattenAndNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'S%' and (surname, name) in (select surname, name from C where name = 'Jon')", ZERO_SUBQUERY_NODES, "" +
                        "NAME | SURNAME |\n" +
                        "----------------\n" +
                        " Jon |  Snow   |"
        );
    }

    @Test
    public void testMultiColumnLikeNotFlattenOrNode() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C where surname like 'A%' or (name, surname) in (select * from C where name = 'Robb')", ONE_SUBQUERY_NODE, "" +
                        "NAME | SURNAME |\n" +
                        "----------------\n" +
                        " Jon |  Arryn  |\n" +
                        "Robb |  Stark  |"
        );
    }

    @Test
    public void testMultiColumnLikeNotFlattenOrNodeCorrelated() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from C as O where 1+1=1 or (name, surname) in (select * from C as I --splice-properties joinStrategy=nestedloop\n where length(I.surname) = length(O.surname))", ONE_SUBQUERY_NODE, "" +
                        "NAME  | SURNAME |\n" +
                        "------------------\n" +
                        "Eddard |  Stark  |\n" +
                        "  Jon  |  Arryn  |\n" +
                        "  Jon  |  Snow   |\n" +
                        " Robb  |  Stark  |"
        );
    }

    @Test
    public void testMultiColumnNotInNotFlatten() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where (a1, a2) not in (select * from B where b2 <= 20)", ONE_SUBQUERY_NODE, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where (a1, a2) not in (select * from B where b2 <= 20 or b2 is null)", ONE_SUBQUERY_NODE, ""
        );
    }

    @Test
    public void testMultiColumnNotInFlatten() throws Exception {
        methodWatcher.executeUpdate("create table if not exists A1 (a1 int not null, a2 int not null)");
        methodWatcher.executeUpdate("create table if not exists B1 (b1 int not null, b2 int not null)");
        methodWatcher.executeUpdate("delete from A1");
        methodWatcher.executeUpdate("delete from B1");
        methodWatcher.executeUpdate("insert into A1 values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");
        methodWatcher.executeUpdate("insert into B1 values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50)");

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A1 where (a1, a2) not in (select * from B1 where b2 <= 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A1 where a1 between 4 and 10 and (a1, a2) not in (select * from B1 where b2 <= 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );
    }

    @Test
    public void testMultiColumnNotInWithNulls() throws Exception {
        methodWatcher.executeUpdate("create table if not exists AWN (a1 int not null, a2 int)");
        methodWatcher.executeUpdate("create table if not exists BWN (b1 int, b2 int not null)");
        methodWatcher.executeUpdate("delete from AWN");
        methodWatcher.executeUpdate("delete from BWN");
        methodWatcher.executeUpdate("insert into AWN values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50),(5,NULL)");
        methodWatcher.executeUpdate("insert into BWN values(0,0),(0,0),(1,10),(1,10),(2,20),(2,20),(3,30),(3,30),(4,40),(4,40),(5,50),(5,50),(NULL,50)");

        String expected = "A1 |A2 |\n" +
                "--------\n" +
                " 3 |30 |\n" +
                " 4 |40 |\n" +
                " 5 |50 |";
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from AWN where (a1, a2) not in (select b1, b2 from BWN where b2 <= 20)", ONE_SUBQUERY_NODE, expected);
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from AWN where (a2, a1) not in (select b2, b1 from BWN where b2 <= 20)", ONE_SUBQUERY_NODE, expected);

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from AWN where (a1, a2) not in (select b1, b2 from BWN where b2 <= 20 or b1 is null)", ONE_SUBQUERY_NODE, "");

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from AWN where (a2, a1) not in (select b2, b1 from BWN where b2 <= 20 or b1 is null)", ONE_SUBQUERY_NODE, "");
    }

    @Test
    public void testMultiColumnTupleSemantic() throws Exception {
        methodWatcher.executeUpdate("create table if not exists ATS (a1 int, a2 int)");
        methodWatcher.executeUpdate("create table if not exists BTS (b1 int, b2 int)");
        methodWatcher.executeUpdate("delete from ATS");
        methodWatcher.executeUpdate("delete from BTS");
        methodWatcher.executeUpdate("insert into ATS values(0,0),(0,10),(1,10),(1,20),(2,20),(2,30),(3,30),(3,40),(4,40),(4,50),(5,50)");
        methodWatcher.executeUpdate("insert into BTS values(0,0),(1,10),(2,20),(3,30),(4,40),(5,50)");

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from ATS where (a1, a2) in (select * from BTS where b2 <= 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 | 0 |\n" +
                        " 1 |10 |\n" +
                        " 2 |20 |"
        );

        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from ATS where (a1, a2) not in (select * from BTS)", ONE_SUBQUERY_NODE, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 0 |10 |\n" +
                        " 1 |20 |\n" +
                        " 2 |30 |\n" +
                        " 3 |40 |\n" +
                        " 4 |50 |"
        );
    }

    @Test
    public void degeneratedLeftTuple() throws Exception {
        assertUnorderedResult(methodWatcher.getOrCreateConnection(),
                "select * from A where (a1) in (select b1 from B where b2 > 20)", ZERO_SUBQUERY_NODES, "" +
                        "A1 |A2 |\n" +
                        "--------\n" +
                        " 3 |30 |\n" +
                        " 4 |40 |\n" +
                        " 5 |50 |"
        );

        try {
            methodWatcher.executeQuery("select * from A where (a1) in (select b1, b2 from B where b2 > 20)");
            Assert.fail("expect exception of number of columns mismatch");
        } catch (SQLException e) {
            Assert.assertEquals("42X58", e.getSQLState());
        }
    }

    @Test
    public void numberOfColumnsMismatch() throws Exception {
        try {
            methodWatcher.executeQuery("select * from A where a1 in (select b1, b2 from B where b2 > 20)");
            Assert.fail("expect exception of number of columns mismatch");
        } catch (SQLException e) {
            Assert.assertEquals("42X58", e.getSQLState());
        }

        try {
            methodWatcher.executeQuery("select * from A where (a1, a2) in (select b1 from B where b2 > 20)");
            Assert.fail("expect exception of number of columns mismatch");
        } catch (SQLException e) {
            Assert.assertEquals("42X58", e.getSQLState());
        }
    }
}
