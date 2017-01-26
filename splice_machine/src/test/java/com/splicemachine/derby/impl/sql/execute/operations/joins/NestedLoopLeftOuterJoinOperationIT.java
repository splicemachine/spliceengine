/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for OUTER nested loop joins.
 */
public class NestedLoopLeftOuterJoinOperationIT {

    private static final String SCHEMA = NestedLoopLeftOuterJoinOperationIT.class.getSimpleName().toUpperCase();
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
        classWatcher.executeUpdate("create table C(c1 int, c2 int)");
        classWatcher.executeUpdate("create table EMPTY_TABLE(e1 int, e2 int)");

        classWatcher.executeUpdate("insert into A values(null,null),(null,null),(1,10),(2,20),           (3,30),(3,30),                               (7,70)");
        classWatcher.executeUpdate("insert into B values(null,null),                   (2,20),(2,20),                  (4,40),(4,40)");
        classWatcher.executeUpdate("insert into C values                               (2,20),(2,20),(2,20),                     (5,50),(6,60),(6,60),(7,70)");
    }

    @Test
    public void simpleCaseTwoTables() throws Exception {
        // A left B
        ResultSet rs = methodWatcher.executeQuery("select * from A left join B --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=b1");
        assertEquals("" +
                "A1  | A2  | B1  | B2  |\n" +
                "------------------------\n" +
                "  1  | 10  |NULL |NULL |\n" +
                "  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |", toString(rs));

        // B left A
        rs = methodWatcher.executeQuery("select * from B left join A --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=b1");
        assertEquals("" +
                "B1  | B2  | A1  | A2  |\n" +
                "------------------------\n" +
                "  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |\n" +
                "  4  | 40  |NULL |NULL |\n" +
                "  4  | 40  |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |", toString(rs));

        // C left A
        rs = methodWatcher.executeQuery("select * from C left join A --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=c1");
        assertEquals("" +
                "C1 |C2 | A1  | A2  |\n" +
                "--------------------\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 2 |20 |  2  | 20  |\n" +
                " 5 |50 |NULL |NULL |\n" +
                " 6 |60 |NULL |NULL |\n" +
                " 6 |60 |NULL |NULL |\n" +
                " 7 |70 |  7  | 70  |", toString(rs));
    }

    @Test
    public void simpleCaseThreeTables() throws Exception {
        // A left B left C
        ResultSet rs = methodWatcher.executeQuery(
                "select * from A left join B --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=b1 left join C --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on c1=b1"
        );
        assertEquals("" +
                "A1  | A2  | B1  | B2  | C1  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  3  | 30  |NULL |NULL |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |", toString(rs));

        // A left B left C (same thing, but different join clause for C)
        rs = methodWatcher.executeQuery(
                "select * from A left join B --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=b1 left join C --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on c1=a1"
        );
        assertEquals("" +
                "A1  | A2  | B1  | B2  | C1  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  2  | 20  |  2  | 20  |  2  | 20  |\n" +
                "  3  | 30  |NULL |NULL |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |  7  | 70  |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |", toString(rs));
    }

    @Test
    public void emptyRightSide() throws Exception {
        // simple empty table
        ResultSet rs = methodWatcher.executeQuery("select * from A left join EMPTY_TABLE --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on a1=e1");
        assertEquals("" +
                "A1  | A2  | E1  | E2  |\n" +
                "------------------------\n" +
                "  1  | 10  |NULL |NULL |\n" +
                "  2  | 20  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |", toString(rs));

        // join right side on TRUE
        rs = methodWatcher.executeQuery("select * from A left join EMPTY_TABLE --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on TRUE");
        assertEquals("" +
                "A1  | A2  | E1  | E2  |\n" +
                "------------------------\n" +
                "  1  | 10  |NULL |NULL |\n" +
                "  2  | 20  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  3  | 30  |NULL |NULL |\n" +
                "  7  | 70  |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |NULL |", toString(rs));


        // join right side on TRUE, right side is select
        rs = methodWatcher.executeQuery("select * from A left join (select 1 r from EMPTY_TABLE) foo --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on TRUE");
        assertEquals("" +
                "A1  | A2  |  R  |\n" +
                "------------------\n" +
                "  1  | 10  |NULL |\n" +
                "  2  | 20  |NULL |\n" +
                "  3  | 30  |NULL |\n" +
                "  3  | 30  |NULL |\n" +
                "  7  | 70  |NULL |\n" +
                "NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |", toString(rs));

        // join right side on TRUE, right side is select, aggregate on top (regression test for DB-4003)
        rs = methodWatcher.executeQuery("select count(*) from A left join (select 1 r from EMPTY_TABLE) foo --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on TRUE");
        assertEquals("" +
                "1 |\n" +
                "----\n" +
                " 7 |", toString(rs));
    }

    private String toString(ResultSet rs) throws Exception {
        return TestUtils.FormattedResult.ResultFactory.toString(rs);
    }

}