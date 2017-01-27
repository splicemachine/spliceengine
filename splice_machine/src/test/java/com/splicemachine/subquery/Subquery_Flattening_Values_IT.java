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

package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

import static com.splicemachine.subquery.SubqueryITUtil.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Test coverage for flattening of values subqueries.
 */
public class Subquery_Flattening_Values_IT {


    private static final String SCHEMA = Subquery_Flattening_Values_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);


    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @BeforeClass
    public static void createSharedTables() throws Exception {
        classWatcher.executeUpdate("create table A(a1 int, a2 int, a3 int)");
        classWatcher.executeUpdate("insert into A values(0,0,0),(1,10,10),(2,20,20),(3,30,30)");
    }

    @Test
    public void values() throws Exception {
        assertUnorderedResult(conn(), "select * from A where a1 = (values 1)", ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 1 |10 |10 |");
        assertUnorderedResult(conn(), "select * from A where a1 > (values 1)", ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |");
        assertUnorderedResult(conn(), "select * from A where a1 < (values 1)", ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |");
        assertUnorderedResult(conn(), "select * from A where a1 != (values 1)", ZERO_SUBQUERY_NODES, "" +
                "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 0 | 0 | 0 |\n" +
                " 2 |20 |20 |\n" +
                " 3 |30 |30 |");
    }

    @Test
    public void valuesThrows() throws Exception {
        try {
            assertUnorderedResult(conn(), "select * from A where a1 = (values 1,2)", ZERO_SUBQUERY_NODES, "");
            fail("expected exception");
        } catch (SQLException e) {
            assertEquals("Scalar subquery is only allowed to return a single row.", e.getMessage());
        }
    }


    private Connection conn() {
        return methodWatcher.getOrCreateConnection();
    }


}
