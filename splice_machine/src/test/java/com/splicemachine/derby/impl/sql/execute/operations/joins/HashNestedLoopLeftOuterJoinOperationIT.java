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

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;

import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
@Ignore
public class HashNestedLoopLeftOuterJoinOperationIT {

    private static final String CLASS_NAME = HashNestedLoopLeftOuterJoinOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher watcher = new SpliceWatcher(CLASS_NAME);

    @Test
    public void leftJoin() throws Exception {

        Connection conn = watcher.getOrCreateConnection();

        new TableCreator(conn)
                .withCreate("create table t1 (c1 int, c2 int, primary key(c1))")
                .withInsert("insert into t1 values(?,?)")
                .withRows(rows(row(1, 10), row(2, 20), row(3, 30), row(4, 40), row(5, 50), row(6, 60))).create();

        new TableCreator(conn)
                .withCreate("create table t2 (c1 int, c2 int, primary key(c1))")
                .withInsert("insert into t2 values(?,?)")
                .withRows(rows(row(1, 10), row(3, 30), row(5, 50), row(6, 60))).create();

        String JOIN_SQL = "select * from --SPLICE-PROPERTIES joinOrder=fixed\n" +
                "t1 LEFT join t2 --SPLICE-PROPERTIES joinStrategy=HASH\n" +
                "on t1.c1 = t2.c1";

        ResultSet rs = conn.createStatement().executeQuery(JOIN_SQL);

        String EXPECTED = "" +
                "C1 |C2 | C1  | C2  |\n" +
                "--------------------\n" +
                " 1 |10 |  1  | 10  |\n" +
                " 2 |20 |NULL |NULL |\n" +
                " 3 |30 |  3  | 30  |\n" +
                " 4 |40 |NULL |NULL |\n" +
                " 5 |50 |  5  | 50  |\n" +
                " 6 |60 |  6  | 60  |";

        assertEquals(EXPECTED, TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

}
