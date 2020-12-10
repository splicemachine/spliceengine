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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class CachedOperationIT extends SpliceUnitTest {

    public static final String CLASS_NAME = CachedOperationIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void loadData() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table a(i int)")
                .withInsert("insert into a values(?)")
                .withRows(rows(
                        row(1),
                        row(2),
                        row(3),
                        row(4)))
                .create();
    }

    @Test
    public void testCacheDistinctScan() throws Exception {
        String sql = "select * from a where i in (select distinct (i) from a intersect select i from a) order by 1";
        try(ResultSet rs = methodWatcher.executeQuery(sql)){
            String s = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            String expected =
                    "I |\n" +
                    "----\n" +
                    " 1 |\n" +
                    " 2 |\n" +
                    " 3 |\n" +
                    " 4 |";
            Assert.assertEquals(s, expected, s);
        }
    }
}
