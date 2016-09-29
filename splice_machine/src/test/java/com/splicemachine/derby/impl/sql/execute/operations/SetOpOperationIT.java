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
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 * Test for flushing out Splice Machine handling of with clauses
 *
 * WITH... with_query_1 [(col_name[,...])]AS (SELECT ...),
 *  ... with_query_2 [(col_name[,...])]AS (SELECT ...[with_query_1]),
 *  .
 *  .
 *  .
 *  ... with_query_n [(col_name[,...])]AS (SELECT ...[with_query1, with_query_2, with_query_n [,...]])
 *  SELECT
 *
 *
 */
@RunWith(Parameterized.class)
public class SetOpOperationIT extends SpliceUnitTest {
        private static final String SCHEMA = SetOpOperationIT.class.getSimpleName().toUpperCase();
        private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
        private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public SetOpOperationIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }



    @ClassRule
        public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

        @Rule
        public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

        @BeforeClass
        public static void createSharedTables() throws Exception {
            TestConnection connection = spliceClassWatcher.getOrCreateConnection();
            new TableCreator(connection)
                    .withCreate("create table FOO (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO values(?,?)")
                    .withRows(rows(row(1, 1), row(2, 1), row(3, 1), row(4, 1), row(5, 1))).create();

            new TableCreator(connection)
                    .withCreate("create table FOO2 (col1 int primary key, col2 int)")
                    .withInsert("insert into FOO2 values(?,?)")
                    .withRows(rows(row(1, 5), row(3, 7), row(5, 9))).create();

        }

    @Test
    public void testIntercept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(format("select count(*), max(col1), min(col1) " +
                " from  (select col1 from foo --SPLICE-PROPERTIES useSpark = %s  \n" +
                "intersect select col1 from foo2) argh",useSpark));

        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 3, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 5, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 1, rs.getInt(3));
    }

    @Test(expected = SQLException.class)
    public void testInterceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo --SPLICE-PROPERTIES useSpark = %s  \n intersect all select col1 from foo2) argh",
                    useSpark));
    }

    @Test
    public void testExcept() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo  --SPLICE-PROPERTIES useSpark = %s  \n except select col1 from foo2) argh",useSpark));
        Assert.assertTrue("intersect incorrect",rs.next());
        Assert.assertEquals("Wrong Count", 2, rs.getInt(1));
        Assert.assertEquals("Wrong Max", 4, rs.getInt(2));
        Assert.assertEquals("Wrong Min", 2, rs.getInt(3));
    }

    @Test(expected = SQLException.class)
    public void testExceptAll() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                format("select count(*), max(col1), min(col1) from (" +
                "select col1 from foo --SPLICE-PROPERTIES useSpark = %s except all select col1 from foo2) argh",useSpark));
    }

}