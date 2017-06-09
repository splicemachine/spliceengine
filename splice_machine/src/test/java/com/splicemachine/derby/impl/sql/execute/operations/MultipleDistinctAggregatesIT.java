/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.impl.sql.compile.UnsatTreePruningIT;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 6/8/17.
 */
public class MultipleDistinctAggregatesIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(UnsatTreePruningIT.class);
    public static final String CLASS_NAME = UnsatTreePruningIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 varchar(10), c1 float)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();


        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 varchar(10), c2 float)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 varchar(10), c3 float)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 varchar(10), c4 float)")
                .withInsert("insert into t4 values(?,?,?)")
                .withRows(rows(
                        row(1, "aaa", 1.0),
                        row(2, "bbb", 2.0),
                        row(null, null, null)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testMainQueryWithUnsat() throws Exception {

    }
}
