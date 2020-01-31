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
 * Some parts of this source code are based n Apache Derby, and the following notices apply to
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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;


/**
 * Test costing of Merge Join.
 */
@Category(value = {SerialTest.class})
public class MergeJoinCostingIT extends SpliceUnitTest {


    private static final String SCHEMA = MergeJoinCostingIT.class.getSimpleName();
    public static final String CLASS_NAME = MergeJoinCostingIT.class.getSimpleName().toUpperCase();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void initTables() throws Exception {
        TestConnection conn;
        spliceSchemaWatcher.cleanSchemaObjects();
        conn = classWatcher.createConnection(true);
        classWatcher.setAutoCommit(false);

        conn.execute("create table t1 (a int, primary key(a))");
        conn.execute("create table t2 (a int, b int, c int)");
        conn.execute("create index mjcosting_idx1 on t1(a)");
        conn.execute("create index mjcosting_idx2 on t2(a, b, c)");

        String [] Inserts = {"insert into t1 values (1)",
        "insert into t1 select a+1 from t1",
        "insert into t1 select a+2 from t1",
        "insert into t1 select a+4 from t1",
        "insert into t1 select a+8 from t1",
        "insert into t1 select a+16 from t1",
        "insert into t1 select a+32 from t1",
        "insert into t1 select a+64 from t1",
        "insert into t1 select a+128 from t1",
        "insert into t1 select a+256 from t1",
        "insert into t1 select a+512 from t1",
        "insert into t1 select a+1024 from t1",
        "insert into t1 select a+2048 from t1",
        "insert into t1 select a+4096 from t1",
        "insert into t1 select a+8192 from t1",
        "insert into t1 select a+16384 from t1",
        "insert into t1 select a+32768 from t1",
        "insert into t2 select a,a,a from t1",
        "insert into t1 select a+65536 from t1",
        "insert into t1 select a+131072 from t1",
        "insert into t1 select a+262144 from t1",
        "insert into t1 select a+524288 from t1",
        "insert into t1 select a+1048576 from t1"};

        for (String sql:Inserts)
            conn.execute(sql);

        conn.execute("analyze table t1");
        conn.execute("analyze table t2");
    }

    @AfterClass
    public static void cleanTables() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
    }

    @Test
    public void testSmallTablePickedAsLeftOfMergeJoin() throws Exception {
        String sql = "explain\n" +
                     "select count(*) from t1 --splice-properties index=mjcosting_idx1\n" +
                     ", t2 --splice-properties index=mjcosting_idx2\n" +
                     "where t1.a = t2.a and t2.b between 1 and 1000";
        String matchString = "T2.B[0:2] <= 1000";

        // Reading the small table first avoids scanning most of the rows
        // in the large table in this case.
        rowContainsQuery(6, sql, "MergeJoin", classWatcher);
        rowContainsQuery(9, sql, matchString, classWatcher);
    }

}
