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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.Statement;

/**
 *
 *
 *
 */
public class NestedLoopJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = NestedLoopJoinSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createJoinDataSet(spliceClassWatcher, spliceSchemaWatcher.toString());
    }
    @Test
    public void innerJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk, ts_5_spk --splice-properties joinStrategy=NESTEDLOOP\n where ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=10","NestedLoopJoin");
        }
    }

    @Test
    public void antiJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,5},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk where not exists (select * from  ts_5_spk --splice-properties joinStrategy=NESTEDLOOP\n where ts_10_spk.c1 = ts_5_spk.c1)",
                    "rows=10","MergeSortLeftOuterJoin");
        }
    }

    @Test
    public void leftOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk left outer join ts_5_spk --splice-properties joinStrategy=NESTEDLOOP\n on ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=10","NestedLoopLeftOuterJoin");
        }
    }

    @Test
    public void rightOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,3},
                    "explain select * from ts_10_spk --splice-properties joinStrategy=NESTEDLOOP\n right outer join ts_5_spk on ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=5","NestedLoopRightOuterJoin");
        }
    }

    @Test
    //DB-4102: bump up row count to 1 to avoid divided by zero error when computing cost
    public void testEmptyInputTable() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            String query="explain \n"+
                    "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n"+
                    "t2 b --SPLICE-PROPERTIES index=t2j\n"+
                    ",t1 a--SPLICE-PROPERTIES index=t1i, joinStrategy=MERGE\n"+
                    ", t2 c--SPLICE-PROPERTIES joinStrategy=NESTEDLOOP \n"+
                    "where a.i=b.j and b.j = c.j";
            try(ResultSet rs=s.executeQuery(query)){
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.next());
                String str=rs.getString(1);
                Assert.assertFalse(str.contains("totalCost=�"));
            }
        }
    }
}