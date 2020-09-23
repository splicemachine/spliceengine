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
public class MergeJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = MergeJoinSelectivityIT.class.getSimpleName().toUpperCase();
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
            rowContainsQuery(s,
                    new int[]{1,3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk, ts_5_spk --splice-properties joinStrategy=MERGE\n where ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=10","MergeJoin");
        }
    }

    @Test
    public void leftOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_spk left outer join ts_5_spk --splice-properties joinStrategy=MERGE\n on ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=10","MergeLeftOuterJoin");
        }
    }

    @Test
    public void rightOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,4},
                    "explain select * from ts_10_spk --splice-properties joinStrategy=MERGE\n right outer join ts_5_spk on ts_10_spk.c1 = ts_5_spk.c1",
                    "rows=5","MergeLeftOuterJoin");
        }
    }

    @Test
    public void testInnerJoinWithStartEndKey() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(
                    s,
                    new int[]{1,3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_3_spk, ts_10_spk --splice-properties joinStrategy=MERGE\n where ts_10_spk.c1 = ts_3_spk.c1",
                    "rows=3","MergeJoin");
        }
    }

    @Test
    //DB-4106: make sure for merge join, plan has a lower cost if outer table is empty
    public void testEmptyInputTable() throws Exception {
        try(Statement statement = methodWatcher.getOrCreateConnection().createStatement()){
            String query="explain \n"+
                    "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n"+
                    "t1 --SPLICE-PROPERTIES index=t1i\n"+
                    ", t2--SPLICE-PROPERTIES index=t2j,joinStrategy=MERGE\n"+
                    "where i=j";

            String s=null;
            try(ResultSet rs=statement.executeQuery(query)){
                while(rs.next()){
                    s=rs.getString(1);
                    if(s.contains("MergeJoin"))
                        break;
                }
            }
            double cost1=getTotalCost(s);

            query="explain \n"+
                    "select * from --SPLICE-PROPERTIES joinOrder=FIXED\n"+
                    "t2 --SPLICE-PROPERTIES index=t2j\n"+
                    ", t1--SPLICE-PROPERTIES index=t1i, joinStrategy=MERGE\n"+
                    "where i=j";
            try(ResultSet rs=statement.executeQuery(query)){
                while(rs.next()){
                    s=rs.getString(1);
                    if(s.contains("MergeJoin"))
                        break;
                }
            }
            double cost2=getTotalCost(s);

            Assert.assertTrue(cost1<cost2);
        }
    }
}
