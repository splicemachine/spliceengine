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

import java.sql.Statement;

/**
 *
 *
 *
 */
public class BroadcastJoinSelectivityIT extends BaseJoinSelectivityIT {
    public static final String CLASS_NAME = BroadcastJoinSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(spliceSchemaWatcher.schemaName);

    @BeforeClass
    public static void createDataSet() throws Exception {
        createJoinDataSet(spliceClassWatcher, spliceSchemaWatcher.toString());
    }
    @Test
    public void innerJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(s,
                    new int[]{3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_npk, ts_5_npk --splice-properties joinStrategy=BROADCAST\n where ts_10_npk.c1 = ts_5_npk.c1",
                    "BroadcastJoin");
        }
    }

    @Test
    public void leftOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(s,
                    new int[]{3},
                    "explain select * from --splice-properties joinOrder=fixed\n ts_10_npk left outer join ts_5_npk --splice-properties joinStrategy=BROADCAST\n on ts_10_npk.c1 = ts_5_npk.c1",
                    "BroadcastLeftOuterJoin");
        }
    }

    @Test
    public void rightOuterJoin() throws Exception {
        try(Statement s = methodWatcher.getOrCreateConnection().createStatement()){
            rowContainsQuery(s,
                    new int[]{3},
                    "explain select * from ts_10_npk --splice-properties joinStrategy=BROADCAST\n right outer join ts_5_npk on ts_10_npk.c1 = ts_5_npk.c1",
                    "BroadcastRightOuterJoin");
        }
    }

}