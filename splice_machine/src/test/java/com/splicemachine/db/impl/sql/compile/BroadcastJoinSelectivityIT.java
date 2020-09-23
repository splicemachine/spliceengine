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
                    new int[]{4},
                    "explain select * from ts_10_npk --splice-properties joinStrategy=BROADCAST\n right outer join ts_5_npk on ts_10_npk.c1 = ts_5_npk.c1",
                    "BroadcastLeftOuterJoin");
        }
    }

}
