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
 *
 */

package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.procedures.SpliceAdmin;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

@Category(value = {SerialTest.class})
public class FlushDeadlockWhenRegionInClosingStateIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(FlushDeadlockWhenRegionInClosingStateIT.class);
    private static final String SCHEMA = FlushDeadlockWhenRegionInClosingStateIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
          .around(spliceSchemaWatcher);

    @Rule
    public final SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    @Test(timeout = 60000)
    public void testNoDeadlock() throws Exception {
        String table = "testNoDeadlock";
        long[] conglomerates = SpliceAdmin.getConglomNumbers(methodWatcher.getOrCreateConnection(), "SYS", "SYSTABLES");
        methodWatcher.execute(format("create table %s.%s (a int)", SCHEMA, table));
        methodWatcher.executeUpdate("CALL SYSCS_UTIL.INVALIDATE_GLOBAL_DICTIONARY_CACHE()");

        try (Admin admin = ConnectionFactory.createConnection(new Configuration()).getAdmin()) {
            /*
             * Disabling SYS.SYSTABLES will trigger a flush as part of closing the region and will end up in a bad state
             * if we do not account for the region closing in SpliceDefaultFlusher
             * See DB-11693
             */
            HBaseTestUtils.disable(admin, TableName.valueOf("splice:" + conglomerates[0]), LOG);
            HBaseTestUtils.enable(admin, TableName.valueOf("splice:" + conglomerates[0]), LOG);
        }
    }
}
