/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.util.Collection;

import static org.junit.Assert.assertTrue;

/**
 * Test Row Trigger Performance
 */
@Category(value = {SerialTest.class})
@RunWith(Parameterized.class)
public class Trigger_Performance_IT extends SpliceUnitTest {
    
    private Boolean useSpark;
    private static int numTables = 0;
    private static boolean isMemPlatform = false;
    
    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }
    public static final String CLASS_NAME = Trigger_Performance_IT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);
    
    public Trigger_Performance_IT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    @BeforeClass
    public static void createdSharedTables() throws Exception {
        spliceClassWatcher.executeUpdate("create table dummyTable(a int, b int)");
        spliceClassWatcher.executeUpdate("insert into dummyTable values(2,2)");

        spliceClassWatcher.executeUpdate("create table sourceTable(a int, b int)");
        spliceClassWatcher.executeUpdate("insert into sourceTable values(1,1)");

        spliceClassWatcher.executeUpdate("create table targetTable(a int, b int)");

        String sqlText = "insert into sourceTable select * from sourceTable";
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);
		spliceClassWatcher.executeUpdate(sqlText);

		spliceClassWatcher.execute(format("call syscs_util.syscs_flush_table('%s', 'sourceTable')", CLASS_NAME));

        sqlText = "CREATE TRIGGER Trig%d\n" +
                "  NO CASCADE\n" +
                "  BEFORE INSERT ON targetTable\n" +
                "  REFERENCING NEW AS N\n" +
                "  FOR EACH ROW\n" +
                "  WHEN ((N.a NOT IN\n" +
                "         (SELECT A.a FROM dummyTable A, dummyTable B WHERE A.a = B.a ))\n" +
                "        AND\n" +
                "        (N.b NOT IN (SELECT b FROM dummyTable C WHERE C.a = 2)))\n" +
                "  BEGIN ATOMIC\n" +
                "    select 1;\n" +
                "  END";
        for (int i=0; i < 15; i++)
            spliceClassWatcher.executeUpdate(format(sqlText, i));
    }

    @BeforeClass
    public static void recordNumTables() throws Exception {
        isMemPlatform = isMemPlatform(spliceClassWatcher);
        vacuum();
        numTables = getNumTables();
    }

    @AfterClass
    public static void checkNumTables() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
        spliceClassWatcher.close();
        vacuum();
        int newNumTables = getNumTables();
        // Mem platform doesn't physically remove dropped tables, so
        // this is an HBase-only check.
        if (!isMemPlatform)
            assertTrue("\nStarted with " + numTables + " tables and ended with " + newNumTables,
                         numTables >= newNumTables);
    }

    public static void
    vacuum() throws Exception{
        // Mem doesn't have vacuum.
        if (!isMemPlatform)
            spliceClassWatcher.executeUpdate("CALL SYSCS_UTIL.VACUUM()");
    }

    public static int
    getNumTables() throws Exception{
        try (ResultSet rs = spliceClassWatcher.executeQuery("CALL SYSCS_UTIL.SYSCS_GET_TABLE_COUNT()")) {
            rs.next();
            return ((Integer)rs.getObject(1));
        }
    }

    @Test
    public void testConcurrentTriggersRuntimeUnderLimit() throws Exception {
        // Make sure spark is warmed up
        if (useSpark) {
            try (ResultSet rs = methodWatcher.executeQuery("select * from sourceTable" +
                "--splice-properties useSpark=" + useSpark + "\n")) {
            }
            try (ResultSet rs = methodWatcher.executeQuery("select * from sourceTable" +
                "--splice-properties useSpark=" + useSpark + "\n")) {
            }
        }
        long startTime = System.currentTimeMillis();
        methodWatcher.execute("insert into targetTable --splice-properties useSpark=" + useSpark +
                              "\n select * from sourceTable");
        long endTime = System.currentTimeMillis();
        long runTime = endTime - startTime;
        assertTrue("Expected runtime to be less than 15 seconds.  Actual time: " + runTime + " milliseconds", runTime < 15000);

    }

}
