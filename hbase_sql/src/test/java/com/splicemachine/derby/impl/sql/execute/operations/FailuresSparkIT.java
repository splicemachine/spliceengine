/*
 *
 *  * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *  *
 *  * This file is part of Splice Machine.
 *  * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 *  * GNU Affero General Public License as published by the Free Software Foundation, either
 *  * version 3, or (at your option) any later version.
 *  * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 *  * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 *  * See the GNU Affero General Public License for more details.
 *  * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 *  * If not, see <http://www.gnu.org/licenses/>.
 *
 *
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.HBaseTestUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * A Collection of ITs oriented around scanning and inserting from Spark with failures.
 */
public class FailuresSparkIT {
    private static Logger LOG=Logger.getLogger(FailuresSparkIT.class);

    public static final String CLASS_NAME = FailuresSparkIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    private static final SpliceSchemaWatcher schema = new SpliceSchemaWatcher(CLASS_NAME);

    private static final SpliceTableWatcher a_table = new SpliceTableWatcher("a",CLASS_NAME,"(col1 int primary key)");

    @ClassRule
    public static TestRule chain=RuleChain.outerRule(classWatcher)
            .around(schema)
            .around(a_table);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(CLASS_NAME);

    @Test
    public void testPKViolationIsRolledback() throws Throwable {
        try(Statement s =methodWatcher.getOrCreateConnection().createStatement()) {
            String sql = "insert into a (col1) select * from ( " +
                    "values (1) union all values (1) " +
                    "union all select * from a --splice-properties useSpark=true\n" +
                    ") a";
            try {
                s.executeUpdate(sql);
                fail("Should have failed due to PK violation");
            } catch (SQLException se) {
                // ERROR 23505: The statement was aborted because it would have caused a duplicate key value in a unique
                Assert.assertEquals("Should fail with PK violation exception", "23505", se.getSQLState());
            }
            try (ResultSet rs = s.executeQuery("select * from a")) {
                Assert.assertFalse("Rows returned from query!", rs.next());
            }
        }
    }
}
