/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 *
 * IT's for external table functionality
 *
 */
public class PinTableIT extends SpliceUnitTest{

    private static final String SCHEMA_NAME = PinTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("t1",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("t2",SCHEMA_NAME,"(col1 int)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
    .around(spliceTableWatcher2);
    @Test
    public void testPinTableDoesNotExist() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("pin table foo");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","X0X05",e.getSQLState());
        }
    }

    @Test
    public void testPinTable() throws Exception {
            methodWatcher.executeUpdate("insert into t1 values (1)");
            methodWatcher.executeUpdate("pin table t1");
            ResultSet rs = methodWatcher.executeQuery("select * from t1 --splice-properties pin=true");
            Assert.assertEquals("COL1 |\n" +
                    "------\n" +
                    "  1  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void selectFromPinThatDoesNotExist() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into t2 values (1)");
            ResultSet rs = methodWatcher.executeQuery("select * from t2 --splice-properties pin=true");
            Assert.assertEquals("foo", TestUtils.FormattedResult.ResultFactory.toString(rs));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT16",e.getSQLState());
        }
    }

}