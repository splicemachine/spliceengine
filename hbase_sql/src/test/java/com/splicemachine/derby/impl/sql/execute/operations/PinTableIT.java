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
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("PinTable1",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("PinTable2",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("PinTable3",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("PinTable4",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("PinTable5",SCHEMA_NAME,"(col1 int)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5);
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
        methodWatcher.executeUpdate("insert into PinTable1 values (1)");
        methodWatcher.executeUpdate("pin table PinTable1");
        ResultSet rs = methodWatcher.executeQuery("select * from PinTable1 --splice-properties pin=true");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }

    @Test
    public void selectFromPinThatDoesNotExist() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into PinTable2 values (1)");
            ResultSet rs = methodWatcher.executeQuery("select * from PinTable2 --splice-properties pin=true");
            Assert.assertEquals("foo", TestUtils.FormattedResult.ResultFactory.toString(rs));
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT16",e.getSQLState());
        }
    }


    @Test
    public void testPinTableMarkedInDictionnary() throws Exception {
        methodWatcher.executeUpdate("insert into PinTable3 values (1)");
        methodWatcher.executeUpdate("pin table PinTable3");
        ResultSet rs = methodWatcher.executeQuery("select IS_PINNED from SYS.SYSTABLES where TABLENAME='PINTABLE3'");
        Assert.assertEquals("IS_PINNED |\n" +
                "------------\n" +
                "   true    |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testPinTableNotMarkedInDictionnary() throws Exception {
        methodWatcher.executeUpdate("insert into PinTable4 values (1)");
        ResultSet rs = methodWatcher.executeQuery("select IS_PINNED from SYS.SYSTABLES where TABLENAME='PINTABLE4'");
        Assert.assertEquals("IS_PINNED |\n" +
                "------------\n" +
                "   false   |", TestUtils.FormattedResult.ResultFactory.toString(rs));
    }


    @Test
    public void testPinTableInsertViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into PinTable5  --splice-properties pin=true \n values (1)");
            Assert.fail("INSERT  in a pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT29",e.getSQLState());
        }
    }

    @Test
    public void testPinTableUpdateViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("UPDATE PinTable5 --splice-properties pin=true \n SET col1=20");
            Assert.fail("UPDATE on a pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT30",e.getSQLState());
        }
    }

    @Test
    public void testPinTableDeleteViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("DELETE FROM PinTable5 --splice-properties pin=true \n");
            Assert.fail("DELETE from a pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT30",e.getSQLState());
        }
    }
}