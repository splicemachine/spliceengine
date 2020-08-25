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

/**
 *
 * IT's for external table functionality
 *
 */
@Ignore  // DB-9778 is removing PIN tables.  Disable these tests for a clean IT run.
public class PinTableIT extends SpliceUnitTest{

    private static final String SCHEMA_NAME = PinTableIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("PinTable1",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher2 = new SpliceTableWatcher("PinTable2",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher3 = new SpliceTableWatcher("PinTable3",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher4 = new SpliceTableWatcher("PinTable4",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher5 = new SpliceTableWatcher("PinTable5",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher6 = new SpliceTableWatcher("PinTable6",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher7 = new SpliceTableWatcher("PinTable7",SCHEMA_NAME,"(col1 int)");
    private static final SpliceTableWatcher spliceTableWatcher8 = new SpliceTableWatcher("PinTable8",SCHEMA_NAME,"(col1 int)");

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(spliceTableWatcher2)
            .around(spliceTableWatcher3)
            .around(spliceTableWatcher4)
            .around(spliceTableWatcher5)
            .around(spliceTableWatcher6)
            .around(spliceTableWatcher7)
            .around(spliceTableWatcher8);
    @Test
    public void testPinTableDoesNotExist() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("pin table foo");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","42Y55",e.getSQLState());
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

        methodWatcher.executeUpdate("unpin table PinTable1");
        try {
            rs = methodWatcher.executeQuery("select * from PinTable1 --splice-properties pin=true");
            Assert.fail("pin read from unpinned table didn't fail");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT16",e.getSQLState());
        }
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
    public void testPinTableInsertViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("insert into PinTable5  --splice-properties pin=true \n values (1)");
            Assert.fail("INSERT is not allowed in pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT29",e.getSQLState());
        }
    }

    @Test
    public void testPinTableUpdateViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("UPDATE PinTable5 --splice-properties pin=true \n SET col1=20");
            Assert.fail("UPDATE is not allowed in pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT30",e.getSQLState());
        }
    }

    @Test
    public void testPinTableDeleteViolation() throws Exception {
        try {
            methodWatcher.executeUpdate("DELETE FROM PinTable5 --splice-properties pin=true \n");
            Assert.fail("UPDATE is not allowed in pin table but it didn't failed");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT31",e.getSQLState());
        }
    }

    @Test
    public void testDropPinnedTable() throws Exception {
        methodWatcher.executeUpdate("insert into PinTable6 values (1)");
        methodWatcher.executeUpdate("pin table PinTable6");
        ResultSet rs = methodWatcher.executeQuery("select * from PinTable6 --splice-properties pin=true");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        methodWatcher.executeUpdate("drop table PinTable6");
    }

    @Test
    public void testDropUnpinnedTable() throws Exception {
        methodWatcher.executeUpdate("insert into PinTable7 values (1)");
        methodWatcher.executeUpdate("pin table PinTable7");
        ResultSet rs = methodWatcher.executeQuery("select * from PinTable7 --splice-properties pin=true");
        Assert.assertEquals("COL1 |\n" +
                "------\n" +
                "  1  |", TestUtils.FormattedResult.ResultFactory.toString(rs));

        methodWatcher.executeUpdate("unpin table PinTable7");
        methodWatcher.executeUpdate("drop table PinTable7");
    }

    @Test
    public void testPinTwice() throws Exception {
        try {
            // Row Format not supported for Parquet
            methodWatcher.executeUpdate("pin table PinTable8");
            methodWatcher.executeUpdate("pin table PinTable8");
            Assert.fail("Exception not thrown");
        } catch (SQLException e) {
            Assert.assertEquals("Wrong Exception","EXT35",e.getSQLState());
        }
    }
}
