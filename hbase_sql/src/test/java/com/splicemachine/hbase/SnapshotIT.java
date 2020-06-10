/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.hbase;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by jyuan on 6/6/17.
 */
public class SnapshotIT extends SpliceUnitTest
{
    private static final String SCHEMA = SnapshotIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createTables() throws Exception {
        TestConnection conn = classWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table T (i int)")
                .withInsert("insert into T values(?)")
                .withRows(rows(
                        row(1),
                        row(2),
                        row(3),
                        row(4)))
                .withIndex("create index ti on t(i)")
                .create();

        new TableCreator(conn)
                .withCreate("create table A (i int)")
                .withInsert("insert into A values(?)")
                .withRows(rows(
                        row(1),
                        row(2),
                        row(3),
                        row(4)))
                .withIndex("create index ai on a(i)")
                .create();

        conn.execute("call syscs_util.snapshot_table('SNAPSHOTIT', 'T', 'EXIST')");
    }

    @AfterClass
    public static void cleanup() throws Exception
    {
        try (TestConnection conn = classWatcher.getOrCreateConnection()) {
            conn.execute("call syscs_util.delete_snapshot('EXIST')");
            try (PreparedStatement ps = conn.prepareStatement("select count(*) from sys.syssnapshots")) {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                    Assert.assertEquals(0, rs.getInt(1));
                }
            }
        }
    }

    @Test
    public void testNonexistSnapshot() throws Exception
    {
        try
        {
            methodWatcher.execute("call syscs_util.restore_snapshot('nonexist')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals(sqlState, "SNA02");
        }
    }

    @Test
    public void testExistSnapshot() throws Exception
    {
        try
        {
            methodWatcher.execute("call syscs_util.snapshot_schema('SNAPSHOTIT','EXIST')");
            Assert.fail("Expected to fail");
        }
        catch (SQLException e)
        {
            String sqlState = e.getSQLState();
            Assert.assertEquals(sqlState, "SNA01");
        }
    }

    @Test
    public void testSnapshotTable() throws Exception
    {
        methodWatcher.execute("call syscs_util.snapshot_table('SNAPSHOTIT', 'T', 'S1')");
        ResultSet rs = methodWatcher.executeQuery("select count(*) from sys.syssnapshots where snapshotname='S1'");
        rs.next();
        Assert.assertEquals(2, rs.getInt(1));

        methodWatcher.execute("insert into t select * from t");
        rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();
        Assert.assertEquals(8, rs.getInt(1));

        methodWatcher.execute("call syscs_util.restore_snapshot('S1')");
        rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();
        Assert.assertEquals(4, rs.getInt(1));

        methodWatcher.execute("call syscs_util.delete_snapshot('S1')");
        rs = methodWatcher.executeQuery("select count(*) from sys.syssnapshots where snapshotname='S1'");
        rs.next();
        Assert.assertEquals(0, rs.getInt(1));
    }

    @Test
    public void testSnapshotSchema() throws Exception
    {
        methodWatcher.execute("call syscs_util.snapshot_schema('SNAPSHOTIT', 'S2')");
        ResultSet rs = methodWatcher.executeQuery("select count(*) from sys.syssnapshots where snapshotname='S2'");
        rs.next();
        Assert.assertEquals(4, rs.getInt(1));

        methodWatcher.execute("insert into t select * from t");
        rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();
        Assert.assertEquals(8, rs.getInt(1));

        methodWatcher.execute("call syscs_util.restore_snapshot('S2')");
        rs = methodWatcher.executeQuery("select count(*) from t");
        rs.next();
        Assert.assertEquals(4, rs.getInt(1));

        methodWatcher.execute("call syscs_util.delete_snapshot('S2')");
        rs = methodWatcher.executeQuery("select count(*) from sys.syssnapshots where snapshotname='S2'");
        rs.next();
        Assert.assertEquals(0, rs.getInt(1));
    }
}
