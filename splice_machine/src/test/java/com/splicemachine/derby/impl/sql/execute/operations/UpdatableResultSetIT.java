/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class UpdatableResultSetIT {

    private static final     String              CLASS_NAME         = UpdatableResultSetIT.class.getSimpleName().toUpperCase();
    private static final     SpliceWatcher       spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    private static final     SpliceSchemaWatcher schemaWatcher      = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final   SpliceTableWatcher  A_TABLE            = new SpliceTableWatcher("A", schemaWatcher.schemaName,
                                                                                             "(c1 int, c2 varchar(10))");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(A_TABLE);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @Before
    public void prepareTableA() throws Exception {
        methodWatcher.executeUpdate("DELETE FROM A");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (2, 'helium')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (10, 'neon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (18, 'argon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (38, 'krypton')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (54, 'xenon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (86, 'radon')");
        methodWatcher.executeUpdate("INSERT INTO A VALUES (118, 'oganesson')");
    }

    private void tableAshouldBe(Object[][] expected) throws SQLException {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A ORDER BY C1 ASC")) {
            int row = 0;
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    Assert.assertEquals(expected[row][0], resultSet.getInt(1));
                    Assert.assertEquals(expected[row][1], resultSet.getString(2));
                    row++;
                }
            }
        }
    }

    @Test
    public void testUpdate() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "HELIUM"},
                {10, "NEON"},
                {18, "ARGON"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"}});
}

    @Test
    public void testUpdateWithFilter() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE C1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 20);
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    resultSet.updateString(2, resultSet.getString(2).toUpperCase());
                    resultSet.updateRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {10, "neon"},
                {18, "argon"},
                {38, "KRYPTON"},
                {54, "XENON"},
                {86, "RADON"},
                {118, "OGANESSON"}});
    }

    @Test
    public void testDelete() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    resultSet.deleteRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{});
    }

    @Test
    public void testDeleteWithFilter() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE C1 BETWEEN ? AND ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 10);
            statement.setInt(2, 40);
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    resultSet.deleteRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {54, "xenon"},
                {86, "radon"},
                {118, "oganesson"}});
}

    @Test
    public void testInsert() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    int c1 = resultSet.getInt(1);
                    String c2 = resultSet.getString(2);
                    resultSet.moveToInsertRow();
                    resultSet.updateInt(1, c1);
                    resultSet.updateString(2, c2);
                    resultSet.insertRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {2, "helium"},
                {10, "neon"},
                {10, "neon"},
                {18, "argon"},
                {18, "argon"},
                {38, "krypton"},
                {38, "krypton"},
                {54, "xenon"},
                {54, "xenon"},
                {86, "radon"},
                {86, "radon"},
                {118, "oganesson"},
                {118, "oganesson"}
        });
    }

    @Test
    public void testInsertWithFilter() throws Exception {
        try(PreparedStatement statement = methodWatcher.prepareStatement("SELECT * FROM A WHERE c1 > ?", ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            statement.setInt(1, 20);
            try(ResultSet resultSet = statement.executeQuery()) {
                while(resultSet.next()) {
                    int c1 = resultSet.getInt(1);
                    String c2 = resultSet.getString(2);
                    resultSet.moveToInsertRow();
                    resultSet.updateInt(1, c1);
                    resultSet.updateString(2, c2);
                    resultSet.insertRow();
                }
            }
        }
        tableAshouldBe(new Object[][]{
                {2, "helium"},
                {10, "neon"},
                {18, "argon"},
                {38, "krypton"},
                {38, "krypton"},
                {54, "xenon"},
                {54, "xenon"},
                {86, "radon"},
                {86, "radon"},
                {118, "oganesson"},
                {118, "oganesson"}
        });
    }
}
