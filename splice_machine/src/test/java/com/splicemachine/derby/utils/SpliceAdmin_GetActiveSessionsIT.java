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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

@Category(value = {SerialTest.class})
public class SpliceAdmin_GetActiveSessionsIT extends SpliceUnitTest {
    final protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = SpliceAdminIT.class.getSimpleName().toUpperCase();
    final protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain =
            RuleChain.outerRule(spliceClassWatcher)
                    .around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testGetActiveSessions() throws Exception {
        try (TestConnection rs1Conn = methodWatcher.createConnection()) {
            Set<String> rs1Result = new HashSet<>();
            String rs1ConnId;
            try (Statement s = rs1Conn.createStatement()) {
                try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_active_sessions()")) {
                    while (rs.next()) {
                        rs1Result.add(rs.getString(1));
                    }
                }
                try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_session_info()")) {
                    Assert.assertTrue(rs.next());
                    rs1ConnId = (rs.getString(3));
                }
            }
            Assert.assertTrue(rs1Result.contains(rs1ConnId));

            Set<String> rs2Result = new HashSet<>();
            String rs2ConnId;
            if (isMemPlatform(spliceClassWatcher)) {
                try (TestConnection secondConn = methodWatcher.createConnection()) {
                    try (Statement s = secondConn.createStatement()) {
                        try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_active_sessions()")) {
                            while (rs.next()) {
                                rs2Result.add(rs.getString(1));
                            }
                        }
                        try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_session_info()")) {
                            Assert.assertTrue(rs.next());
                            rs2ConnId = (rs.getString(3));
                        }
                    }
                }
            } else {
                try (TestConnection rs2Conn = spliceClassWatcher.connectionBuilder().user("splice").password("admin").port(1528).create(true).build()) {
                    try (Statement s = rs2Conn.createStatement()) {
                        try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_active_sessions()")) {
                            while (rs.next()) {
                                rs2Result.add(rs.getString(1));
                            }
                        }
                        try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_session_info()")) {
                            Assert.assertTrue(rs.next());
                            rs2ConnId = (rs.getString(3));
                        }
                    }
                }
            }
            Assert.assertTrue(rs2Result.contains(rs2ConnId));
            Assert.assertTrue(rs2Result.contains(rs1ConnId));
            Assert.assertTrue(rs2Result.size() > rs1Result.size());
            Assert.assertTrue(rs2Result.containsAll(rs1Result));


            Set<String> rs1Result2 = new HashSet<>();
            try (Statement s = rs1Conn.createStatement()) {
                try (ResultSet rs = s.executeQuery("call syscs_util.syscs_get_active_sessions()")) {
                    while (rs.next()) {
                        rs1Result2.add(rs.getString(1));
                    }
                }
            }
            Assert.assertTrue(rs1Result2.contains(rs1ConnId));
            Assert.assertFalse(rs1Result2.contains(rs2ConnId));
        }
    }
}
