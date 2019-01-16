/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * Created by jyuan on 12/17/18.
 */
public class SpliceClobIT extends SpliceUnitTest {
    private static final String CLASS_NAME = SpliceClobIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static TestConnection conn;

    @ClassRule
    public static SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @BeforeClass
    public static void setUp() throws Exception{
        conn = methodWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table documents (id INT, c CLOB(64 K))")
                .create();
    }

    @Test
    public void testClob() throws Exception {
        String clobString = "A TEST OF CLOB INSERT...";
        PreparedStatement ps = conn.prepareStatement("INSERT INTO documents VALUES (?, ?)");
        Clob clob = conn.createClob();
        clob.setString(1,clobString);
        ps.setInt(1, 100);
        ps.setClob(2, clob);
        ps.execute();

        ps = conn.prepareStatement("SELECT c FROM documents where id=100");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String s = rs.getString(1);
            Assert.assertEquals(s, clobString, s);
        }
    }

    @Test
    public void testClobWithString() throws Exception {
        PreparedStatement ps = conn.prepareStatement("INSERT INTO documents VALUES (?, ?)");
        String clobString = "Another TEST OF CLOB INSERT...";
        ps.setInt(1, 200);
        ps.setString(2, clobString);
        ps.execute();
        ps = conn.prepareStatement("SELECT c FROM documents where id=200");
        ResultSet rs = ps.executeQuery();
        while (rs.next()) {
            String s = rs.getString(1);
            Assert.assertEquals(s, clobString, s);
        }
    }
}
