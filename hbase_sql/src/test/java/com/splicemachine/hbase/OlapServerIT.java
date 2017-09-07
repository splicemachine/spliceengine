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
 *
 */

package com.splicemachine.hbase;

import com.google.common.io.ByteStreams;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@Category({SerialTest.class})
public class OlapServerIT extends SpliceUnitTest {

    private static final String SCHEMA = OlapServerIT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test
    public void testKillOlapServer() throws Exception {

        String sql = "select * from sys.systables --splice-properties useSpark=true";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());
        rs.close();

        // kill olap server master
        String cmd[] = {
                "/bin/sh",
                "-c",
                "kill `jps | grep OlapServerMaster | cut -d \" \" -f 1`"
        };
        String env[] = { "PATH=/bin:/usr/bin"};
        Process proc = Runtime.getRuntime().exec(cmd, env);
        proc.waitFor();
        InputStream stdout = new BufferedInputStream(proc.getInputStream());
        ByteStreams.copy(stdout, System.out);


        InputStream stderr = new BufferedInputStream(proc.getErrorStream());
        ByteStreams.copy(stderr, System.err);

        Thread.sleep(5000);

        try {
            rs = methodWatcher.executeQuery(sql);
            rs.next();
            fail("Expected excetion");
        } catch (SQLException e) {
            assertEquals("OS001", e.getSQLState());
        }

        Thread.sleep(5000);

        rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());
    }

    public static void main(String[] args) throws IOException {
        Runtime.getRuntime().exec("sh -c \"kill `jps | grep OlapServerMaster | cut -d \\\" \\\" -f 1`\"");
    }
}
