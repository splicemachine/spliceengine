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
 *
 */

package com.splicemachine.hbase;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.ResultSet;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

@Category({SerialTest.class})
public class OlapServerIT extends SpliceUnitTest {
    private static final Logger LOG = Logger.getLogger(OlapServerIT.class);

    private static final String SCHEMA = OlapServerIT.class.getSimpleName().toUpperCase();
    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Test(timeout = 120000)
    public void testKillOlapServer() throws Exception {

        String sql = "select * from sys.systables --splice-properties useSpark=true";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());
        rs.close();

        int pid = getPid();

        LOG.info("Current pid " + pid);

        killOlapServer(pid);

        int newPid = pid;
        while (newPid == pid) {
            Thread.sleep(500);
            newPid = getPid();
        }
        assertNotEquals(pid, newPid);

        Thread.sleep(2000);

        // after DB-8949 the first query should already succeed
        rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());

        Thread.sleep(2000);

        rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());
    }

    @Test(timeout = 120000)
    public void restartOlapServer() throws Exception {
        String sql = "select * from sys.systables --splice-properties useSpark=true";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertTrue(rs.next());
        rs.close();

        int pid = getPid();
        LOG.info("Current pid " + pid);

        rs = methodWatcher.executeQuery("call syscs_util.restart_olap_server()");
        rs.next();
        Assert.assertEquals("Restarted Olap server", rs.getString(1));

        int newPid = getPid();
        while (newPid == pid) {
            Thread.sleep(500);
            newPid = getPid();
        }
        LOG.info("new pid " + newPid);
    }

    private int getPid() throws Exception {
        // get olap server master pid
        String getPid[] = {
                "/bin/sh",
                "-c",
                "ps aux | grep OlapServerMaster | grep -v grep | grep -v bash | awk '{print $2}'"
        };
        String env[] = { "PATH=/bin:/usr/bin"};
        while (true) {
            Process proc = Runtime.getRuntime().exec(getPid, env);
            proc.waitFor();
            BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
            String line = reader.readLine();
            if (line != null) {
                int pid = Integer.parseInt(line);
                return pid;
            }
            else {
                LOG.info("Killed");
            }
            Thread.sleep(1000);
        }
    }

    private void killOlapServer(int pid) throws Exception{
        String env[] = {"PATH=/bin:/usr/bin"};
        // kill olap server master pid
        String kill[] = {
                "/bin/sh",
                "-c",
                "kill " + pid
        };

        Process proc = Runtime.getRuntime().exec(kill, env);
        proc.waitFor();

    }
}
