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

package com.splicemachine.derby.utils;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import org.apache.log4j.Logger;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(SerialTest.class)
public class SpliceAdmin_OperationsIT extends SpliceUnitTest{
    private static final Logger LOG = Logger.getLogger(SpliceAdmin_OperationsIT.class);
    public static final String CLASS_NAME = SpliceAdmin_OperationsIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceTableWatcher bigTableWatcher = new SpliceTableWatcher("TEST_BIG",CLASS_NAME,"(a int)");
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static final String USER_NAME = CLASS_NAME+"_USER";

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher).
            around(spliceSchemaWatcher).around(bigTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description){
                    try {
                        spliceClassWatcher.execute(String.format("call SYSCS_UTIL.SYSCS_CREATE_USER('%s', '%s')", USER_NAME, USER_NAME));
                    } catch (Exception e) {
                        // ignore if user already exists
                    }

                    try {
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT EXECUTE ON PROCEDURE SYSCS_UTIL.SYSCS_KILL_OPERATION TO " + USER_NAME);
                        spliceClassWatcher.execute("GRANT ALL PRIVILEGES ON TABLE " + bigTableWatcher + " TO " + USER_NAME);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    try{
                        PreparedStatement ps=spliceClassWatcher.prepareStatement(format("insert into %s (a) values (?)",bigTableWatcher));
                        for(int i=0;i<10;i++){
                            ps.setInt(1,i);
                            ps.executeUpdate();
                        }
                        ps=spliceClassWatcher.prepareStatement(format("insert into %s select * from %s",bigTableWatcher, bigTableWatcher));
                        for(int i=0;i<12;i++){
                            ps.executeUpdate();
                        }
                    }catch(Exception e){
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher();


    @Test
    public void testRunningOperations() throws Exception {
        String sql= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        // we should have 1 running operation, the call itself
        assertTrue(rs.next());
        assertEquals("SPLICE", rs.getString(2)); // check user
        assertEquals(sql, rs.getString(3)); // check user
    }

    @Test
    public void testKillOpenCursorControl() throws Exception {
        testKillOpenCursor(false);
    }

    @Test
    public void testKillOpenCursorSpark() throws Exception {
        testKillOpenCursor(true);
    }

    public void testKillOpenCursor(boolean useSpark) throws Exception {
        String sql= "select * from "+bigTableWatcher + " --splice-properties useSpark="+useSpark;

        PreparedStatement ps = methodWatcher.getOrCreateConnection().prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());

        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = methodWatcher.getOrCreateConnection().query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(3).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself

        // kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        methodWatcher.getOrCreateConnection().execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("SE008", se.getSQLState());
        }
    }


    @Test
    public void testOtherUsersCantKillOperation() throws Exception {
        TestConnection connection1 = methodWatcher.getOrCreateConnection();
        TestConnection connection2 = methodWatcher.createConnection(USER_NAME, USER_NAME);


        String sql= "select * from "+bigTableWatcher;

        PreparedStatement ps = connection1.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());


        // View from conn1
        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = connection1.query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(3).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself
        opsRs.close();


        // View from conn2
        opsRs = connection2.query(opsCall);

        count = 0;
        while(opsRs.next()) {
            count++;
        }
        assertEquals(1, count); // the open cursor should not be visible


        // try to kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        try {
            connection2.execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251N", se.getSQLState());
        }


        // kill the cursor
        connection1.execute(killCall);

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("SE008", se.getSQLState());
        }
    }

    @Test
    public void testDbOwnerCanKillOperation() throws Exception {
        TestConnection connection1 = methodWatcher.createConnection(USER_NAME, USER_NAME);
        TestConnection connection2 = methodWatcher.getOrCreateConnection();


        String sql= "select * from "+bigTableWatcher;

        PreparedStatement ps = connection1.prepareStatement(sql);
        ResultSet rs = ps.executeQuery();
        assertTrue(rs.next());


        // View from conn1
        String opsCall= "call SYSCS_UTIL.SYSCS_GET_RUNNING_OPERATIONS()";

        ResultSet opsRs = connection1.query(opsCall);

        int count = 0;
        String uuid = null;
        while(opsRs.next()) {
            count++;
            if (opsRs.getString(3).equals(sql)) {
                uuid = opsRs.getString(1);
            }
        }
        assertEquals(2, count); // 2 running operations, cursor + the procedure call itself
        opsRs.close();


        // View from conn2
        opsRs = connection2.query(opsCall);

        count = 0;
        while(opsRs.next()) {
            count++;
        }
        assertEquals(2, count); // the open cursor should also be visible to the db owner


        // kill the cursor
        String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('"+uuid+"')";
        connection2.execute(killCall);

        // try to kill the cursor again (it should fail)
        try {
            connection1.execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }

        // iterate over the cursor, should raise exception
        int rows = 0;
        try {
            while(rs.next()) {
                rows++;
            }
            fail("Should have raised exception");
        } catch (SQLException se) {
            LOG.debug("Raised exception after " + rows + " rows.");
            assertEquals("SE008", se.getSQLState());
        }
    }

    @Test
    public void testKillBadUUID() throws Exception {
        try {
            // wrong format for the UUID
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('')";
            methodWatcher.getOrCreateConnection().execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("22008", se.getSQLState());
        }

        try {
            // Invented UUID
            String killCall = "call SYSCS_UTIL.SYSCS_KILL_OPERATION('00000000-0000-0000-0000-000000000000')";
            methodWatcher.getOrCreateConnection().execute(killCall);
            fail("Should have raised exception");
        } catch (SQLException se) {
            assertEquals("4251P", se.getSQLState());
        }
    }

}
