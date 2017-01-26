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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.derbynet;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Tests of the properties of the DRDA network protocol implementation.
 */
public class DRDAProtocolTest extends BaseJDBCTestCase {
    
    private String threadName;

    public void setUp() {
        /* Save the thread name as it gets changed in one of the fixtures */
        threadName = Thread.currentThread().getName();
    }

    public void tearDown() throws Exception {
        /* Restore the original thread name */

        super.tearDown();

        Thread.currentThread().setName(threadName);
    }

    /**
     * Tests the support for threads with characters not supported by EBCDIC
     *
     * @throws SQLException
     */
    public void testNonEBCDICCharacters() throws SQLException {
        Thread.currentThread().setName("\u4e10");

        /* Open a connection while the thread name has Japanese characters */
        Connection conn2 = openConnection("FIRSTDB1");
        conn2.close();
    }

    /** 
     * Tests whether multiple connections to different databases
     * on the same Derby instance are working without exceptions.
     * 
     * @throws SQLException if database interaction fails
     */
    public void testMultipleConnections() throws SQLException {
        Connection conn1 = openConnection("FIRSTDB1");
        conn1.setAutoCommit(false);

        Statement st = conn1.createStatement();
        st.execute("create table FIRSTDB_T1 (i int, j int, k int)");
        st.execute("insert into FIRSTDB_T1 values (1, 3, 5)");
        PreparedStatement pSt1 =
                conn1.prepareStatement("select * from FIRSTDB_T1");
        
        ResultSet rs1 = pSt1.executeQuery();
        rs1.next();
        rs1.close();
        pSt1.close();
        st.close();
        
        Connection conn2 = openConnection("SECONDDB2");
        conn2.setAutoCommit(false);
        Statement st2 = conn2.createStatement();
        st2.execute("create table SECONDDB_T1 (i int, j int, k int)");
        st2.execute("insert into SECONDDB_T1 values (2, 4, 6)");
        PreparedStatement pSt2 =
                conn2.prepareStatement("select * from SECONDDB_T1");
        
        rs1 = pSt2.executeQuery();
        rs1.next();
        rs1.close();
        pSt2.close();
        st2.close();
        
        JDBC.cleanup(conn1);
        JDBC.cleanup(conn2);
    }
    
    /* ------------------- end helper methods  -------------------------- */
    public DRDAProtocolTest(String name) {
        super(name);
    }
    
    public static Test suite() {
        Test test;
        test = TestConfiguration.clientServerSuite(DRDAProtocolTest.class);
        test = TestConfiguration.additionalDatabaseDecorator(test, "FIRSTDB1");
        test = TestConfiguration.additionalDatabaseDecorator(test, "SECONDDB2");
        return test;
    }
}
