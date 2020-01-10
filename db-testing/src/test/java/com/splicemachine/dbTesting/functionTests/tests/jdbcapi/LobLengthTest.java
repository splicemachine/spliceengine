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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.PreparedStatement;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetStream;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This tests a fix for a defect (DERBY-121) where the server and
 * client were processing lob lengths incorrectly.  For lob lengths
 * that are represented by 24 or more bits, the server and Derby
 * client were doing incorrect bit-shifting.  This test makes sure
 * that problem no longer occurs.
 */

public class LobLengthTest extends BaseJDBCTestCase {

    /**
     * Creates a new instance of LobLengthTest
     *
     * @param name name of the test.
     */
    public LobLengthTest(String name)
    {
        super(name);
    }

    
    public static Test suite() 
    {
        return TestConfiguration.defaultSuite(LobLengthTest.class);
    }


    /**
     * Create a JDBC connection using the arguments passed
     * in from the harness, and create the table to be used by the test.
     */
    public void setUp() throws Exception
    {
        getConnection().setAutoCommit(false);

        // Create a test table.
        Statement st = createStatement();
        st.execute("create table lobTable100M(bl blob(100M))");
        st.close();
    }


    /**
     * Cleanup: Drop table and close connection.
     */
    public void tearDown() throws Exception 
    {
        Statement st = createStatement();
        st.execute("drop table lobTable100M");
        st.close();

        commit();
        super.tearDown();
    }


    /**
     * There was a defect (DERBY-121) where the server and client
     * were processing lob lengths incorrectly.  For lob lengths
     * that are represented by 24 or more bits, the server and
     * Derby client were doing incorrect bit-shifting.  This
     * test makes sure that problem no longer occurs.
     */
    public void testLongLobLengths() throws Exception
    {
        PreparedStatement pSt = prepareStatement(
            "insert into lobTable100M(bl) values (?)");

        // The error we're testing occurs when the server
        // is shifting bits 24 and higher of the lob's
        // length (in bytes).  This means that, in order
        // to check for the error, we have to specify a
        // lob length (in bytes) that requires at least
        // 24 bits to represent.  Thus for a blob the
        // length of the test data must be specified as
        // at least 2^24 bytes (hence the '16800000' in
        // the next line).
        int lobSize = 16800000;
        pSt.setBinaryStream(1,
            new LoopingAlphabetStream(lobSize), lobSize);

        // Now try the insert; this is where the server processes
        // the lob length.
        pSt.execute();
        pSt.close();
    }
}
