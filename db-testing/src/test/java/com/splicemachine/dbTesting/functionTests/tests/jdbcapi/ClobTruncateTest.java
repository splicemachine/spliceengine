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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Clob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import junit.framework.Test;
import com.splicemachine.dbTesting.functionTests.util.streams.CharAlphabet;
import com.splicemachine.dbTesting.functionTests.util.streams.LoopingAlphabetReader;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test case for Clob.truncate
 */
public class ClobTruncateTest extends BaseJDBCTestCase {

    public ClobTruncateTest (String name) {
        super (name);
    }

    private void insertClobs () throws SQLException, IOException {
        PreparedStatement ps = prepareStatement (
                "insert into truncateclob" +
                " values (?,?,?)");
        //insert a small clob
        StringBuffer sb = new StringBuffer ();
        for (int i = 0; i < 100; i++)
            sb.append ("small clob");
        int length = sb.length();
        ps.setInt (1, length);
        ps.setCharacterStream (2, new StringReader (sb.toString()), length);
        ps.setInt (3, length/2);
        ps.execute();

        //insert a large clob
        LoopingAlphabetReader reader = new LoopingAlphabetReader (1024 * 1024);
        ps.setInt (1, 1024 * 1024);
        ps.setCharacterStream (2, reader, 1024 * 1024);
        ps.setInt (3, 1024 * 1024 / 2);
        ps.execute();

        //insert a non ascii clob
        LoopingAlphabetReader uReader =
                new LoopingAlphabetReader (300000, CharAlphabet.tamil());
        ps.setInt (1, 300000);
        ps.setCharacterStream (2, uReader, 300000);
        ps.setInt (3, 150000);
        ps.execute();
    }

    private void checkTruncate (int size, Clob clob, int newSize)
            throws SQLException {
        assertEquals ("unexpected clob size", size, clob.length());
        clob.truncate (newSize);
        assertEquals ("truncate failed ", newSize, clob.length());
        //try once more
        clob.truncate (newSize/2);
        assertEquals ("truncate failed ", newSize/2, clob.length());
    }

    public void testTruncateOnClob () throws SQLException, IOException {
        insertClobs();
        getConnection().setAutoCommit (false);
        ResultSet rs = createStatement().executeQuery("select size, data," +
                " newsize from truncateclob");
        try {
            while (rs.next()) {
                checkTruncate (rs.getInt (1), rs.getClob(2), rs.getInt(3));
            }
        }
        finally {
            rs.close();
            getConnection().commit();
        }
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate ("drop table truncateclob");
        super.tearDown();
    }

    protected void setUp() throws Exception {
        super.setUp();
        Statement stmt = createStatement();
        stmt.executeUpdate ("create table truncateclob " +
                "(size integer, data clob, newSize integer)");
    }

    public static Test suite() {
        //client code is caching clob length so this test will fail
        return TestConfiguration.embeddedSuite(ClobTruncateTest.class);
    }
}
