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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import junit.framework.Test;
import junit.framework.TestSuite;

import java.sql.*;
import java.io.IOException;

/**
 * Functional test for ansi trim functionality added for DERBY-1623.
 * 
 * @since May 6, 2007
 */
public class AnsiTrimTest extends BaseJDBCTestCase {

    /**
     * Create a test case with the given name.
     *
     * @param name of the test case.
     */
    public AnsiTrimTest(String name) {
        super(name);
    }

    /*
     * Factored out for reuse in other TestCases which add
     * the same test method in their suite() method.
     *
     * Currently done for a few testcases reused in replication testing:
     * o.a.dT.ft.tests.replicationTests.StandardTests.
     */
    public static void decorate(Statement s)
    throws SQLException
    {
        s.executeUpdate("create table tt (id int, v varchar(16), c char(16), cl clob(10240))");
        s.executeUpdate("insert into tt values (1, 'abcaca', 'abcaca', 'abcaca')");
        s.executeUpdate("create table nt (v varchar(2))");
        s.executeUpdate("insert into nt values (null)");
    }
   public static Test suite() {
        TestSuite suite = new TestSuite("AnsiTrimTest");
        suite.addTestSuite(AnsiTrimTest.class);
        return new CleanDatabaseTestSetup(suite) {
            public void decorateSQL(Statement s)
                    throws SQLException {
                decorate(s);
            }

        };
    }

    /**
     * trim a column with a constant trim char.
     */
    public void testColumnsWithConstant()
            throws SQLException {
        positiveTest("select trim(' ' from v) from tt where id = 1", "abcaca");
        positiveTest("select trim('a' from v) from tt where id = 1", "bcac");
        positiveTest("select trim(both 'a' from v) from tt where id = 1", "bcac");
        positiveTest("select trim(leading 'a' from v) from tt where id = 1", "bcaca");
        positiveTest("select trim(trailing 'a' from v) from tt where id = 1", "abcac");

        // chars are padded with spaces at the end.
        positiveTest("select trim(' ' from c) from tt where id = 1", "abcaca");
        positiveTest("select trim('a' from c) from tt where id = 1", "bcaca          ");
        positiveTest("select trim(both 'a' from c) from tt where id = 1", "bcaca          ");
        positiveTest("select trim(leading 'a' from c) from tt where id = 1", "bcaca          ");
        positiveTest("select trim(trailing 'a' from c) from tt where id = 1", "abcaca          ");

        //positiveTest("select trim(' ' from cl) from tt", "abcaca");
    }

    /**
     * Use a few expressions as the trim character.
     */
    public void testTrimCharIsExpr()
            throws SQLException {
        String expr;
        positiveTest(
                "SELECT count(*) FROM tt  " +
                "WHERE id = 1 AND (trim (leading substr(v,1,1) from v)) = 'bcaca'", new Integer(1));

        positiveTest(
                "select trim (both (case when length(v) = 6 then 'a' else 'b' end) from v) from tt",
                "bcac");

        positiveTest(
                "SELECT trim(TRAILING lcase(ucase('a')) from v) from tt", "abcac");
        
    }

    /**
     * A clob column is the input source.
     */
    public void testTrimFromClobColumn()
            throws SQLException, IOException {
        String sql = "SELECT trim('a' from cl) from tt";
        ResultSet rs = null;

        PreparedStatement ps = null;
        try {
            ps = prepareStatement(sql);
            rs = ps.executeQuery();
            // positiveTest does not deal with clobs.
            assertTrue(rs.next());
            Clob clob = rs.getClob(1);
            char[] cbuf = new char[128];
            assertEquals(4, clob.length());
            clob.getCharacterStream().read(cbuf);
            assertEquals("bcac", new String(cbuf, 0, 4));
            assertFalse(rs.next());
        } finally {
            if (rs != null) { try { rs.close(); } catch (SQLException e) {/* ignore */} }
            if (ps != null) { try { ps.close(); } catch (SQLException e) {/* ignore */} }
        }
    }
    
        
    /**
     * Use a few different expressions as the trim source.
     */
    public void testTrimSourceIsExpr()
            throws SQLException {
        positiveTest("SELECT trim(' ' from cast(v as char(7))) from tt", "abcaca");
        positiveTest("SELECT trim('a' from v||v) from tt", "bcacaabcac");
        positiveTest("SELECT trim('a' from ltrim(rtrim(c))) from tt", "bcac");        
    }

    /**
     * All the characters are trimmed.
     */
    public void testTrimResultIsEmpty()
            throws SQLException {
        positiveTest("select trim(' ' from '     ' ) from tt", "");
        positiveTest("select trim(LEADING ' ' from '     ' ) from tt", "");
        positiveTest("select trim(TRAILING ' ' from '     ' ) from tt", "");
        positiveTest("select trim(BOTH ' ' from '     ' ) from tt", "");
    }

    public void testSourceIsEmpty()
            throws SQLException {
        positiveTest("select trim(' ' from '') from tt", "");
        positiveTest("select trim(leading ' ' from '') from tt", "");
        positiveTest("select trim(trailing ' ' from '') from tt", "");
        positiveTest("select trim(both ' ' from '') from tt", "");
    }

    public void testSourceIsNull()
            throws SQLException {
        positiveTest("select trim(' ' from v) from nt", null);
        positiveTest("select trim(leading ' ' from v) from nt", null);
        positiveTest("select trim(trailing ' ' from v) from nt", null);
        positiveTest("select trim(both ' ' from v) from nt", null);                
    }

    public void testSourceIsSingleChar()
            throws SQLException {
        positiveTest("select trim(' ' from 'a') from nt", "a");
        positiveTest("select trim(leading ' ' from 'a') from nt", "a");
        positiveTest("select trim(trailing ' ' from 'a') from nt", "a");
        positiveTest("select trim(both ' ' from 'a') from nt", "a");        

        positiveTest("select trim('a' from 'a') from nt", "");
        positiveTest("select trim(leading 'a' from 'a') from nt", "");
        positiveTest("select trim(trailing 'a' from 'a') from nt", "");
        positiveTest("select trim(both 'a' from 'a') from nt", "");                
    }

    public void testCharIsNull() throws SQLException {
        positiveTest("select trim ((values cast (null as char(1))) from v) from tt", null);
    }

    private void positiveTest(String sql, Object expected)
            throws SQLException {
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            ps = prepareStatement(sql);
            rs = ps.executeQuery();
            JDBC.assertFullResultSet(rs, new Object[][] {{expected}}, false, /*closeResultSet=*/true);
        } finally {
            // assertFullResultSet closes rs.
            if (ps != null) { ps.close(); }
        }
    }
}


