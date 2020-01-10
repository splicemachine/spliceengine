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

package com.splicemachine.dbTesting.functionTests.tests.tools;

import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.Derby;

/**
 * Test the ij.runScript api method.
 *
 */
public class IJRunScriptTest extends BaseJDBCTestCase {
    
    public static Test suite()
    {
        if (Derby.hasTools()) {
            TestSuite suite = new TestSuite("IJRunScriptTest");
            suite.addTestSuite(IJRunScriptTest.class);
            return new CleanDatabaseTestSetup(suite);
        }
        
        return new TestSuite("empty: no tools support");
    }
    
    public IJRunScriptTest(String name)
    {
        super(name);
    }
    
    /**
     * Test execution of scripts by executing them and
     * seeing if the object exists.
     * @throws SQLException
     * @throws UnsupportedEncodingException 
     */
    public void testScriptExecution()
        throws SQLException, UnsupportedEncodingException
    {       
        runTestingScript("CREATE TABLE T1(I INT);\nCREATE TABLE T2(I INT)", 0);
        
        // Check they exist by inserting rows.
        
        Statement s = createStatement();
        
        // Insert two rows into the first table
        assertEquals(2, s.executeUpdate("INSERT INTO T1 VALUES 1,2"));

        // Insert three rows into the second table
        assertEquals(3, s.executeUpdate("INSERT INTO T2 VALUES 1,2,4"));
        
        runTestingScript("DROP TABLE T1;DROP TABLE T2", 0);
               
        s.close();
    }

    /**
     * Test support for nested bracketed comments.
     * @throws SQLException
     * @throws UnsupportedEncodingException
     */
    public void testBracketedComment()
        throws SQLException, UnsupportedEncodingException
    {
        runTestingScript("VALUES /* comment /* nested comment */ 5; */ 1;", 0);
        runTestingScript("/* just a comment */", 0);
        runTestingScript("/* a /* nested */ comment */", 0);
    }

    /**
     * Test execution an empty script.
     * @throws SQLException
     * @throws UnsupportedEncodingException 
     */
    public void testEmptyScript()
        throws SQLException, UnsupportedEncodingException
    {       
        runTestingScript("", 0);
    }   

    /**
     * Test execution of the IJ AUTOCOMMIT statement.
     * @throws SQLException
     * @throws UnsupportedEncodingException 
     */
    public void testAutoCommitCommand()
        throws SQLException, UnsupportedEncodingException
    {      
        Connection conn = getConnection();
        assertTrue(conn.getAutoCommit());
        runTestingScript("AUTOCOMMIT OFF;", 0);
        
        assertFalse(conn.isClosed());
        assertFalse(conn.getAutoCommit());
    }
    
    /**
     * Test error counting.
     * @throws SQLException
     * @throws UnsupportedEncodingException 
     */
    public void testErrorsCount()
        throws SQLException, UnsupportedEncodingException
    {      
       // just a single error
       runTestingScript("CREATE TAAABLE T (I INT);", 1);
       runTestingScript("INSERT INTO TIJ VALUES 1;", 1);

       // two errors
       runTestingScript("INSERT INTO TIJ VALUES 1;\nDELETE FROM SYS.SYSTABLES", 2);
       runTestingScript("INSERT INTO TIJ VALUES 1;DELETE FROM SYS.SYSTABLES", 2);
       
       // mixture of errors (two in all)
       runTestingScript("CREATX TABLE TIJME(I INT);CREATE TABLE TIJME(I INT);" +
               "INSERT INTO TIJME VALUES 1,3,4;" +
               "INSERT INTO TIJME VALUESS 1,3,4;" +
               "DROP TABLE TIJME"
               , 2);
       
   }
        

    /**
     * Run a test script using the passed in String as the source
     * for the script. Script is run using the UTF-8 encoding and
     * the output discarded.
     * @param script
     * @throws UnsupportedEncodingException
     * @throws SQLException
     */
    private void runTestingScript(String script, int expectedErrorCount)
        throws UnsupportedEncodingException, SQLException
    {       
        int errorCount = runSQLCommands(script);
        assertEquals("Error count on " + script,
                expectedErrorCount, errorCount );
    }
    
}
