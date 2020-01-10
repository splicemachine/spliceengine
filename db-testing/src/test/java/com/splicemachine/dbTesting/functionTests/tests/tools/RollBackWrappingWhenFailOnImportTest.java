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

import java.sql.CallableStatement;


import java.sql.Connection;

import java.sql.SQLException;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedConnection30;
import com.splicemachine.db.jdbc.Driver30;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This test case comes from DERBY-4443. It's for show changes related to 
 * wrap rollback in exception handlers in try-catch.
 * In this test case, a MockInternalDriver is used to create a MockConnectionFailWhenRollBack
 * which will fail when rollback() is called.
 * 
 */
public class RollBackWrappingWhenFailOnImportTest extends BaseJDBCTestCase {
    class MockInternalDriver extends Driver30 {

        public class MockConnectionFailWhenRollBack extends EmbedConnection30 {

            public MockConnectionFailWhenRollBack(Connection connection) {
                super((EmbedConnection)connection);
            }

            public void rollback() throws SQLException {
                throw new SQLException("error in roll back", "XJ058");
            }
        }

        public Connection connect(String url, Properties info) {
            Connection conn = null;
            try {
                conn = super.connect(url, info);
            } catch (Exception e) {
                //this exception is ignored for mocking
            }
            return new MockConnectionFailWhenRollBack(conn);
        }
    }

    private String nonexistentFileName = "test/test.dat";

    public RollBackWrappingWhenFailOnImportTest(String name) {
        super(name);        
    }
    
    public static Test suite() {
        TestSuite suite = new TestSuite("RollBackWrappingWhenFailOnImportTest");
        
        if (!JDBC.vmSupportsJDBC3()) {
            return suite;
        }       
        
        Test test = new CleanDatabaseTestSetup(
                TestConfiguration.embeddedSuite(
                        RollBackWrappingWhenFailOnImportTest.class));
                        
        suite.addTest(test);
        
        return suite;
    }

    protected void setUp() throws Exception {
        openDefaultConnection();
        
        MockInternalDriver dvr = new MockInternalDriver();
        dvr.boot(false, null);
        
        SupportFilesSetup.deleteFile(nonexistentFileName);
    }
    
    protected void tearDown() throws Exception {        
        try {           
            getTestConfiguration().shutdownEngine();            
        } catch (Exception e) {
            //Ignore exception for shut down mock driver            
        }        
        
        super.tearDown();
    }

    public void testRollBackWhenFailOnImportTable() throws SQLException { 
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE (" +
                "null, 'IMP_EMP', '"  + nonexistentFileName + "test/test.dat" + 
                "' , null, null, null, 0) ";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    public void testRollBackWhenFailOnImportTableLobsFromEXTFile() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_TABLE_LOBS_FROM_EXTFILE(" +
                "null, 'IET1' , '" + nonexistentFileName + "', null, null, null, 0)";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    public void testRollBackWhenFailOnImportData() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_DATA(null, 'IMP_EMP', " +
                "null, null, '" + nonexistentFileName +  "', null, null, null, 1) ";
        realTestRollBackWhenImportOnNonexistentFile(callSentence);        
    }  
    
    public void testRollBackWhenFailOnImportDataLobsFromExtFile() throws SQLException {
        String callSentence = "call SYSCS_UTIL.SYSCS_IMPORT_DATA_LOBS_FROM_EXTFILE(" +
                "null, 'IET1', null, null, '" + nonexistentFileName +
                "', null, null, null, 1)";
        
        realTestRollBackWhenImportOnNonexistentFile(callSentence);
    }
    
    /**
     * Call passed importSentence and process the error.
     * @param importSentence a call sentence to to import data from a nonexistent file.
     */
    private void realTestRollBackWhenImportOnNonexistentFile(
            String importSentence) throws SQLException {
      //import a non-existing file will certainly fail
        CallableStatement cSt = prepareCall(importSentence);
        
        try {
            cSt.executeUpdate();
            fail("a SQLException should be thrown " +
                    "as we import data from a nonexistent file");
        } catch (SQLException e) {            
            assertSQLState("XIE0M", e);
            assertSQLState("XJ058", e.getNextException());            
        } finally {
            cSt.close();
        }
    }
}
