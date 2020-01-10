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
package com.splicemachine.dbTesting.functionTests.suites;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.splicemachine.dbTesting.functionTests.tests.store.AccessTest;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.Decorator;
import com.splicemachine.dbTesting.junit.JDBC;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * A suite that runs a set of tests using encrypted
 * databases with a number of algorithms.
 * This is a general encryption test to see if
 * tests run without any problems when encryption
 * is enabled.
 * <BR>
 * It is not for testing of encryption functionality,
 * e.g. testing that bootPassword must be a certain
 * length etc. That should be in a specific JUnit test
 * that probably needs to control database creation
 * more carefully than this.
 * <BR>
 * The same set of tests is run for each algorithm,
 * and each algorithm (obviously) uses a single
 * use database with the required encryption setup.
 * 
 * @see Decorator#encryptedDatabase(Test)
 * @see Decorator#encryptedDatabase(Test, String)
 *
 */
public final class EncryptionSuite extends BaseJDBCTestCase {
    

    public EncryptionSuite(String name) {
        super(name);
    }
    
    /**
     * Runs tests with a set of encryption algorithms.
     * The set comes from the set of algorithms used
     * for the same purpose in the old harness.
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("Encrpytion Suite");
        
        // Encryption only supported for Derby in J2SE/J2EE environments.
        // J2ME (JSR169) does not support encryption.
        if (JDBC.vmSupportsJDBC3()) {
        
          suite.addTest(Decorator.encryptedDatabase(baseSuite("default")));
          suite.addTest(encryptedSuite("AES/CBC/NoPadding"));
          suite.addTest(encryptedSuite("DES/ECB/NoPadding"));
          suite.addTest(encryptedSuite("DESede/CFB/NoPadding"));
          suite.addTest(encryptedSuite("DES/CBC/NoPadding"));
          suite.addTest(encryptedSuite("Blowfish/CBC/NoPadding"));
          suite.addTest(encryptedSuite("AES/OFB/NoPadding"));
        }
        
        return suite;
    }
    
    private static Test encryptedSuite(String algorithm)
    {
        return Decorator.encryptedDatabase(baseSuite(algorithm), algorithm);
    }
    
    /**
     * Set of tests which are run for each encryption algorithm.
     */
    private static Test baseSuite(String algorithm)
    {
        TestSuite suite = new TestSuite("Encryption Algorithm: " + algorithm);
        
        // Very simple test to get the setup working while we have
        // no tests that were previously run under encryption converted.
        suite.addTestSuite(EncryptionSuite.class);
        
        Properties sysProps = new Properties();
        sysProps.put("derby.optimizer.optimizeJoinOrder", "false");
        sysProps.put("derby.optimizer.ruleBasedOptimization", "true");
        sysProps.put("derby.optimizer.noTimeout", "true");
        
        suite.addTestSuite(AccessTest.class);
        
        return suite;
    }
    
    protected void setUp() {
        
        try { 
                Connection conn = getConnection();
                Statement s = createStatement();

                s.execute("CREATE FUNCTION  PADSTRING (DATA VARCHAR(32000), "
                        + "LENGTH INTEGER) RETURNS VARCHAR(32000) EXTERNAL NAME " +
                        "'com.splicemachine.dbTesting.functionTests.util.Formatters" +
                ".padString' LANGUAGE JAVA PARAMETER STYLE JAVA");
                s.close();
                conn.close();

        } catch (SQLException se) {
            // ignore
        }
    }
    
    public void tearDown() throws Exception {
        Statement st = createStatement();
        super.tearDown();
        try {
            st.executeUpdate("DROP FUNCTION PADSTRING");
        } catch (SQLException e) {
            // never mind.
        }
    }
    
    /**
     * Very simple test that ensures we can get a connection to
     * the booted encrypted database.
     * @throws SQLException
     */
    public void testEncryptedDBConnection() throws SQLException
    {
        getConnection().close();
    }
    
    
}
