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

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Set of tests that ensure SQL routines implemented in Java are
 * correctly restricted in their actions when a security manager
 * is in place. Assumption is that the standard running of this
 * test will setup a security manager.
 *
 */
public class RoutineSecurityTest extends BaseJDBCTestCase {
    
    public RoutineSecurityTest(String name)
    {
        super(name);
    }
    
    /**
     * Test only runs in embedded as it is testing server
     * side SQL routines implemented in Java.
     */
    public static Test suite()
    {
        Test suite = TestConfiguration.embeddedSuite(RoutineSecurityTest.class);
        
        // Create all the routines we need up front.
        return new CleanDatabaseTestSetup(suite)
            {
              protected void decorateSQL(Statement s) throws SQLException {
                  s.executeUpdate(
                    "CREATE FUNCTION GET_SYS_PROP(PROPERTY_KEY VARCHAR(60)) " +
                    "RETURNS VARCHAR(255) " +
                    "EXTERNAL NAME 'java.lang.System.getProperty' " +
                    "LANGUAGE JAVA PARAMETER STYLE JAVA");
                  
                  s.executeUpdate(
                    "CREATE PROCEDURE DENIAL_OF_SERVICE(RC INT) " +
                    "EXTERNAL NAME 'java.lang.System.exit' " +
                    "LANGUAGE JAVA PARAMETER STYLE JAVA");
                  
                  s.executeUpdate(
                     "CREATE PROCEDURE FORCEGC() " +
                     "EXTERNAL NAME 'java.lang.System.gc' " +
                     "LANGUAGE JAVA PARAMETER STYLE JAVA");

               }
             };
    }
    
    /**
     * Test obtaining a system property using the Java library
     * method System.getProperty() directly. Note that since
     * the system method is called directly there is no
     * privilege block and so to read a property the permission
     * must have been granted all the way up the stack *including*
     * the generated class. This can only occur for a generic
     * grant entry in the policy file (with no code URL). 
     * 
     * @throws SQLException
     */
    public void testGetSystemProperty() throws SQLException
    {
        PreparedStatement ps = prepareStatement("VALUES GET_SYS_PROP(?)");
        
        String[] restricted = {
                "derby.system.home", // not granted to all code on the stack
                // DERBY-5514: Property read permission on user.dir is granted
                // to all code bases when running code instrumented with EMMA,
                // so reading the property will succeed under EMMA.
                // "user.dir",  // restricted by jvm
                "user.home",  // restricted by jvm
                "java.class.path", // restricted by jvm
                "java.home",  // restricted by jvm
                "derbyRoutineSecurityTest.no", // not granted at all
                "derbyTesting.fred" // only granted to derbyTesting.jar
                };
        
        for (int i = 0; i < restricted.length; i++)
        {
            ps.setString(1, restricted[i]);
            try {
                ResultSet rs =ps.executeQuery();
                rs.next(); 
                fail("Succeeded reading " + restricted[i] + rs.getString(1));
            } catch (SQLException e) {
                assertSecurityException(e);
            }
        }
        
        // Should be ok to read these unrestricted or
        // granted_to_all_code properties.
        String[] notRestrictedAndGranted = {
           "java.version", // open to all readers
           "java.specification.name", // open to all readers
           "derbyRoutineSecurityTest.yes" // granted to all code in the policy file
        };
        for (int i = 0; i < notRestrictedAndGranted.length; i++)
        {
            ps.setString(1, notRestrictedAndGranted[i]);
            ResultSet rs =ps.executeQuery();
            rs.next(); 
            rs.getString(1);
            rs.close();
        }
        ps.close();
    }
    
    /**
     * Check that System.exit() cannot be called directly from a procedure.
     * @throws SQLException
     */
    public void testSystemExit() throws SQLException
    {
        CallableStatement cs = prepareCall("CALL DENIAL_OF_SERVICE(?)");
        
        cs.setInt(1, -1);
        try {
            cs.executeUpdate();
            fail("Tough to get here since exit would have been called.");
        } catch (SQLException e) {
            assertSecurityException(e);
        }
        cs.setInt(1, 0);
        try {
            cs.executeUpdate();
            fail("Tough to get here since exit would have been called.");
        } catch (SQLException e) {
            assertSecurityException(e);
        }
        cs.close();
    }
    /**
     * Check that System.gc() can be called directly from a procedure.
     * @throws SQLException
     */
    public void testSystemGC() throws SQLException
    {
        CallableStatement cs = prepareCall("CALL FORCEGC()");
        cs.executeUpdate();
        cs.close();
    }
    
    /**
     * Check that a user routine cannot resolve to a
     * internal db class, currently limited to not
     * resolving to any class in the com.splicemachine.db namespace.
     */
    public void testInternalClass() throws SQLException
    {
        Statement s = createStatement();
        
        s.executeUpdate(
                "CREATE FUNCTION HACK_DERBY(PROPERTY_KEY VARCHAR(60)) " +
                "RETURNS VARCHAR(60) " +
                "EXTERNAL NAME 'com.splicemachine.db.catalog.SystemProcedures.SYSCS_GET_DATABASE_PROPERTY' " +
                "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        s.executeUpdate(
                "CREATE PROCEDURE HACK_DERBY_2() " +
                "EXTERNAL NAME 'com.splicemachine.db.catalog.SystemProcedures.SYSCS_UNFREEZE_DATABASE' " +
                "LANGUAGE JAVA PARAMETER STYLE JAVA");
        
        // Some random potential Derby class to ensure the checks
        // are not limited to the catalog class.
        s.executeUpdate(
                "CREATE PROCEDURE HACK_DERBY_3() " +
                "EXTERNAL NAME 'com.splicemachine.db.any.clazz.method' " +
                "LANGUAGE JAVA PARAMETER STYLE JAVA");


        s.close();
        
        assertCompileError("42X51", "VALUES HACK_DERBY(?)");
        assertCompileError("42X51", "CALL HACK_DERBY_2()");
        assertCompileError("42X51", "CALL HACK_DERBY_3()");       

    }
    
    /**
     * Test for a security exception within a routine.
     * Current test is that the SQLException returned
     * to the client has SQLState 38000 and wraps a
     * SQLException with SQLState XJ001 which corresponds
     * to wrapped Java exception.
     * @param e
     */
    private void assertSecurityException(SQLException e)
    {
        assertSQLState("38000", e);
        e = e.getNextException();
        assertNotNull(e);
        assertSQLState("XJ001", e);
}

}
