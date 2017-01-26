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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.functionTests.util.SQLStateConstants;

public class SQLAuthorizationPropTest extends BaseJDBCTestCase {

	public SQLAuthorizationPropTest(String name) {
		super(name);
	}

	public static Test suite() {
		TestSuite suite = new TestSuite(SQLAuthorizationPropTest.class,
                "SQLAuthorizationPropTest");
		
		// Use DatabasePropertyTestSetup decorator to set the property
		// required by this test (and shutdown the database for the
        // property to take effect.
		Properties props = new Properties();
	    props.setProperty("derby.database.sqlAuthorization", "true");
	    Test test = new SQLAuthorizationPropTest("grantRevokeAfterSettingSQLAuthProperty");
	    suite.addTest(new DatabasePropertyTestSetup (test, props, true));
	    
	    // This test has to be run after SQL authorization property has been 
	    // set to true. 
	    suite.addTest(new SQLAuthorizationPropTest("resetSQLAuthProperty"));
	    
        // This test needs to run in a new single use database as upon entry
        // the test expects SQL authorization to be off and then sets it
        // which cannot be undone.
	    return TestConfiguration.singleUseDatabaseDecorator(suite);
	}
	
    /**
     * Create a table to test grant/revoke statements
     */
    protected void setUp() throws SQLException {
        Statement stmt = createStatement();
        stmt.execute("create table GR_TAB (id int)");
        stmt.close();
    }

    /**
     * Drop the table created during setup.
     * @throws Exception 
     */
    protected void tearDown()
        throws Exception {
        Statement stmt = createStatement();
        stmt.execute("drop table GR_TAB");
        stmt.close();
        super.tearDown();
    }
    
	/**
	 * This method tests that grant/revoke is not available if 
	 * db.database.sqlAuthorization property is not set.
	 * 
	 * @throws SQLException
	 */
	public void testGrantRevokeWithoutSQLAuthProperty() throws SQLException{
		Statement stmt = createStatement();
		
    	try {
    		stmt.execute("grant select on GR_TAB to some_user");
    		fail("FAIL: Grant statement should have failed when SQL authorization is not set");
    	} catch(SQLException sqle) {
    		assertSQLState(SQLStateConstants.LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS, sqle);
    	}
    	
    	try {
    		stmt.execute("revoke select on GR_TAB from some_user");
    		fail("FAIL: Revoke statement should have failed when SQL authorization is not set");
    	} catch(SQLException sqle) {
    		assertSQLState(SQLStateConstants.LANG_GRANT_REVOKE_WITH_LEGACY_ACCESS, sqle);
    	}
    	stmt.close();
	}
	
	/**
	 * This method tests that grant/revoke is available 
	 * once db.database.sqlAuthorization property is set to true.
	 * 
	 * @throws SQLException
	 */
	public void grantRevokeAfterSettingSQLAuthProperty() throws SQLException{
		
		Statement stmt = createStatement();
		stmt.execute("grant select on GR_TAB to some_user");
    	stmt.execute("revoke select on GR_TAB from some_user");
    	stmt.close();
	}
	
	/**
	 * This method tests that once db.database.sqlAuthorization property
	 * has been set to true, it cannot be reset to any other value. For the 
	 * test to be valid, it must follow the test method which sets 
	 * db.database.sqlAuthorization property to true.
	 * 
	 * @throws SQLException
	 */
	public void resetSQLAuthProperty() throws SQLException {
		Connection conn = getConnection();
        conn.setAutoCommit(false);
        
        CallableStatement setDBP =  conn.prepareCall(
            "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, "derby.database.sqlAuthorization");
        // Resetting to any value other than true should fail
        testPropertyReset(setDBP, "false");
        testPropertyReset(setDBP, null);
        testPropertyReset(setDBP, "some_value");
        // This should work
        testPropertyReset(setDBP, "true");
        
        setDBP.close();
	}
	
	/**
	 * This method executes a callable statement to set the database property
	 * to a given value. It checks that reset to any value other than "true" 
	 * fails.
	 * 
	 * @param cs CallableStatement object used to set database property
	 * @param value value of database property
	 * @throws SQLException
	 */
	private void testPropertyReset(CallableStatement cs, String value) throws SQLException {

		cs.setString(2, value);
        
		try {
        	cs.executeUpdate();
        	if(value.compareToIgnoreCase("true") != 0)
        		fail("FAIL: Should not be possible to reset sql authorization once it has been turned on");
        } catch (SQLException sqle) {
        	assertSQLState(SQLStateConstants.PROPERTY_UNSUPPORTED_CHANGE, sqle);
        }
        
	}

	/**
	 * Verify that you can't make the database unbootable by changing
     * the database version. See DERBY-5838.
	 */
	public void test_5838() throws Exception
    {
        Statement stmt = createStatement();

        assertStatementError
            (
             "XCY02",
             stmt,
             "call syscs_util.syscs_set_database_property( 'DataDictionaryVersion', 'foobar' )"
             );
    }
    
}
