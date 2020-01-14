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

import java.sql.Connection;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;

/**
 * Tests that certain operations can only be performed by the DBO.
 */
public class DBOAccessTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      JANET = "JANET";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, JANET };

    // Name of the log file to use when testing VTIs that expect one.
    private static final String TEST_LOG_FILE = "sys_vti_test_derby.tstlog";

    private static  final   String      ONLY_DBO = "4251D";
    private static  final   String      HIDDEN_COLUMN = "4251E";

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Create a new instance.
     */

    public DBOAccessTest( String name )
    {
        super( name );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OVERRIDABLE BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  boolean authorizationIsOn() { return true; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // NESTED CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   class   NoAuthorization extends DBOAccessTest
    {
        public NoAuthorization( String name )
        {
            super( name );
        }

        public  boolean authorizationIsOn() { return false; }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////


    /**
     * Construct top level suite in this JUnit test
     */
    public static Test suite()
    {
        String[]    testFiles = new String[] { "functionTests/tests/lang/" + TEST_LOG_FILE };
        
        Test    authorizedRun = TestConfiguration.embeddedSuite( DBOAccessTest.class );
        authorizedRun = DatabasePropertyTestSetup.builtinAuthentication
            ( authorizedRun, LEGAL_USERS, "authorizationOnDBOAccessTest" );
        authorizedRun = new SupportFilesSetup( authorizedRun, testFiles );
        authorizedRun = TestConfiguration.sqlAuthorizationDecorator( authorizedRun );

        Test    unauthorizedRun = TestConfiguration.embeddedSuite( NoAuthorization.class );
        unauthorizedRun = new SupportFilesSetup( unauthorizedRun, testFiles );

        TestSuite suite = new TestSuite();
        suite.addTest( authorizedRun );
        suite.addTest( unauthorizedRun );

        return suite;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Tests that only the DBO can run diagnostic VTIs which return sensitive information.
     * See DERBY-5395.
     * </p>
     */
    public  void    test_5395() throws Exception
    {
        println( "authorizationIsOn() = " + authorizationIsOn() );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        minion_5395( dboConnection, true );
        minion_5395( janetConnection, !authorizationIsOn() );
    }
    private void    minion_5395( Connection conn, boolean shouldSucceed ) throws Exception
    {
        vet_5395( conn, shouldSucceed, "select * from syscs_diag.statement_cache" );
        vet_5395( conn, shouldSucceed, "select * from syscs_diag.transaction_table" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.error_log_reader( ) ) s" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.statement_duration() ) s" );
        
        java.net.URL logURL = SupportFilesSetup.getReadOnlyURL( TEST_LOG_FILE );
        String vtiArg = "'" + logURL.getFile() + "'";

        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.error_log_reader( " + vtiArg + " ) ) s" );
        vet_5395( conn, shouldSucceed, "select * from table( syscs_diag.statement_duration( " + vtiArg + " ) ) s" );
    }
    private void    vet_5395( Connection conn, boolean shouldSucceed, String query ) throws Exception
    {
        if ( shouldSucceed ) { goodStatement( conn, query ); }
        else
        {
            expectCompilationError( conn, ONLY_DBO, query );
        }
    }

    /**
     * <p>
     * Tests that only the DBO can select from SYSUSERS and no-one can SELECT
     * SYSUSERS.PASSWORD.
     * </p>
     */
    public  void    testSYSUSERS() throws Exception
    {
        println( "testSYSUSERS authorizationIsOn() = " + authorizationIsOn() );
        
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  janetConnection = openUserConnection( JANET );

        goodStatement( dboConnection, "create view v2 as select username, hashingscheme, lastmodified from sys.sysusers" );
        if ( authorizationIsOn() ) { goodStatement( dboConnection, "grant select on v2 to public" ); }

        vetDBO_OKProbes( dboConnection, true );
        vetDBO_OKProbes( janetConnection, !authorizationIsOn() );

        vetUnauthorizedProbes( dboConnection, !authorizationIsOn(), HIDDEN_COLUMN );
        vetUnauthorizedProbes( janetConnection, !authorizationIsOn(), ONLY_DBO );
    }
    // these statements should always succeed if the dbo is running
    // them or if authorization is not enabled
    private void    vetDBO_OKProbes( Connection conn, boolean shouldSucceed )
        throws Exception
    {
        vetUserProbes( conn, shouldSucceed, "select count(*) from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, hashingscheme, lastmodified from sys.sysusers", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username from sys.sysusers", ONLY_DBO );

        // can't use views to subvert authorization checks
        vetUserProbes( conn, shouldSucceed, "select count(*) from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select * from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username, hashingscheme, lastmodified from test_dbo.v2", ONLY_DBO );
        vetUserProbes( conn, shouldSucceed, "select username from test_dbo.v2", ONLY_DBO );
    }
    // these statements should always fail if authorization is enabled
    private void    vetUnauthorizedProbes( Connection conn, boolean shouldSucceed, String expectedSQLState )
        throws Exception
    {
        vetUserProbes( conn, shouldSucceed, "select * from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select * from sys.sysusers where username='foo'", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select password from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select username, password from sys.sysusers", expectedSQLState );
        vetUserProbes( conn, shouldSucceed, "select username from sys.sysusers where password = 'foo'", expectedSQLState );
    }
    private void    vetUserProbes
        ( Connection conn, boolean shouldSucceed, String query, String expectedSQLState )
        throws Exception
    {
        if ( shouldSucceed ) { goodStatement( conn, query ); }
        else
        {
            expectCompilationError( conn, expectedSQLState, query );
        }
    }

}




