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

import java.sql.Connection;

import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;

/**
 * <p>
 * Test permissions on sequences. See DERBY-712.
 * </p>
 */
public class SequencePermsTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";
    private static  final   String      RUTH = "RUTH";
    private static  final   String      ALICE = "ALICE";
    private static  final   String      FRANK = "FRANK";
    private static  final   String      IRMA = "IRMA";
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK, IRMA  };

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

    public SequencePermsTest(String name)
    {
        super(name);
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(SequencePermsTest.class);

        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "sequencePermissions" );
        Test        authorizedTest = TestConfiguration.sqlAuthorizationDecorator( authenticatedTest );

        return authorizedTest;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that you need USAGE privilege on a sequence in order to issue a NEXT VALUE FOR
     * on it and in order to declare objects which mention that type.
     * </p>
     */
    public  void    test_001_basicGrant()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );
        Connection  frankConnection = openUserConnection( FRANK );

        //
        // Create a sequence and view. Make the view
        // public. Verify that it is still not generally usable because the
        // sequence is not public yet.
        //
        goodStatement
            (
             ruthConnection,
             "create sequence seq_01\n"
             );
        goodStatement
            (
             ruthConnection,
             "create table t_01( c int )\n"
             );
        goodStatement
            (
             ruthConnection,
             "insert into t_01( c ) values ( 1 )\n"
             );
        goodStatement
            (
             ruthConnection,
             "create view v_01( a, b ) as select c, next value for seq_01 from t_01\n"
             );
        goodStatement
            (
             ruthConnection,
             "grant select on v_01 to alice\n"
             );

        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "values ( next value for ruth.seq_01 )\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_COLUMN_PRIV,
             "select * from ruth.t_01\n"
             );

        // but this succeeds because of definer's rights on the view
        goodStatement
            (
             ruthConnection,
             "select * from ruth.v_01\n"
             );

        //
        // The DBO however is almighty.
        //
        goodStatement
            (
             ruthConnection,
             "values ( next value for ruth.seq_01 )\n"
             );

        //
        // Now grant USAGE on the sequence. User Alice should now have all the
        // privileges she needs.
        //
        goodStatement
            (
             ruthConnection,
             "grant usage on sequence seq_01 to alice\n"
             );
        goodStatement
            (
             aliceConnection,
             "values( next value for ruth.seq_01 )\n"
             );

    }
    
    /**
     * <p>
     * Test that you need USAGE privilege on a sequence in order to issue a NEXT VALUE FOR
     * on it the privilege can't be revoked while the object still exists.
     * </p>
     */
    public  void    test_002_basicRevoke()
        throws Exception
    {
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  frankConnection = openUserConnection( FRANK );
        
        goodStatement
            (
             ruthConnection,
             "create sequence seq_02\n"
             );
        goodStatement
            (
             frankConnection,
             "create table t_01( c int )\n"
             );
        expectExecutionError
            (
             frankConnection,
             LACK_USAGE_PRIV,
             "values ( next value for ruth.seq_02 )\n"
             );

        //
        // Only RESTRICTed revokes allowed.
        //
        goodStatement
            (
             ruthConnection,
             "grant usage on sequence seq_02 to public\n"
             );
        expectCompilationError( ruthConnection, SYNTAX_ERROR, "revoke usage on sequence seq_02 from public\n" );
        goodStatement
            (
             ruthConnection,
             "revoke usage on sequence seq_02 from public restrict\n"
             );

        //
        // Now test revokes when objects depend on the sequence.
        //
        
        String grantUsage = "grant usage on sequence seq_02 to frank\n";
        String revokeUsage = "revoke usage on sequence seq_02 from frank restrict\n";
        String createStatement;
        String dropStatement;
        String badRevokeSQLState;
        
        // view
        createStatement = "create view v_01( a, b ) as select c, next value for ruth.seq_02 from t_01\n";
        dropStatement = "drop view v_01\n";
        badRevokeSQLState = VIEW_DEPENDENCY;
        verifyRevokePrivilege
            (
             ruthConnection,
             frankConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // trigger
        createStatement = "create trigger trig_01 after update on t_01 for each statement insert into t_01( c ) values ( next value for ruth.seq_02 )\n";
        dropStatement = "drop trigger trig_01\n";
        badRevokeSQLState = OPERATION_FORBIDDEN;
        verifyRevokePrivilege
            (
             ruthConnection,
             frankConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // constraint
        //
        // no longer possible because syntax is illegal. see DERBY-4513
        //
        //        createStatement = "create table t_02( c int check ( ( next value for ruth.seq_02 ) < c ) )\n";
        //        dropStatement = "drop table t_02\n";
        //        badRevokeSQLState = OPERATION_FORBIDDEN;
        //        verifyRevokePrivilege
        //            (
        //             ruthConnection,
        //             frankConnection,
        //             grantUsage,
        //             revokeUsage,
        //             createStatement,
        //             dropStatement,
        //             badRevokeSQLState
        //             );

        
    }

  /**
     * <p>
     * Test that you can't drop a schema if it contains a sequence.
     * </p>
     */
    public  void    test_003_dropSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  irmaConnection = openUserConnection( IRMA );

        goodStatement
            ( irmaConnection, "create sequence seq_01\n" );
        expectExecutionError( dboConnection, NON_EMPTY_SCHEMA, "drop schema irma restrict\n" );

        goodStatement
            (irmaConnection, "drop sequence seq_01 restrict\n" );
       goodStatement
            ( dboConnection, "drop schema irma restrict\n" );
    }

}
