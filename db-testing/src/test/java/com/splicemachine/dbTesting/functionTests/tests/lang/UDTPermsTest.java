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
 * Test permissions on UDTs. See DERBY-651.
 * </p>
 */
public class UDTPermsTest extends GeneratedColumnsHelper
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
    private static  final   String[]    LEGAL_USERS = { TEST_DBO, ALICE, RUTH, FRANK  };

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

    public UDTPermsTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(UDTPermsTest.class);

        Test        cleanTest = new CleanDatabaseTestSetup( suite );
        Test        authenticatedTest = DatabasePropertyTestSetup.builtinAuthentication
            ( cleanTest, LEGAL_USERS, "udtPermissions" );
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
     * Test that you need USAGE privilege on a type in order to select columns
     * of that type and in order to declare objects which mention that type.
     * </p>
     */
    public  void    test_001_basicGrant()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        //
        // Create a type, function, and table. make the function and table
        // public. Verify that they are still not generally usable because the
        // type is not public yet.
        //
        goodStatement
            (
             ruthConnection,
             "create type price_ruth_01_a external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price' language java\n"
             );
        goodStatement
            (
             ruthConnection,
             "create function makePrice_ruth_01( )\n" +
             "returns price_ruth_01_a\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price.makePrice'\n"
             );
        goodStatement
            (
             ruthConnection,
             "create table t_ruth_01( a price_ruth_01_a )\n"
             );
        goodStatement
            (
             ruthConnection,
             "insert into t_ruth_01( a ) values ( makePrice_ruth_01() )\n"
             );
        goodStatement
            (
             ruthConnection,
             "grant select on t_ruth_01 to public\n"
             );
        goodStatement
            (
             ruthConnection,
             "grant execute on function makePrice_ruth_01 to public\n"
             );

        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "select * from ruth.t_ruth_01\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "values( ruth.makePrice_ruth_01() )\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "create table t_alice_01( a ruth.price_ruth_01_a )\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "create function makePrice_alice_01_a( )\n" +
             "returns ruth.price_ruth_01_a\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price.makePrice'\n"
             );
        expectExecutionError
            (
             aliceConnection,
             LACK_USAGE_PRIV,
             "create function makePrice_alice_01_b( a ruth.price_ruth_01_a )\n" +
             "returns int\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price.makePrice'\n"
             );

        //
        // The DBO however is almighty.
        //
        assertResults
            (
             dboConnection,
             "select * from ruth.t_ruth_01",
             new String[][]
             {
                 { "Price( USD, 1, XXX )" },
             },
             false
             );
        assertResults
            (
             dboConnection,
             "values( ruth.makePrice_ruth_01() )\n",
             new String[][]
             {
                 { "Price( USD, 1, XXX )" },
             },
             false
             );
        goodStatement
            (
             dboConnection,
             "create table t_dbo_01( a ruth.price_ruth_01_a )\n"
             );
        goodStatement
            (
             dboConnection,
             "insert into t_dbo_01( a ) values ( ruth.makePrice_ruth_01() )\n"
             );
        assertResults
            (
             dboConnection,
             "select * from t_dbo_01\n",
             new String[][]
             {
                 { "Price( USD, 1, XXX )" },
             },
             false
             );
        goodStatement
            (
             dboConnection,
             "drop table t_dbo_01\n"
             );

        //
        // Now grant USAGE on the type. User Alice should now have all the
        // privileges she needs.
        //
        goodStatement
            (
             ruthConnection,
             "grant usage on type price_ruth_01_a to public\n"
             );
        
        assertResults
            (
             aliceConnection,
             "select * from ruth.t_ruth_01",
             new String[][]
             {
                 { "Price( USD, 1, XXX )" },
             },
             false
             );
        goodStatement
            (
             aliceConnection,
             "create table t_alice_01( a ruth.price_ruth_01_a )\n"
             );
        goodStatement
            (
             aliceConnection,
             "insert into t_alice_01( a ) values ( ruth.makePrice_ruth_01() )\n"
             );
        assertResults
            (
             aliceConnection,
             "select * from t_alice_01\n",
             new String[][]
             {
                 { "Price( USD, 1, XXX )" },
             },
             false
             );

    }
    
   /**
     * <p>
     * Test that USAGE privilege can't be revoked if it would make objects
     * unusable by their owners.
     * </p>
     */
    public  void    test_002_basicRevoke()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  ruthConnection = openUserConnection( RUTH );
        Connection  aliceConnection = openUserConnection( ALICE );

        goodStatement
            (
             ruthConnection,
             "create type price_ruth_02_a external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price' language java\n"
             );

        // only RESTRICTed revocations allowed
        expectCompilationError( ruthConnection, SYNTAX_ERROR, "revoke usage on type price_ruth_02_a from ruth\n" );

        // can't revoke USAGE from owner
        expectCompilationError
            (
             ruthConnection,
             GRANT_REVOKE_NOT_ALLOWED,
             "revoke usage on type price_ruth_02_a from ruth restrict\n"
             );

        String grantUsage = "grant usage on type price_ruth_02_a to alice\n";
        String revokeUsage = "revoke usage on type price_ruth_02_a from alice restrict\n";
        String createStatement;
        String dropStatement;
        String badRevokeSQLState;
        
        // can't revoke USAGE if a routine depends on it
        createStatement =
             "create function makePrice_alice_02( )\n" +
             "returns ruth.price_ruth_02_a\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price.makePrice'\n"
            ;
        dropStatement = "drop function makePrice_alice_02\n";
        badRevokeSQLState = ROUTINE_DEPENDS_ON_TYPE;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // can't revoke USAGE if a table depends on it
        createStatement = "create table t_alice_02( a ruth.price_ruth_02_a )\n";
        dropStatement = "drop table t_alice_02\n";
        badRevokeSQLState = TABLE_DEPENDS_ON_TYPE;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // can't revoke USAGE if a view depends on it
        createStatement = "create view v_alice_02( a ) as select cast (null as ruth.price_ruth_02_a ) from sys.systables\n";
        dropStatement = "drop view v_alice_02\n";
        badRevokeSQLState = VIEW_DEPENDENCY;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );

        // can't revoke USAGE if a trigger depends on it
        goodStatement( aliceConnection, "create table t_03_a( a int )\n" );
        goodStatement( aliceConnection, "create table t_03_b( a int )\n" );
        createStatement =
            "create trigger trig_03_a after insert on t_03_a\n" +
            "  insert into t_03_b( a ) select a from t_03_a where ( cast( null as ruth.price_ruth_02_a ) ) is not null\n";
        dropStatement = "drop trigger trig_03_a\n";
        badRevokeSQLState = OPERATION_FORBIDDEN;
        verifyRevokePrivilege
            (
             ruthConnection,
             aliceConnection,
             grantUsage,
             revokeUsage,
             createStatement,
             dropStatement,
             badRevokeSQLState
             );
    }
    
  /**
     * <p>
     * Test that you can't drop a schema if it contains a UDT or routine.
     * </p>
     */
    public  void    test_003_dropSchema()
        throws Exception
    {
        Connection  dboConnection = openUserConnection( TEST_DBO );
        Connection  frankConnection = openUserConnection( FRANK );

        goodStatement
            ( frankConnection, "create function f_frank_03( a int ) returns int language java parameter style java no sql external name 'foo.bar.Wibble'\n" );
        expectExecutionError( dboConnection, NON_EMPTY_SCHEMA, "drop schema frank restrict\n" );

        goodStatement
            (frankConnection, "drop function f_frank_03\n" );
        goodStatement
            ( frankConnection, "create type price_frank_03_a external name 'com.splicemachine.dbTesting.functionTests.tests.lang.Price' language java\n" );
        expectExecutionError( dboConnection, NON_EMPTY_SCHEMA, "drop schema frank restrict\n" );

        goodStatement
            ( frankConnection, "drop type price_frank_03_a restrict\n" );
        goodStatement
            ( dboConnection, "drop schema frank restrict\n" );
    }
    
}
