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
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;

/**
 * <p>
 * Additional tests for Blob/Clobs created from user-supplied large objects.
 * </p>
 */
public class UserLobTest extends GeneratedColumnsHelper
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

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

    public UserLobTest(String name)
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
        TestSuite suite = (TestSuite) TestConfiguration.embeddedSuite(UserLobTest.class);
        Test        result = new CleanDatabaseTestSetup( suite );

        return result;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Test that user-defined LOBs can be stored in varbinary and varchar columns.
     * </p>
     */
    public  void    test_001_casts()
        throws Exception
    {
        Connection  conn = getConnection();

        //
        // Create some user defined functions which return lobs.
        // 
        goodStatement
            (
             conn,
             "create function f_2201_blob_1\n" +
             "(\n" +
             "	a_0 varchar( 10 )\n" +
             ")\n" +
             "returns blob\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.AnsiSignatures.blob_Blob_String'\n"
             );
        goodStatement
            (
             conn,
             "create function f_2201_clob_1\n" +
             "(\n" +
             "	a_0 varchar( 10 )\n" +
             ")\n" +
             "returns clob\n" +
             "language java\n" +
             "parameter style java\n" +
             "no sql\n" +
             "external name 'com.splicemachine.dbTesting.functionTests.tests.lang.AnsiSignatures.clob_Clob_String'\n"
             );

        //
        // Create some tables for storing the results of these functions
        // 
        goodStatement
            (
             conn,
             "create table t_2201_clob_blob_1( a clob, b blob )\n"
             );
        goodStatement
            (
             conn,
             "create table t_2201_chartypes_1( a char( 10 ), b varchar( 10 ), c long varchar )\n"
              );
        
        //
        // Successfully insert into these tables, casting as necessary.
        // 
        goodStatement
            (
             conn,
             "insert into t_2201_clob_blob_1( a, b ) values( f_2201_clob_1( 'abc' ), f_2201_blob_1( 'abc' ) )\n"
              );
        goodStatement
            (
             conn,
             "insert into t_2201_chartypes_1( a, b, c )\n" +
             "values\n" +
             "(\n" +
             "  cast( f_2201_clob_1( 'abc' ) as char( 10)),\n" +
             "  cast( f_2201_clob_1( 'def' ) as varchar( 10)),\n" +
             "  cast( f_2201_clob_1( 'ghi' ) as long varchar )\n" +
             ")\n"
              );
        assertResults
            (
             conn,
             "select * from t_2201_clob_blob_1",
             new String[][]
             {
                 { "abc" ,         "616263" },
             },
             false
             );
        assertResults
            (
             conn,
             "select * from t_2201_chartypes_1",
             new String[][]
             {
                 { "abc       ", "def", "ghi" },
             },
             false
             );
        assertResults
            (
             conn,
             "select length( a ), length( b ), length( c ) from t_2201_chartypes_1",
             new String[][]
             {
                 { "10", "3", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values\n" +
             "(\n" +
             "  length( cast( f_2201_clob_1( 'abc' ) as char( 10)) ),\n" +
             "  length( cast( f_2201_clob_1( 'defg' ) as varchar( 10)) ),\n" +
             "  length( cast( f_2201_clob_1( 'hijkl' ) as long varchar ) ),\n" +
             "  length( f_2201_clob_1( 'mnopqr' ) )\n" +
             ")\n",
             new String[][]
             {
                 { "10", "4", "5", "6" },
             },
             false
             );
        assertResults
            (
             conn,
             "select length( a ), length( b ) from t_2201_clob_blob_1",
             new String[][]
             {
                 { "3", "3" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( varchar( f_2201_clob_1( 'abc' ) ) )",
             new String[][]
             {
                 { "abc" },
             },
             false
             );
        assertResults
            (
             conn,
             "values ( substr( f_2201_clob_1( 'abc' ), 2, 2 ), upper( f_2201_clob_1( 'defg' ) ) )",
             new String[][]
             {
                 { "bc", "DEFG" },
             },
             false
             );
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////


}
