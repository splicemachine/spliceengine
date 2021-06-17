/*

   Derby - Class org.apache.derbyTesting.functionTests.tests.lang.MergeStatementTest

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import static org.junit.Assert.*;

@Ignore // not yet working
public class MergeNodeDerbyIT {

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = MergeNodeIT.class.getSimpleName().toUpperCase();

    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    private Connection openUserConnection(String testDbo) {
        return methodWatcher.getOrCreateConnection();
    }

    ///////////////////////////////////////////////////////////////////////////////////////


    private void goodStatement(Connection connection, String sql) throws Exception {
        try(Statement s = connection.createStatement()) {
            s.execute(sql);
        }
    }

    private void goodUpdate(Connection connection, String sql, int rowsAffected) throws SQLException {
        try(Statement s = connection.createStatement()) {
            Assert.assertEquals(s.executeUpdate(sql), rowsAffected);
        }
    }


    private void assertResults(Connection connection, String sql, String[][] rows, boolean trimResults) throws SQLException {
        try(Statement s = connection.createStatement()) {
            s.execute(sql);
            assertResults(s.getResultSet(), rows, trimResults);
        }
    }

    protected void assertResults(ResultSet rs, String[][] rows, boolean trimResults )
            throws SQLException
    {
        int     rowCount = rows.length;

        for ( int i = 0; i < rowCount; i++ )
        {
            String[]    row = rows[ i ];
            int             columnCount = row.length;

            assertTrue( rs.next() );

            for ( int j = 0; j < columnCount; j++ )
            {
                String  expectedValue =  row[ j ];
                //println( "(row, column ) ( " + i + ", " +  j + " ) should be " + expectedValue );
                String  actualValue = null;
                int         column = j+1;

                actualValue = rs.getString( column );
                if ( rs.wasNull() ) { actualValue = null; }

                if ( (actualValue != null) && trimResults ) { actualValue = actualValue.trim(); }

                assertEquals( (expectedValue == null), rs.wasNull() );

                if ( expectedValue == null )    { assertNull( actualValue ); }
                else { assertEquals(expectedValue, actualValue); }
            }
        }

        assertFalse( rs.next() );
    }

    ///////////////////////////////////////////////////////////////////////////////////////

    private static  final   String      TEST_DBO = "TEST_DBO";

    @Test
    public  void    test_002_deleteAction()
            throws Exception
    {
        Connection dboConnection = openUserConnection( TEST_DBO );

        goodStatement
                ( dboConnection,
                        "create table t1_002( c1 int, c2 int, c3 int generated always as ( c1 + c2 ), c1_4 int )" );
        goodStatement
                ( dboConnection,
                        "create table t2_002( c1 int, c2 int, c3 int, c4 int, c5 varchar( 5 ) )" );

        // a DELETE action without a matching refinement clause
        vet_002
                (
                        dboConnection,
                        "merge into t1_002\n" +
                                "using t2_002\n" +
                                "on 2 * t1_002.c2 = 2 * t2_002.c2\n" +
                                "when matched then delete\n",
                        4,
                        new String[][]
                                {
                                        { "5", "5", "10", "5" },
                                        { "6", "20", "26", "40" },
                                }
                );

        // a DELETE action with a matching refinement clause
        vet_002
                (
                        dboConnection,
                        "merge into t1_002\n" +
                                "using t2_002\n" +
                                "on 2 * t1_002.c2 = 2 * t2_002.c2\n" +
                                "when matched and c1_4 = 5 then delete\n",
                        3,
                        new String[][]
                                {
                                        { "1", "2", "3", "4" },
                                        { "5", "5", "10", "5" },
                                        { "6", "20", "26", "40" },
                                }
                );

        //
        // drop schema
        //
        goodStatement( dboConnection, "drop table t2_002" );
        goodStatement( dboConnection, "drop table t1_002" );
        truncateTriggerHistory();
    }
    private void    vet_002
            (
                    Connection conn,
                    String query,
                    int    rowsAffected,
                    String[][] expectedResults
            )
            throws Exception
    {
        vet_002( conn, query, rowsAffected, expectedResults, false );
        vet_002( conn, query, rowsAffected, expectedResults, true );
    }
    private void    vet_002
            (
                    Connection conn,
                    String query,
                    int    rowsAffected,
                    String[][] expectedResults,
                    boolean    useHashJoinStrategy
            )
            throws Exception
    {
        if ( useHashJoinStrategy ) { query = makeHashJoinMerge( query ); }

        populate_002( conn );
        goodUpdate( conn, query, rowsAffected );
        assertResults( conn, "select * from t1_002 order by c1", expectedResults, false );
    }

    private void    populate_002( Connection conn )
            throws Exception
    {
        goodStatement( conn, "delete from t2_002" );
        goodStatement( conn, "delete from t1_002" );

        goodStatement
                ( conn,
                        "insert into t1_002( c1, c2, c1_4 ) values ( 1, 2, 4 ), (2, 2, 5), (3, 3, 5), (4, 4, 5), (5, 5, 5), ( 6, 20, 40 )"
                );
        goodStatement
                ( conn,
                        "insert into t2_002( c1, c2, c3, c4, c5 ) values ( 1, 2, 3, 4, 'five' ), ( 2, 3, 3, 4, 'five' ), ( 3, 4, 3, 4, 'five' ), ( 4, 200, 300, 400, 'five' )"
                );
    }

    private static ArrayList<String[]> _triggerHistory = new ArrayList<String[]>();
    /** Procedure to truncation the table which records trigger actions */
    public  static  void    truncateTriggerHistory()
    {
        _triggerHistory.clear();
    }

    /**
     * <p>
     * Convert a MERGE statement which uses a nested join strategy
     * into an equivalent MERGE statement which uses a hash join
     * strategy. To do this, we replace the ON clause with an equivalent
     * ON clause which joins on key columns instead of expressions.
     * </p>
     *
     * <p>
     * The original query is a MERGE statement whose ON clauses joins
     * complex expressions, making the optimizer choose a nested-loop
     * strategy. This method transforms the MERGE statement into one
     * whose ON clause joins simple keys. This will make the optimizer
     * choose a hash-join strategy.
     * </p>
     */
    private String  makeHashJoinMerge( String original )
    {
        return original.replace ( "2 *", " " );
    }

}
