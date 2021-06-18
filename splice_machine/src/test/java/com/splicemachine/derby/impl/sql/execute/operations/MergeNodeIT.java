/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import static org.junit.Assert.*;

public class MergeNodeIT
{
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = MergeNodeIT.class.getSimpleName().toUpperCase();

    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void beforeClass() throws Exception {
        //createDataSet();
    }

//    private static void createDataSet() throws Exception {
//        Connection conn = spliceClassWatcher.getOrCreateConnection();
//        conn.createStatement().execute("CREATE SCHEMA " + spliceSchemaWatcher.schemaName + " IF NOT EXISTS");
//        conn.setSchema(spliceSchemaWatcher.schemaName);
//
//        new TableCreator(conn)
//                .withCreate("create table A_src ( i integer, j integer)")
//                .withInsert("insert into A_src values(?,?,?)")
//                .withRows(rows(row(1, 10), row(2, 20), row(3, 30)))
//                .create();
//
//        new TableCreator(conn)
//                .withCreate("create table A_dest ( i integer, j integer)")
//                .withInsert("insert into A_dest values(?,?,?)")
//                .withRows(rows(row(1, 1), row(2, 2), row(3, 3)))
//                .create();
//    }

    private void createTables(String src, String dest) throws Exception {
        dropTables();
        methodWatcher.execute("create table T_src ( i integer, j integer, m integer)");
        methodWatcher.execute("insert into T_src values " + src);
        methodWatcher.execute("create table T_dest ( i integer, j integer, k integer)");
        methodWatcher.execute("insert into T_dest values " + dest);
    }

    private void dropTables() throws Exception {
        methodWatcher.execute("drop table T_src if exists");
        methodWatcher.execute("drop table T_dest if exists");
    }

    void test(String src, String dest, String sql, int expectedCount, String res) throws Exception {
        try {
            createTables(src, dest);
            Assert.assertEquals(expectedCount, methodWatcher.executeUpdate(sql));
            methodWatcher.assertStrResult( res, "select * from T_dest", true);
        }
        finally {
            dropTables();
        }
    }

    @Test
    public void testSimpleInsert() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444)", // src
                 "(1, 10, 3), (2, 20, 3)", // dest

                 "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                         "when not matched then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5)",

                 1, // 1 inserted row

                 "I | J | K |\n" +
                 "------------\n" +
                 " 1 |10 | 3 |\n" +
                 " 2 |20 | 3 |\n" +
                 " 4 |44 | 5 |");
    }

    //@Ignore // fails
    @Test
    public void testSimpleInsertTwoRows() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when not matched then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5)",

                2, // 2 inserted rows

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |\n" +
                " 4 |44 | 5 |\n" +
                " 5 |55 | 5 |");
    }

    @Test
    public void testInsertWithRefinement() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when not matched AND T_src.i > 4 then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5)",

                1, // 1 inserted row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |\n" +
                " 5 |55 | 5 |");
    }

    @Test
    public void testInsertWithRefinementMulti() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when not matched AND T_src.i > 4 then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5) " +
                        "when not matched AND T_src.i < 5 then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 7)",

                2, // 2 inserted row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |\n" +
                " 4 |44 | 7 |\n" +
                " 5 |55 | 5 |");
    }

    // delete

    @Test
    public void testSimpleDelete() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when matched then DELETE",

                1, // 1 deleted row

                "I | J | K |\n" +
                "------------\n" +
                " 2 |20 | 3 |");
    }

    @Test
    public void testSimpleUpdate() throws Exception {
        test(   "(1, 11, 111)", // src
                "(1, 10, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when matched then UPDATE SET k = 5",

                1, // 1 updated row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 5 |");
    }

    @Test
    public void testSimpleUpdate2() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when matched then UPDATE SET T_dest.j = T_src.j",

                1, // 1 updated row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |11 | 3 |\n" +
                " 2 |20 | 3 |");
    }

    @Test
    public void testMultiClauseInsertDeleteUpdate() throws Exception {
        test(   "(1, 11, 111), (2, 0, 0), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3), (-1, -1, -1)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        // will match (4,44,444) -> insert (4,44,5)
                        "when not matched AND T_src.i = 4 then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5) " +
                        // matches (5,55,555) -> insert (5, 55, 7)
                        // would also match (4,44,444), but first clause is executed first (exclusively)
                        "when not matched AND T_src.i > 2 then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 7)" +
                        // deletes (2, 20, 3) from dest
                        "when matched AND T_src.i > 1 then DELETE " +
                        // matches (1, 11, 111) <-> (1, 11, 111), so will update (1, 10 -> 11, 111).
                        // also matches (2, ) but already DELETEd
                        "when matched then UPDATE SET T_dest.j = T_src.j",

                4, // 1 deleted + 2 inserted, 1 updated

                "I | J | K |\n" +
                "------------\n" +
                "-1 |-1 |-1 |\n" +
                " 1 |11 | 3 |\n" +
                " 4 |44 | 5 |\n" +
                " 5 |55 | 7 |");
    }


    public void checkGrammar(String sql) {
        try
        {
            methodWatcher.execute(sql);
            fail("Did not fail");
        } catch (Exception e) {
            assertFalse("unexpected syntax error for sql\n" + sql + "\n:\n" + e.getMessage(),
                    e.getMessage().startsWith("Syntax error"));
        }
    }
    @Test
    public void testGrammar() throws Exception {
        // when matched UPDATE
        checkGrammar("merge into A_dest dest using A_src src on (src.i = src.j) when matched then update set dest.i = src.j");

        // using from subquery
        checkGrammar("merge into A_dest dest using (select i*2, j from A_src) src on (src.i = src.j)" +
                " when matched then update set dest.i = src.j ");

        // when matched DELETE
        checkGrammar("merge into A_dest dest using A_src src on (src.i = src.j) when matched then delete");

        // when + AND
        checkGrammar("merge into A_dest dest using A_src src on (src.i = src.j)" +
                " when matched AND dest.i > 0 then update set dest.i = src.j ");

        // multiple WHEN matched
        checkGrammar("merge into A_dest dest using A_src src on (src.i = src.j)" +
                " when matched AND dest.i > 0 then update set dest.i = src.j " +
                " when matched AND dest.i < 0 then update set dest.i = src.j+1 ");

        // when not matched insert
        checkGrammar("merge into A_dest dest using A_src src on (src.i = src.j)" +
                " when NOT matched then INSERT VALUES (5, 5)");

    }

    @Test
    public void testParseError() throws Exception {
        createTables("(1, 1, 1)", "(2, 2, 2)");
        Connection c = spliceClassWatcher.getOrCreateConnection();
        // when then update -> missing MATCHED / NOT MATCHED
        SpliceUnitTest.assertFailed(c, "merge into A_dest dest using A_src src on (src.i = src.j) " +
                "when then update set dest.i = src.j", SQLState.LANG_SYNTAX_ERROR,
                "Syntax error: Encountered \"then\" at line 1, column 64.");

        // not supported syntax: when not matched only with INSERT, not update
        SpliceUnitTest.assertFailed(c, "merge into A_dest dest using A_src src on (src.i = src.j)" +
                " when matched then update set dest.i = src.j " +
                " when not matched then update set dest.i = src.j", SQLState.LANG_SYNTAX_ERROR,
                "Syntax error: Encountered \"update\" at line 1, column 126.");
        dropTables();
    }
}
