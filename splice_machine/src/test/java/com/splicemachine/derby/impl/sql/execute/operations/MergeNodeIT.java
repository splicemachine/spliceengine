package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import com.sun.org.apache.xalan.internal.xsltc.compiler.util.ErrorMsg;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

public class MergeNodeIT
{
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    public static final String CLASS_NAME = MergeNodeIT.class.getSimpleName().toUpperCase();

    protected static String TABLE_1 = "A";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
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

    void test(String src, String dest, String sql, String res) throws Exception {
        try {
            createTables(src, dest );
            methodWatcher.execute(sql);
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

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |\n" +
                " 4 |44 | 7 |\n" +
                " 5 |55 | 5 |");
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
