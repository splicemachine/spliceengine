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
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportBuilder;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Date;

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
        if(src != null)
            methodWatcher.execute("insert into T_src values " + src);
        methodWatcher.execute("create table T_dest ( i integer, j integer, k integer)");
        if(dest != null)
            methodWatcher.execute("insert into T_dest values " + dest);
    }

    private void dropTables() throws Exception {
        methodWatcher.execute("drop table T_src if exists");
        methodWatcher.execute("drop table T_dest if exists");
    }

    void test(String src, String dest, String sql,
              int expectedCount, String res) throws Exception {
        test(src, dest, null, null, sql, expectedCount, res);

    }
    void test(String src, String dest, String sqlBefore, String sqlAfter, String sql,
               int expectedCount, String res) throws Exception {
        try {
            createTables(src, dest);
            if(sqlBefore != null)
                methodWatcher.execute(sqlBefore);
            Assert.assertEquals(expectedCount, methodWatcher.executeUpdate(sql));
            methodWatcher.assertStrResult( res, "select * from T_dest", true);
        }
        finally {
            if(sqlAfter != null)
                methodWatcher.execute(sqlAfter);
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

    @Test
    public void testSimpleInsertTwoRows() throws Exception {
        test("(1, 11, 111), (4, 44, 444), (5, 55, 555)", // src
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
    public void testSimpleInsertAs() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "merge into T_dest using T_src AS B on (T_dest.i = B.i) " +
                        "when not matched then INSERT (i, j, k) VALUES (B.i, B.j, 5)",

                1, // 1 inserted row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |\n" +
                " 4 |44 | 5 |");
    }

    @Test
    public void testInsertWithTrigger() throws Exception {
        test(   "(1, 11, 111), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3)", // dest

                "CREATE TRIGGER mytrig\n" +
                "   BEFORE INSERT\n" +
                "   ON T_dest\n" +
                "   REFERENCING NEW AS N\n" +
                "   FOR EACH ROW\n" +
                "BEGIN ATOMIC\n" +
                "SET N.j = 222;\n" +
                "END\n",
                "DROP TRIGGER mytrig",

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when not matched then INSERT (i, j, k) VALUES (T_src.i, T_src.j, 5)",
                2,

                "I | J  | K |\n" +
                "-------------\n" +
                " 1 |10  | 3 |\n" +
                " 2 |20  | 3 |\n" +
                " 4 |222 | 5 |\n" +
                " 5 |222 | 5 |");
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
    public void testSimpleUpdateNoMatch() throws Exception {
        test(   "(2, 22, 222)", // src
                "(1, 10, 3)", // dest

                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                        "when matched then UPDATE SET k = 5",

                0, // 1 updated row

                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 3 |");
    }

    @Test
    public void testUpdateWithTrigger() throws Exception {
        createTables(
                "(1, 11, 111)", // src
                "(1, 10, 3)" // dest
        );
        methodWatcher.execute("CREATE TABLE AUDIT_TABLE (T TIMESTAMP, MSG VARCHAR(255))");
        methodWatcher.execute("CREATE TRIGGER mytrig\n" +
                        "   AFTER UPDATE\n" +
                        "   ON T_Dest\n" +
                        "   FOR EACH STATEMENT\n" +
                        "       INSERT INTO AUDIT_TABLE VALUES (CURRENT_TIMESTAMP, 'TARGET_TABLE was updated')" );

        Assert.assertEquals(1, methodWatcher.executeUpdate(
                "merge into T_dest using T_src on (T_dest.i = T_src.i) " +
                "when matched then UPDATE SET k = 5"));

        methodWatcher.assertStrResult(
                "I | J | K |\n" +
                "------------\n" +
                " 1 |10 | 5 |",
                "select * from T_dest", true);
        methodWatcher.assertStrResult(
                "MSG           |\n" +
                "--------------------------\n" +
                "TARGET_TABLE was updated |",
                "select MSG from AUDIT_TABLE", true);

        methodWatcher.execute("DROP TRIGGER mytrig");
        methodWatcher.execute("DROP TABLE AUDIT_TABLE");
        dropTables();

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
        String mergeStatement =
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
                        "when matched then UPDATE SET T_dest.j = T_src.j";

        test(   "(1, 11, 111), (2, 0, 0), (4, 44, 444), (5, 55, 555)", // src
                "(1, 10, 3), (2, 20, 3), (-1, -1, -1)", // dest

                mergeStatement,

                4, // 1 deleted + 2 inserted, 1 updated

                "I | J | K |\n" +
                "------------\n" +
                "-1 |-1 |-1 |\n" +
                " 1 |11 | 3 |\n" +
                " 4 |44 | 5 |\n" +
                " 5 |55 | 7 |");

        test(   null, // src empty table
                "(1, 10, 3), (2, 20, 3), (-1, -1, -1)", // dest

                mergeStatement,

                0, // 1 deleted + 2 inserted, 1 updated

                "I | J | K |\n" +
                "------------\n" +
                "-1 |-1 |-1 |\n" +
                " 1 |10 | 3 |\n" +
                " 2 |20 | 3 |");
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
    public void testColumnDefault() throws Exception {
        try {
            methodWatcher.execute("create table T_src ( i integer, j integer, k integer)");
            methodWatcher.execute("insert into T_src values (1, 10, 100), (2, 20, 200), (3, 30, 300)");
            methodWatcher.execute(
                    "CREATE TABLE T_destDef (i INTEGER, j INTEGER,\n" +
                            " date DATE DEFAULT CURRENT_DATE,\n" +
                            " k INTEGER default 55)");
            methodWatcher.execute("insert into T_destDef (i, j) values (1, 11)");

            Date d1 = null;
            try( ResultSet rs = methodWatcher.executeQuery("VALUES CURRENT_DATE")) {
                Assert.assertTrue(rs.next());
                d1 = rs.getDate(1);
            }

            Assert.assertEquals(2, methodWatcher.executeUpdate("merge into T_destDef using T_src on (T_destDef.i = T_src.i) " +
                    "when not matched then INSERT (i, j) VALUES (T_src.i, T_src.j)"));

            methodWatcher.assertStrResult(
                    "I | J | K |\n" +
                    "------------\n" +
                    " 1 |11 |55 |\n" +
                    " 2 |20 |55 |\n" +
                    " 3 |30 |55 |",
                    "select i, j, k from T_destDef", true);

            try( ResultSet rs = methodWatcher.executeQuery("select date, CURRENT_DATE from T_destDef {limit 1}") ) {
                Assert.assertTrue(rs.next());
                Date res = rs.getDate(1);
                Date d2 = rs.getDate(2);
                // preventing the very rare sporadic around midnight, where d1 can differ from d2, and therefore also res.
                if(d1.equals(d2)) {
                    Assert.assertTrue(d1.equals(res) && d2.equals(res));
                }
                else
                    Assert.assertTrue(d1.equals(res) || d2.equals(res));
            }
        }
        finally{
            methodWatcher.execute("drop table T_destDef if exists");
            methodWatcher.execute("drop table T_src if exists");
        }
    }

    @Test
    public void testGrammar() {
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

    public void testMergeInTrigger(boolean useTrigger) throws Exception {
        try {

            methodWatcher.execute(
                    "CREATE TABLE FTable\n" +
                            "( entity_key INTEGER PRIMARY KEY,\n" +
                            "  last_update_ts TIMESTAMP,\n" +
                            "  feature1 DOUBLE,\n" +
                            "  feature2 DOUBLE)");

            methodWatcher.execute(
                    "CREATE TABLE FTable_History\n" +
                            "( entity_key INTEGER,\n" +
                            "  asof_ts TIMESTAMP,\n" +
                            "  ingest_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,\n" +
                            "  feature1 DOUBLE,\n" +
                            "  feature2 DOUBLE\n" +
                            //"    PRIMARY KEY ( entity_key, asof_ts)\n" +
                            ")");

            methodWatcher.execute("INSERT INTO FTable_History (entity_key, asof_ts, feature1, feature2) VALUES " +
                    "(11, '2011-11-11', 0.100, 0.10), (55, '2025-05-05', 0.15, 0.25)");

            //methodWatcher.execute("INSERT INTO FTable_History (entity_key, asof_ts, feature1, feature2) VALUES (11, '1999-9-9', 0.9, 0.99)");

            // the MERGE INTO statement, used in a trigger (useTrigger == true) or directly used
            // (not we have to fill in the SRC table)
            String mergeInto =
                    "MERGE INTO FTable_History USING %s on FTable_History.entity_key = SRC.entity_key\n" +
                            "        WHEN MATCHED THEN UPDATE SET\n" +
                            "           FTable_History.asof_ts = SRC.last_update_ts,\n" +
                            "           FTable_History.feature1 = SRC.feature1\n" + // don't update f2 on purpose
                            "        WHEN NOT MATCHED THEN INSERT (entity_key, asof_ts, feature1, feature2) VALUES\n" +
                            "           (SRC.entity_key,\n" +
                            "            SRC.last_update_ts,\n" +
                            "            SRC.feature1,\n" +
                            "            SRC.feature2)";

            if (useTrigger) {
                // SRC table in this case is a VTI
                methodWatcher.execute("CREATE TRIGGER FTable_INSERTS\n" +
                        "AFTER INSERT ON FTable\n" +
                        "REFERENCING NEW_TABLE AS SRC\n" +
                        "FOR EACH STATEMENT\n" +
                        "    " + String.format(mergeInto, "SRC"));
            }

            // if useTrigger=true, this also causes the merge to happen
            methodWatcher.execute("INSERT INTO FTable (entity_key, last_update_ts, feature1, feature2) VALUES " +
                    "(11, '2011-01-01', 0.11, 0.21), (22, '2022-02-02', 0.12, 0.22), (33, '2013-03-03', 0.13, 0.23)");

            if (!useTrigger) {
                methodWatcher.execute(String.format(mergeInto, "FTable AS SRC"));
            }

            methodWatcher.assertStrResult(
                    "ENTITY_KEY |   LAST_UPDATE_TS     |FEATURE1 |FEATURE2 |\n" +
                            "-------------------------------------------------------\n" +
                            "    11     |2011-01-01 00:00:00.0 |  0.11   |  0.21   |\n" +
                            "    22     |2022-02-02 00:00:00.0 |  0.12   |  0.22   |\n" +
                            "    33     |2013-03-03 00:00:00.0 |  0.13   |  0.23   |",
                    "select * from FTable", true);

            methodWatcher.assertStrResult(
                    "ENTITY_KEY |       ASOF_TS        |FEATURE1 |FEATURE2 |\n" +
                            "-------------------------------------------------------\n" +
                            "    11     |2011-01-01 00:00:00.0 |  0.11   |   0.1   |\n" +
                            "    22     |2022-02-02 00:00:00.0 |  0.12   |  0.22   |\n" +
                            "    33     |2013-03-03 00:00:00.0 |  0.13   |  0.23   |\n" +
                            "    55     |2025-05-05 00:00:00.0 |  0.15   |  0.25   |",
                    "select entity_key, asof_ts, feature1, feature2 from FTable_History", true);
        }
        finally {
            methodWatcher.execute("DROP TABLE FTable IF EXISTS");
            methodWatcher.execute("DROP TABLE FTable_History IF EXISTS");
        }
    }

    @Test
    public void testMergeInTrigger() throws Exception {
        testMergeInTrigger(true);
    }

    @Test
    public void testMergeDirect() throws Exception {
        testMergeInTrigger(false);
    }

    public static String getResourceDirectory() {
        return SpliceUnitTest.getBaseDirectory()+"/src/test/test-data/";
    }

    // testing large MERGE INTO where we have to break up the insert/update/delete into multiple chunks
    @Test
    public void mergeLarge() throws Exception {
        methodWatcher.execute("CREATE TABLE cust (I INTEGER, NAME VARCHAR(255))");
        ExportBuilder b = new ExportBuilder().path(getResourceDirectory() + "customer_iso_10k.csv");
        methodWatcher.execute(b.importSql("IMPORT_DATA", CLASS_NAME, "cust"));

        methodWatcher.execute("INSERT INTO cust SELECT * FROM cust"); // 20k
        methodWatcher.execute("INSERT INTO cust SELECT * FROM cust"); // 40k
        methodWatcher.execute("INSERT INTO cust SELECT * FROM cust"); // 80k
        long before = System.currentTimeMillis();
        methodWatcher.execute("INSERT INTO cust SELECT * FROM cust"); // 160k
        System.out.println("MergeNodeIT.create took " + 2*(System.currentTimeMillis()-before) + " ms");


        before = System.currentTimeMillis();
        methodWatcher.execute("CREATE TABLE MI_dest (I INTEGER, NAME VARCHAR(255), J INTEGER)");
        Assert.assertEquals(160000, methodWatcher.executeUpdate(
                "merge into MI_dest using cust on (MI_dest.i = cust.i) " +
                        "when not matched then INSERT (i, name, j) VALUES (cust.i, cust.name, cust.i)\n"+
                        "when matched THEN UPDATE SET MI_dest.j=22") );
        System.out.println("MergeNodeIT.mergeLarge took " + (System.currentTimeMillis()-before) + " ms");

        methodWatcher.execute("DROP TABLE MI_dest");
        methodWatcher.execute("DROP TABLE cust");
    }


    // testing large MERGE INTO where we have to break up the insert/update/delete into multiple chunks
    @Test
    public void mergeMultiChunks() throws Exception {
        int N = 8000; // can be up to 10000
        methodWatcher.execute("CREATE TABLE customer_iso_10k (I INTEGER, NAME VARCHAR(255))");
        ExportBuilder b= new ExportBuilder().path(getResourceDirectory() + "customer_iso_10k.csv");
        methodWatcher.execute( b.importSql("IMPORT_DATA", CLASS_NAME, "customer_iso_10k") );

        methodWatcher.execute("CREATE TABLE cust (I INTEGER, NAME VARCHAR(255))");
        methodWatcher.execute("INSERT INTO cust SELECT * FROM customer_iso_10k ORDER BY i {limit " + N + "}");


        methodWatcher.execute("CREATE TABLE MI_dest (I INTEGER, NAME VARCHAR(255), J INTEGER)");
        String insert = "INSERT INTO MI_dest VALUES (2, 'Abarca', 22), (3, 'Abelson', 33), (4, 'Abern', 44)";

        // INSERT more than 1000 rows + UPDATE three of them
        methodWatcher.execute(insert);
        Assert.assertEquals(N, methodWatcher.executeUpdate(
                        "merge into MI_dest using cust on (MI_dest.i = cust.i) " +
                        "when not matched then INSERT (i, name, j) VALUES (cust.i, cust.name, cust.i)\n"+
                        "when matched THEN UPDATE SET MI_dest.j=22") );

        methodWatcher.assertStrResult(
                "I |  NAME   | J |\n" +
                "------------------\n" +
                " 1 |Aaronson | 1 |\n" +
                " 2 | Abarca  |22 |\n" +
                " 3 | Abelson |22 |\n" +
                " 4 |  Abern  |22 |\n" +
                " 5 |  Abram  | 5 |",
                "select * from MI_dest ORDER BY I {limit 5}", true);

        // INSERT only more than 1000 rows
        methodWatcher.execute("DELETE FROM MI_dest");
        methodWatcher.execute(insert);
        Assert.assertEquals(N-3, methodWatcher.executeUpdate(
                        "merge into MI_dest using cust on (MI_dest.i = cust.i) " +
                        "when not matched then INSERT (i, name, j) VALUES (cust.i, cust.name, cust.i)")
        );

        // UPDATE more than 1000 rows
        Assert.assertEquals(N, methodWatcher.executeUpdate(
                "merge into MI_dest using cust on (MI_dest.i = cust.i) " +
                "when matched then UPDATE SET j = 1")
        );


        int sum_1_N = N*(N+1)/2; // = 1 + 2 + ... + 3000
        Assert.assertEquals(sum_1_N, methodWatcher.executeGetInt("select sum(i) from cust", 1));
        Assert.assertEquals(sum_1_N, methodWatcher.executeGetInt("select sum(i) from MI_dest", 1));
        Assert.assertEquals(N, methodWatcher.executeGetInt("select sum(j) from MI_dest", 1));

        // DELETE all rows with i > 100 (-> N-100 deleted)
        Assert.assertEquals(N-100, methodWatcher.executeUpdate(
                        "merge into MI_dest using cust on (MI_dest.i = cust.i) " +
                        "when matched AND cust.i > 100 then DELETE")
        );

        methodWatcher.execute("DROP TABLE MI_dest");
        methodWatcher.execute("DROP TABLE cust");
        methodWatcher.execute("DROP TABLE customer_iso_10k");
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
