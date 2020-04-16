package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.*;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 8/21/18.
 */
public class AddDropPrimaryKeyIT extends SpliceUnitTest {
    public  static final String CLASS_NAME = AddDropPrimaryKeyIT.class.getSimpleName().toUpperCase();
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    private static final SpliceWatcher classWatcher = new SpliceWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        /* create a non-empty table with PK and index */
        new TableCreator(conn)
                .withCreate("create table t2(a2 int, b2 int, c2 int, primary key(b2,a2))")
                .withIndex("create index idx_t2 on t2(b2 desc, c2)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            classWatcher.executeUpdate(SpliceUnitTest.format("insert into t2 select a2, b2+%d,c2 from t2", factor));
            factor = factor * 2;
        }

        /* create a non-empty table with data */
        new TableCreator(conn)
                .withCreate("create table t3(a3 int, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    @Test
    public void testAddDropPrimaryKeyOnNonEmptyTable() throws Exception {
        try {
            methodWatcher.execute("alter table t3 add primary key(a3, b3)");
            methodWatcher.execute("alter table t3 drop primary key");
        } catch (SQLException se) {
            Assert.assertEquals(se.getSQLState(), SQLState.LANG_MODIFYING_PRIMARY_KEY_ON_NON_EMPTY_TABLE.substring(0,5));
        }
    }

    @Test
    public void testDropPrimaryKeyOnNonEmptyTable() throws Exception {
        try {
            methodWatcher.execute("alter table t2 drop primary key");
        } catch (SQLException se) {
            Assert.assertEquals(se.getSQLState(), SQLState.LANG_MODIFYING_PRIMARY_KEY_ON_NON_EMPTY_TABLE.substring(0,5));
        }
    }

    @Test
    public void testAddPrimaryKeyOnEmptyTable() throws Exception {
        Connection c1 = methodWatcher.getOrCreateConnection();
        c1.setAutoCommit(false);

        try {
            Statement s1 = c1.createStatement();
            s1.execute(String.format("create table %s.t1 (a1 int, b1 int not null, c1 int)", CLASS_NAME));

            /* add secondary index */
            s1.execute(String.format("create index %1$s.idx_t1 on %1$s.t1(b1 desc, c1)", CLASS_NAME));

            /* add primary key */
            s1.execute(String.format("alter table %s.t1 add primary key (b1, a1)", CLASS_NAME));

            /* check table description, a1, b1 both should be nullable */
            DatabaseMetaData dmd = c1.getMetaData();
            ResultSet rs = dmd.getColumns(null, CLASS_NAME, "T1", null);
            String expected = "TABLE_CAT |    TABLE_SCHEM     |TABLE_NAME | COLUMN_NAME | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH |DECIMAL_DIGITS |NUM_PREC_RADIX |NULLABLE | REMARKS |COLUMN_DEF | SQL_DATA_TYPE |SQL_DATETIME_SUB | CHAR_OCTET_LENGTH |ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG |SCOPE_SCHEMA | SCOPE_TABLE |SOURCE_DATA_TYPE |IS_AUTOINCREMENT |IS_GENERATEDCOLUMN |SCOPE_CATLOG |\n" +
                    "-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     A1      |     4     |  INTEGER  |     10      |     NULL      |       0       |      10       |    0    |         |   NULL    |     NULL      |      NULL       |       NULL        |        1        |     NO      |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     B1      |     4     |  INTEGER  |     10      |     NULL      |       0       |      10       |    0    |         |   NULL    |     NULL      |      NULL       |       NULL        |        2        |     NO      |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     C1      |     4     |  INTEGER  |     10      |     NULL      |       0       |      10       |    1    |         |   NULL    |     NULL      |      NULL       |       NULL        |        3        |     YES     |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |";
            assertEquals("\n check table definition: B1, C1 should be NOT NULL \n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

            /* check primary key */
            rs = dmd.getPrimaryKeys(null, CLASS_NAME, "T1");
            String[] columnName = {"B1", "A1"};
            int[] keySeq = {1, 2};
            for (int i = 0; i < columnName.length; i++) {
                if (!rs.next() || !rs.getString(4).equals(columnName[i]) || rs.getInt(5) != keySeq[i])
                    Assert.fail("Primary key does not match!");
            }
            rs.close();

            /* populate data in the table */
            s1.execute(String.format("insert into %1$s.t1 select * from %1$s.t2 where b2 > 100 and b2 <=120", CLASS_NAME));

            /* test join on PK */
            String sql = String.format("select * from --splice-properties joinOrder=fixed\n" +
                    "%1$s.t1 --splice-properties index=null\n" +
                    ", %1$s.t2 --splice-properties index=null, joinStrategy=merge\n" +
                    "where b1=b2", CLASS_NAME);

            expected = "A1 |B1  |C1 |A2 |B2  |C2 |\n" +
                    "--------------------------\n" +
                    " 1 |101 | 1 | 1 |101 | 1 |\n" +
                    " 2 |102 | 2 | 2 |102 | 2 |\n" +
                    " 3 |103 | 3 | 3 |103 | 3 |\n" +
                    " 4 |104 | 4 | 4 |104 | 4 |\n" +
                    " 5 |105 | 5 | 5 |105 | 5 |\n" +
                    " 6 |106 | 6 | 6 |106 | 6 |\n" +
                    " 7 |107 | 7 | 7 |107 | 7 |\n" +
                    " 8 |108 | 8 | 8 |108 | 8 |\n" +
                    " 9 |109 | 9 | 9 |109 | 9 |\n" +
                    "10 |110 |10 |10 |110 |10 |\n" +
                    " 1 |111 | 1 | 1 |111 | 1 |\n" +
                    " 2 |112 | 2 | 2 |112 | 2 |\n" +
                    " 3 |113 | 3 | 3 |113 | 3 |\n" +
                    " 4 |114 | 4 | 4 |114 | 4 |\n" +
                    " 5 |115 | 5 | 5 |115 | 5 |\n" +
                    " 6 |116 | 6 | 6 |116 | 6 |\n" +
                    " 7 |117 | 7 | 7 |117 | 7 |\n" +
                    " 8 |118 | 8 | 8 |118 | 8 |\n" +
                    " 9 |119 | 9 | 9 |119 | 9 |\n" +
                    "10 |120 |10 |10 |120 |10 |";
            try {
                rs = s1.executeQuery(sql);
                assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            } catch (SQLException se) {
                Assert.fail("Merge join plan on PK should go through instead of failure");
            } finally {
                rs.close();
            }

            /* test join on index */
            sql = String.format("select b1,b2,c1,c2 from --splice-properties joinOrder=fixed\n" +
                    "%1$s.t1 --splice-properties index=idx_t1\n" +
                    ", %1$s.t2 --splice-properties index=idx_t2, joinStrategy=merge\n" +
                    "where b1=b2", CLASS_NAME);
            expected = "B1  |B2  |C1 |C2 |\n" +
                    "------------------\n" +
                    "120 |120 |10 |10 |\n" +
                    "119 |119 | 9 | 9 |\n" +
                    "118 |118 | 8 | 8 |\n" +
                    "117 |117 | 7 | 7 |\n" +
                    "116 |116 | 6 | 6 |\n" +
                    "115 |115 | 5 | 5 |\n" +
                    "114 |114 | 4 | 4 |\n" +
                    "113 |113 | 3 | 3 |\n" +
                    "112 |112 | 2 | 2 |\n" +
                    "111 |111 | 1 | 1 |\n" +
                    "110 |110 |10 |10 |\n" +
                    "109 |109 | 9 | 9 |\n" +
                    "108 |108 | 8 | 8 |\n" +
                    "107 |107 | 7 | 7 |\n" +
                    "106 |106 | 6 | 6 |\n" +
                    "105 |105 | 5 | 5 |\n" +
                    "104 |104 | 4 | 4 |\n" +
                    "103 |103 | 3 | 3 |\n" +
                    "102 |102 | 2 | 2 |\n" +
                    "101 |101 | 1 | 1 |";
            try {
                rs = s1.executeQuery(sql);
                assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            } catch (SQLException se) {
                Assert.fail("Merge join plan on PK should go through instead of failure");
            } finally {
                rs.close();
            }
        } finally {
            c1.rollback();
            c1.setAutoCommit(true);
        }
    }

    @Test
    public void testDropPrimaryKeyOnEmptyTable() throws Exception {
        Connection c1 = methodWatcher.getOrCreateConnection();
        c1.setAutoCommit(false);

        try {
            Statement s1 = c1.createStatement();
            s1.execute(String.format("create table %s.t1 (a1 int, b1 int not null, c1 int, primary key (b1, a1))", CLASS_NAME));

            /* add secondary index */
            s1.execute(String.format("create index %1$s.idx_t1 on %1$s.t1(b1 desc, c1)", CLASS_NAME));

            /* drop primary key */
            s1.execute(String.format("alter table %s.t1 drop primary key", CLASS_NAME));

            /* check table description, a1, b1 both should be nullable */
            DatabaseMetaData dmd = c1.getMetaData();

            /* check primary key */
            ResultSet rs = dmd.getPrimaryKeys(null, CLASS_NAME, "T1");
            Assert.assertFalse("No primary key should be found!", rs.next());
            rs.close();

            /* populate data in the table */
            s1.execute(String.format("insert into %1$s.t1 select * from %1$s.t2 where b2 > 100 and b2 <=120", CLASS_NAME));

            /* test join on PK */
            String sql = String.format("select * from --splice-properties joinOrder=fixed\n" +
                    "%1$s.t1 --splice-properties index=null\n" +
                    ", %1$s.t2 --splice-properties index=null, joinStrategy=merge\n" +
                    "where b1=b2", CLASS_NAME);

            String expected = "A1 |B1  |C1 |A2 |B2  |C2 |\n" +
                    "--------------------------\n" +
                    " 1 |101 | 1 | 1 |101 | 1 |\n" +
                    " 2 |102 | 2 | 2 |102 | 2 |\n" +
                    " 3 |103 | 3 | 3 |103 | 3 |\n" +
                    " 4 |104 | 4 | 4 |104 | 4 |\n" +
                    " 5 |105 | 5 | 5 |105 | 5 |\n" +
                    " 6 |106 | 6 | 6 |106 | 6 |\n" +
                    " 7 |107 | 7 | 7 |107 | 7 |\n" +
                    " 8 |108 | 8 | 8 |108 | 8 |\n" +
                    " 9 |109 | 9 | 9 |109 | 9 |\n" +
                    "10 |110 |10 |10 |110 |10 |\n" +
                    " 1 |111 | 1 | 1 |111 | 1 |\n" +
                    " 2 |112 | 2 | 2 |112 | 2 |\n" +
                    " 3 |113 | 3 | 3 |113 | 3 |\n" +
                    " 4 |114 | 4 | 4 |114 | 4 |\n" +
                    " 5 |115 | 5 | 5 |115 | 5 |\n" +
                    " 6 |116 | 6 | 6 |116 | 6 |\n" +
                    " 7 |117 | 7 | 7 |117 | 7 |\n" +
                    " 8 |118 | 8 | 8 |118 | 8 |\n" +
                    " 9 |119 | 9 | 9 |119 | 9 |\n" +
                    "10 |120 |10 |10 |120 |10 |";
            try {
                rs = s1.executeQuery(sql);
                Assert.fail("query with merge join plan should fail!");
            } catch (SQLException se) {
                Assert.assertTrue(se.getMessage().compareTo("No valid execution plan was found for this statement. This is usually because an infeasible join strategy was chosen, or because an index was chosen which prevents the chosen join strategy from being used.")==0);
            } finally {
                rs.close();
            }
            sql = String.format("select * from --splice-properties joinOrder=fixed\n" +
                    "%1$s.t1 --splice-properties index=null\n" +
                    ", %1$s.t2 --splice-properties index=null\n" +
                    "where b1=b2 order by b1", CLASS_NAME);
            rs = s1.executeQuery(sql);
            assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));

            /* test join on index, merge join plan should still work */
            sql = String.format("select b1,b2,c1,c2 from --splice-properties joinOrder=fixed\n" +
                    "%1$s.t1 --splice-properties index=idx_t1\n" +
                    ", %1$s.t2 --splice-properties index=idx_t2, joinStrategy=merge\n" +
                    "where b1=b2", CLASS_NAME);
            expected = "B1  |B2  |C1 |C2 |\n" +
                    "------------------\n" +
                    "120 |120 |10 |10 |\n" +
                    "119 |119 | 9 | 9 |\n" +
                    "118 |118 | 8 | 8 |\n" +
                    "117 |117 | 7 | 7 |\n" +
                    "116 |116 | 6 | 6 |\n" +
                    "115 |115 | 5 | 5 |\n" +
                    "114 |114 | 4 | 4 |\n" +
                    "113 |113 | 3 | 3 |\n" +
                    "112 |112 | 2 | 2 |\n" +
                    "111 |111 | 1 | 1 |\n" +
                    "110 |110 |10 |10 |\n" +
                    "109 |109 | 9 | 9 |\n" +
                    "108 |108 | 8 | 8 |\n" +
                    "107 |107 | 7 | 7 |\n" +
                    "106 |106 | 6 | 6 |\n" +
                    "105 |105 | 5 | 5 |\n" +
                    "104 |104 | 4 | 4 |\n" +
                    "103 |103 | 3 | 3 |\n" +
                    "102 |102 | 2 | 2 |\n" +
                    "101 |101 | 1 | 1 |";
            try {
                rs = s1.executeQuery(sql);
                assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            } catch (SQLException se) {
                Assert.fail("Merge join plan on PK should go through instead of failure");
            } finally {
                rs.close();
            }
        } finally {
            c1.rollback();
            c1.setAutoCommit(true);
        }
    }

    @Test
    public void testAddPrimaryKeyWithVariousDataType() throws Exception {
        Connection c1 = methodWatcher.getOrCreateConnection();
        c1.setAutoCommit(false);

        try {
            Statement s1 = c1.createStatement();
            s1.execute(String.format("create table %s.t1 (a1 int, b1 date default '1999-09-01', c1 varchar(10))", CLASS_NAME));

            /* add primary key */
            s1.execute(String.format("alter table %s.t1 add primary key (c1, b1)", CLASS_NAME));

            /* check table description, a1, b1 both should be nullable */
            DatabaseMetaData dmd = c1.getMetaData();
            ResultSet rs = dmd.getColumns(null, CLASS_NAME, "T1", null);
            String expected = "TABLE_CAT |    TABLE_SCHEM     |TABLE_NAME | COLUMN_NAME | DATA_TYPE | TYPE_NAME | COLUMN_SIZE | BUFFER_LENGTH |DECIMAL_DIGITS |NUM_PREC_RADIX |NULLABLE | REMARKS | COLUMN_DEF  | SQL_DATA_TYPE |SQL_DATETIME_SUB | CHAR_OCTET_LENGTH |ORDINAL_POSITION | IS_NULLABLE | SCOPE_CATALOG |SCOPE_SCHEMA | SCOPE_TABLE |SOURCE_DATA_TYPE |IS_AUTOINCREMENT |IS_GENERATEDCOLUMN |SCOPE_CATLOG |\n" +
                    "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     A1      |     4     |  INTEGER  |     10      |     NULL      |       0       |      10       |    1    |         |    NULL     |     NULL      |      NULL       |       NULL        |        1        |     YES     |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     B1      |    91     |   DATE    |     10      |     NULL      |       0       |      10       |    0    |         |'1999-09-01' |     NULL      |      NULL       |       NULL        |        2        |     NO      |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |\n" +
                    "           |ADDDROPPRIMARYKEYIT |    T1     |     C1      |    12     |  VARCHAR  |     10      |     NULL      |     NULL      |     NULL      |    0    |         |    NULL     |     NULL      |      NULL       |        20         |        3        |     NO      |     NULL      |    NULL     |    NULL     |      NULL       |       NO        |        NO         |    NULL     |";
            assertEquals("\n check table definition: B1, C1 should be NOT NULL \n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

            /* check primary key */
            rs = dmd.getPrimaryKeys(null, CLASS_NAME, "T1");
            String[] columnName = {"C1", "B1"};
            int[] keySeq = {1, 2};
            for (int i = 0; i < columnName.length; i++) {
                if (!rs.next() || !rs.getString(4).equals(columnName[i]) || rs.getInt(5) != keySeq[i])
                    Assert.fail("Primary key does not match!");
            }
            rs.close();

            // populate table with some data
            s1.execute(String.format("insert into %s.t1 values (1, '2018-08-23', 'fff'), (2,'2018-07-23', 'fff'), (3, '2018-05-30', 'ggg')", CLASS_NAME));
            s1.execute(String.format("insert into %s.t1(a1,c1) values (4, 'aaa')", CLASS_NAME));

            //query the table with PK
            String sql = String.format("select * from %s.t1", CLASS_NAME);
            expected = "A1 |    B1     |C1  |\n" +
                    "---------------------\n" +
                    " 4 |1999-09-01 |aaa |\n" +
                    " 2 |2018-07-23 |fff |\n" +
                    " 1 |2018-08-23 |fff |\n" +
                    " 3 |2018-05-30 |ggg |";
            rs = s1.executeQuery(sql);
            assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();

            // query the table with predicate on PK
            sql = String.format("select * from %s.t1 where c1='fff' and b1=date('2018-07-23')", CLASS_NAME);
            expected = "A1 |    B1     |C1  |\n" +
                    "---------------------\n" +
                    " 2 |2018-07-23 |fff |";
            rs = s1.executeQuery(sql);
            assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        } finally {
            c1.rollback();
            c1.setAutoCommit(true);
        }
    }

    @Test
    public void testDropPrimaryKeyWithPKFKConstraint() throws Exception {
        Connection c1 = methodWatcher.getOrCreateConnection();
        c1.setAutoCommit(false);

        try {
            Statement s1 = c1.createStatement();
            s1.execute(String.format("create table %s.t1 (a1 int, b1 int, c1 int, primary key (a1))", CLASS_NAME));
            s1.execute(String.format("create table %s.t4 (a4 int, b4 int, c4 int, constraint fk_con foreign key (c4) references t1(a1))", CLASS_NAME));

            try {
                s1.execute(String.format("alter table %s.t1 drop primary key", CLASS_NAME));
            } catch (SQLException se) {
                Assert.assertEquals(se.getSQLState(), SQLState.LANG_PROVIDER_HAS_DEPENDENT_OBJECT.substring(0, 5));
            }
        } finally {
            c1.rollback();
            c1.setAutoCommit(true);
        }
    }
}
