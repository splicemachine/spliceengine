package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Foreign Key tests for *checking* FKs on INSERT.
 */
public class ForeignKey_Check_Insert_IT {

    private static final String SCHEMA = ForeignKey_Check_Insert_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    /* @Before for the tests, @After so SpliceSchemaWatcher doesn't blow up on deleting with FK dependencies (DB-2576) */
    @Before
    @After
    public void after() throws Exception {
        new TableDAO(connection()).deleteTableForce("C", "P");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // single column foreign keys
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void referencing_singleColumn_primaryKey() throws Exception {

        new TableCreator(connection())
                .withCreate("create table P (a varchar(10), b int, primary key(a))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        new TableCreator(connection())
                .withCreate("create table C (a varchar(10) CONSTRAINT c_fk_1 REFERENCES P, b int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        assertEquals(3L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));

        assertInsertFail("insert into C values('D', 200)", "INSERT on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
    }

    @Test
    public void referencing_singleColumn_uniqueIndex() throws Exception {

        new TableCreator(connection())
                .withCreate("create table P (a varchar(10) unique, b int)")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        new TableCreator(connection())
                .withCreate("create table C (a varchar(10) CONSTRAINT c_fk_1 REFERENCES P(a), b int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        assertEquals(3L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));

        assertInsertFail("insert into C values('D', 200)", "INSERT on table 'C' caused a violation of foreign key constraint 'C_FK_1' for key (A).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // multi-column foreign keys
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void referencing_twoColumn_primaryKey() throws Exception {

        new TableCreator(connection())
                .withCreate("create table P (a varchar(10), b int, c int, primary key(a, b))")
                .withInsert("insert into P values(?,?,?)")
                .withRows(rows(
                        row("A", 100, 1),
                        row("A", 150, 1),
                        row("B", 200, 2),
                        row("B", 250, 2),
                        row("C", 300, 3)
                ))
                .create();

        new TableCreator(connection())
                .withCreate("create table C (a varchar(10), b int, CONSTRAINT id_fk FOREIGN KEY (a,b) REFERENCES P(a,b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        assertEquals(5L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));

        /* one column value missing */
        assertInsertFail("insert into C values('C', 700)", "INSERT on table 'C' caused a violation of foreign key constraint 'ID_FK' for key (A,B).  The statement has been rolled back.");
        /* two columns values missing */
        assertInsertFail("insert into C values('D', 200)", "INSERT on table 'C' caused a violation of foreign key constraint 'ID_FK' for key (A,B).  The statement has been rolled back.");
    }

    @Test
    public void referencing_twoColumn_uniqueIndex() throws Exception {

        new TableCreator(connection())
                .withCreate("create table P (a varchar(10), b int, c int, UNIQUE(a,b))")
                .withInsert("insert into P values(?,?,?)")
                .withRows(rows(
                                row("A", 100, 1),
                                row("A", 200, 1),
                                row("B", 100, 2),
                                row("B", 200, 2),
                                row("C", 300, 3)
                        )
                )
                .create();

        new TableCreator(connection())
                .withCreate("create table C (a varchar(10), b int, CONSTRAINT c_fk1 FOREIGN KEY (a,b) REFERENCES P(a,b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row("A", 100), row("B", 200), row("C", 300)))
                .create();

        assertEquals(5L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));

        /* one column value missing */
        assertInsertFail("insert into C values('D', 200)", "INSERT on table 'C' caused a violation of foreign key constraint 'C_FK1' for key (A,B).  The statement has been rolled back.");
        /* two columns values missing */
        assertInsertFail("insert into C values('A', 300)", "INSERT on table 'C' caused a violation of foreign key constraint 'C_FK1' for key (A,B).  The statement has been rolled back.");
    }

    @Test
    public void referencing_twoColumn_uniqueIndex_withOrderSwap() throws Exception {

        new TableCreator(connection())
                .withCreate("create table P (a int, b int, UNIQUE(a, b))")
                .withInsert("insert into P values(?,?)")
                .withRows(rows(row(100, 1), row(100, 2), row(100, 3)))
                .create();

        new TableCreator(connection())
                .withCreate("create table C (x int, y int, CONSTRAINT fk FOREIGN KEY (y, x) REFERENCES P(a, b))")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1, 100), row(2, 100), row(3, 100)))
                .create();

        assertEquals(3L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));

        /* one column value missing */
        assertInsertFail("insert into C values(4, 100)", "INSERT on table 'C' caused a violation of foreign key constraint 'FK' for key (Y,X).  The statement has been rolled back.");
        /* two columns values missing */
        assertInsertFail("insert into C values(9, 900)", "INSERT on table 'C' caused a violation of foreign key constraint 'FK' for key (Y,X).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // failure semantics
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void failure_rollsBackFailedStatementOnlyNotEntireTransaction() throws Exception {
        Connection conn = connection();
        conn.setAutoCommit(false);
        assertFalse(conn.getAutoCommit());

        new TableCreator(conn).withCreate("create table P (a int, b int, CONSTRAINT p_index UNIQUE(a))").create();
        new TableCreator(conn).withCreate("create table C (a int REFERENCES P(a), b int)").create();

        methodWatcher.executeUpdate("insert into P values(100,1),(200,2),(300,3)");
        methodWatcher.executeUpdate("insert into C values(100,1)");
        try {
            methodWatcher.executeUpdate("insert into C values(-1,-1)");
            fail();
        } catch (SQLException e) {
            assertTrue(e.getMessage().startsWith("INSERT on table 'C' caused a violation of foreign key constraint"));
        }
        assertEquals(3L, methodWatcher.query("select count(*) from P"));
        assertEquals(1L, methodWatcher.query("select count(*) from C"));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // large-ish inserts
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* Also verifies that FK constraint is not enforcing uniqueness in child table. */
    @Test
    public void large_oneThousandRowsInChildTable() throws Exception {
        Connection conn = connection();
        assertFalse(conn.getAutoCommit());

        new TableCreator(conn).withCreate("create table P (a int, b int, CONSTRAINT p_index UNIQUE(a))").create();
        new TableCreator(conn).withCreate("create table C (a int REFERENCES P(a), b int)").create();

        methodWatcher.executeUpdate("insert into P values(10,1),(20,2),(30,3),(40,4),(50,5),(60,6),(70,7),(80,8)");
        methodWatcher.executeUpdate("insert into C values(10,1),(20,2),(30,3),(40,4),(50,5),(60,6),(70,7),(80,8)");

        for (int i = 0; i < 7; i++) {
            methodWatcher.executeUpdate("insert into C select * from C");
        }

        assertEquals(8L, methodWatcher.query("select count(*) from P"));
        assertEquals(1024L, methodWatcher.query("select count(*) from C"));

        // these should all fail
        try {
            methodWatcher.executeUpdate("insert into C select b,a from C");
            fail();
        } catch(SQLException e) {
        }
        assertEquals(1024L, methodWatcher.query("select count(*) from C"));
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // helper methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private Connection connection() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);
        return connection;
    }

    private void assertInsertFail(String sql, String expectedExceptionMessage) {
    /* both keys do not exist. */
        try {
            methodWatcher.executeUpdate(sql);
            fail();
        } catch (Exception e) {
            assertEquals(expectedExceptionMessage, e.getMessage());
        }
    }

}