package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import org.junit.*;

import java.sql.Connection;
import java.sql.SQLIntegrityConstraintViolationException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Foreign key tests for referential actions:
 *
 * ON DELETE NO ACTION
 * ON DELETE CASCADE
 * ON DELETE SET NULL
 * ON UPDATE NO ACTION
 */
public class ForeignKey_Action_IT {

    private static final String SCHEMA = ForeignKey_Action_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Before
    public void deleteTables() throws Exception {
        new TableDAO(connection()).drop(SCHEMA, "C", "P");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // fk references unique index
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void onDeleteNoAction() throws Exception {
        methodWatcher.executeUpdate("create table P (a int unique, b int)");
        methodWatcher.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
        methodWatcher.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
        methodWatcher.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

        assertQueryFail("delete from P where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update P set a=-1 where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
    }

    @Test
    public void onDeleteNoActionImplicit() throws Exception {
        methodWatcher.executeUpdate("create table P (a int unique, b int)");
        methodWatcher.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a))");
        methodWatcher.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
        methodWatcher.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

        assertQueryFail("delete from P where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update P set a=-1 where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // fk references primary key
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void onDeleteNoAction_primaryKey() throws Exception {
        methodWatcher.executeUpdate("create table P (a int primary key, b int)");
        methodWatcher.executeUpdate("create table C (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a) ON DELETE NO ACTION)");
        methodWatcher.executeUpdate("insert into P values(1,10),(2,20),(3,30)");
        methodWatcher.executeUpdate("insert into C values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

        assertQueryFail("delete from P where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update P set a=-1 where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
    }

    /* Make sure FKs still work when we create the parent, write to it first, then create the child that actually has the FK? */
    @Test
    public void onDeleteNoAction_primaryKey_initializeWriteContextOfParentFirst() throws Exception {
        methodWatcher.executeUpdate("create table P (a int primary key, b int unique)");
        methodWatcher.executeUpdate("insert into P values(1,10),(2,20),(3,30),(4,40)");

        methodWatcher.executeUpdate("create table C1 (a int, b int, CONSTRAINT FK_1 FOREIGN KEY (a) REFERENCES P(a))");
        methodWatcher.executeUpdate("insert into C1 values(1,10),(1,15),(2,20),(2,20),(3,30),(3,35)");

        assertQueryFail("delete from P where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update P set a=-1 where a = 2", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");

        methodWatcher.executeUpdate("create table C2 (a int, b int, CONSTRAINT FK_2 FOREIGN KEY (b) REFERENCES P(b))");
        methodWatcher.executeUpdate("insert into C2 values(4,40)");

        // verify NEW FK constraint works
        assertQueryFail("delete from P where a = 4", "Operation on table 'P' caused a violation of foreign key constraint 'FK_2' for key (B).  The statement has been rolled back.");
        assertQueryFail("update P set b=-1 where a = 4", "Operation on table 'P' caused a violation of foreign key constraint 'FK_2' for key (B).  The statement has been rolled back.");

        // verify FIRST FK constraint STILL works
        assertQueryFail("delete from P where a = 1", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
        assertQueryFail("update P set a=-1 where a = 1", "Operation on table 'P' caused a violation of foreign key constraint 'FK_1' for key (A).  The statement has been rolled back.");
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

    private void assertQueryFail(String sql, String expectedExceptionMessage) {
        try {
            methodWatcher.executeUpdate(sql);
            fail();
        } catch (Exception e) {
            assertEquals(expectedExceptionMessage, e.getMessage());
            assertEquals(SQLIntegrityConstraintViolationException.class, e.getClass());
        }
    }

}