package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.sql.Connection;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Foreign key alter table tests.
 *
 * FKs added by alter table is a significant distinct case because when a FK is added by alter table the four FK
 * WriteHandlers have to be added to write contexts (which may already be initialised) on all nodes for both the parent
 * and child tables.  On the other hand when a FK constraint is created as part of a create table statement the write
 * context, for the child at least, does not already exist, and the FK WriteHandlers get created from DB metadata the
 * first time someone writes to the table.
 */
public class ForeignKey_AlterTable_IT {

    private static final String SCHEMA = ForeignKey_AlterTable_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Before
    public void deleteTables() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();
        connection.setAutoCommit(false);
        new TableDAO(connection).drop(SCHEMA, "C", "P");
    }

    @Test
    public void foreignKeyAddedAfterParentAndChildWriteContextsInitializedIsEnforced() throws Exception {
        // given -- parent table with initialized write context
        methodWatcher.executeUpdate("create table P(a int primary key)");
        methodWatcher.executeUpdate("insert into P values(1),(2),(3)");
        // given -- child table with initialized write context
        methodWatcher.executeUpdate("create table C(a int primary key)");
        methodWatcher.executeUpdate("insert into C values(1)");

        // when -- we add the foreign key after the write contexts are initialized
        methodWatcher.executeUpdate("alter table C add constraint FK1 foreign key (a) references P(a)");

        // then -- the foreign key constraint is still enforced
        assertQueryFail("delete from P where a=1", "Operation on table 'P' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        assertQueryFail("insert into C values(222)", "Operation on table 'C' caused a violation of foreign key constraint 'FK1' for key (A).  The statement has been rolled back.");
        // then -- we can still insert values into the child that do exist in parent
        methodWatcher.executeUpdate("insert into C values(2)");
        // then -- we can still delete unreferenced values from the parent
        methodWatcher.executeUpdate("delete from P where a=3");
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // helper methods
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    private void assertQueryFail(String sql, String expectedExceptionMessage) {
        try {
            methodWatcher.executeUpdate(sql);
            fail(String.format("Expected query '%s' to fail with error message '%s'", sql, expectedExceptionMessage));
        } catch (Exception e) {
            assertEquals(expectedExceptionMessage, e.getMessage());
        }
    }

}