package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.ExpectedException;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Foreign Key tests for *defining* FK constraints.
 */
public class ForeignKey_Define_IT {

    private static final String SCHEMA = ForeignKey_Define_IT.class.getSimpleName();

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @Before
    public void deleteTables() throws Exception {
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA, "C", "P");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via create table, column level
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void createTable_colLevel() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
        verifyForeignKey();
    }

    @Test
    public void createTable_colLevel_noConstraintName() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int REFERENCES P(id))");
        verifyForeignKey();
    }

    @Test
    public void createTable_colLevel_implicitReferencedColumn() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int REFERENCES P)");
        verifyForeignKey();
    }

    @Test
    public void createTable_colLevel_referencingUnique() throws Exception {
        methodWatcher.executeUpdate("create table P (id int unique, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
        verifyForeignKey();
    }

    @Test
    public void createTable_colLevel_referencingNonUniqueFails() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
        verifyForeignKey();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // create table, table level
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void createTable_tableLevel() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
        verifyForeignKey();
    }


    @Test
    public void createTable_tableLevel_noConstraintName() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int, FOREIGN KEY (a_id) REFERENCES P(id))");
        verifyForeignKey();
    }

    @Test
    public void createTable_tableLevel_implicitReferencedColumn() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int, FOREIGN KEY (a_id) REFERENCES P)");
        verifyForeignKey();
    }

    @Test
    public void createTable_tableLevel_referencingUnique() throws Exception {
        methodWatcher.executeUpdate("create table P (id int unique, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
        verifyForeignKey();
    }

    @Test
    public void createTable_tableLevel_referencingNonUniqueFails() throws Exception {
        expectedException.expect(SQLException.class);
        expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
    }

    @Test
    public void createTable_tableLevel_composite() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, unique(id, a))");
        methodWatcher.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id, a) REFERENCES P(id, a))");
        verifyForeignKey();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via alter table
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void alterTable() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
        verifyForeignKey();
    }


    @Test
    public void alterTable_noConstraintName() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD FOREIGN KEY (a_id) REFERENCES P(id)");
        verifyForeignKey();
    }

    @Test
    public void alterTable_implicitReferencedColumn() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD FOREIGN KEY (a_id) REFERENCES P");
        verifyForeignKey();
    }

    @Test
    public void alterTable_referencingUnique() throws Exception {
        methodWatcher.executeUpdate("create table P (id int unique, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
        verifyForeignKey();
    }

    @Test
    public void alterTable_referencingNonUniqueFails() throws Exception {
        expectedException.expect(Exception.class);
        expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int)");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id)");
    }

    @Test
    public void alterTable_composite() throws Exception {
        methodWatcher.executeUpdate("create table P (id int, a int, b int, c int, primary key(id), unique(id, a))");
        methodWatcher.executeUpdate("create table C (a int, a_id int)");
        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id, a) REFERENCES P(id, a)");
        verifyForeignKey();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via alter table - existing data
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void alterTable_existingData_success() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();

        new TableCreator(connection)
                .withCreate("create table P (id int, a int, b int, c int, primary key(id))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1), row(2, 2, 2, 2), row(3, 3, 3, 3))).create();
        new TableCreator(connection)
                .withCreate("create table C (a int, a_id int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(3, 3))).create();

        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
    }

    @Test
    public void alterTable_existingData_success_withNulls() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();

        new TableCreator(connection)
                .withCreate("create table P (a int, b int, c int, d int, primary key(b,c))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1), row(2, 2, 2, 2), row(3, 3, 3, 3))).create();

        new TableCreator(connection)
                .withCreate("create table C (a int, b int, c int)")
                .withInsert("insert into C values(?,?,?)")
                .withRows(rows(row(1, 1, 1), row(2, 2, 2), row(null, -1, -1), row(-1, 2, null))).create();

        methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a,c) REFERENCES P(b,c)");
    }

    @Test
    public void alterTable_existingData_fails_ifFkConstraintViolatedByExistingData() throws Exception {
        Connection connection = methodWatcher.getOrCreateConnection();

        new TableCreator(connection)
                .withCreate("create table P (id int, a int, b int, c int, primary key(id))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1, 1, 1, 1), row(2, 2, 2, 2), row(3, 3, 3, 3))).create();

        new TableCreator(connection)
                .withCreate("create table C (a int, a_id int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1, 1), row(2, 2), row(-1, -1))).create();

        try {
            methodWatcher.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
            fail("expected exception");
        } catch (Exception e) {
            assertEquals("Foreign key constraint 'FK_1' cannot be added to or enabled on table \"FOREIGNKEY_DEFINE_IT\".\"C\" because one or more foreign keys do not have matching referenced keys.", e.getMessage());
        }

        // This should succeed, the FK should not be in place.
        methodWatcher.executeUpdate("insert into C values(100,100)");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // dropping table
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void dropTable_FailsIfTableWithDependentFKExists() throws Exception {
        methodWatcher.executeUpdate("create table P (a int, b int, constraint pk1 primary key(a))");
        methodWatcher.executeUpdate("create table C (a int, CONSTRAINT fk1 FOREIGN KEY(a) REFERENCES P(a))");
        try {
            methodWatcher.executeUpdate("drop table P");
            fail("Should not be able to drop P");
        } catch (Exception e) {
            assertEquals("Operation 'DROP CONSTRAINT' cannot be performed on object 'PK1' because CONSTRAINT 'FK1' is dependent on that object.", e.getMessage());
        }
        // This order should succeed.
        methodWatcher.executeUpdate("drop table C");
        methodWatcher.executeUpdate("drop table P");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    private void verifyForeignKey() throws Exception {
        methodWatcher.executeUpdate("insert into P values (100,1,0,0),(200,2,0,0),(300,3,0,0),(400,4,0,0),(500,5,0,0),(600,6,0,0)");
        methodWatcher.executeUpdate("insert into C values (1,100),(2,200),(3,300)");
        assertEquals(6L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));
        try {
            methodWatcher.executeUpdate("insert into C values (-1,-1)");
            fail("Expected exception from FK violation");
        } catch (SQLIntegrityConstraintViolationException e) {
            assertTrue(e.getMessage(), e.getMessage().startsWith("Operation on table 'C' caused a violation of foreign key constraint"));
        }
        assertEquals(6L, methodWatcher.query("select count(*) from P"));
        assertEquals(3L, methodWatcher.query("select count(*) from C"));
    }

}
