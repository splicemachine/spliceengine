/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.foreignkeys;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_dao.Constraint;
import com.splicemachine.test_dao.ConstraintDAO;
import com.splicemachine.test_dao.TableDAO;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.spark_project.guava.collect.Iterables;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLIntegrityConstraintViolationException;
import java.sql.Statement;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.*;

/**
 * Foreign Key tests for *defining* FK constraints.
 */
public class ForeignKey_Define_IT{

    private static final String SCHEMA=ForeignKey_Define_IT.class.getSimpleName();

    @Rule
    public ExpectedException expectedException=ExpectedException.none();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher=new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher=new SpliceWatcher(SCHEMA);

    private TestConnection conn;

    @Before
    public void deleteTables() throws Exception{
        new TableDAO(methodWatcher.getOrCreateConnection()).drop(SCHEMA,"C","P");
        conn=methodWatcher.getOrCreateConnection();
        conn.setAutoCommit(false);
    }

    @After
    public void tearDown() throws Exception{
        conn.rollback();
        conn.reset();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via create table, column level
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void createTable_colLevel() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void testCreateTableThenDropTable() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
            verifyForeignKey(conn);
            //now drop the table
        }
        new TableDAO(conn).drop(SCHEMA,"C","P");
    }

    @Test
    public void createTable_colLevel_noConstraintName() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_colLevel_implicitReferencedColumn() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int REFERENCES P)");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_colLevel_referencingUnique() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int unique, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_colLevel_referencingNonUniqueFails() throws Exception{
        try(Statement s=conn.createStatement()){
            expectedException.expect(SQLException.class);
            expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

            s.executeUpdate("create table P (id int, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int CONSTRAINT id_fk REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // create table, table level
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void createTable_tableLevel() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }


    @Test
    public void createTable_tableLevel_noConstraintName() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int, FOREIGN KEY (a_id) REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_tableLevel_implicitReferencedColumn() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int, FOREIGN KEY (a_id) REFERENCES P)");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_tableLevel_referencingUnique() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int unique, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void createTable_tableLevel_referencingNonUniqueFails() throws Exception{
        try(Statement s=conn.createStatement()){
            expectedException.expect(SQLException.class);
            expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

            s.executeUpdate("create table P (id int, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id))");
        }
    }

    @Test
    public void createTable_tableLevel_composite() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, unique(id, a))");
            s.executeUpdate("create table C (a int, a_id int, CONSTRAINT id_fk FOREIGN KEY (a_id, a) REFERENCES P(id, a))");
            verifyForeignKey(conn);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via alter table
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    public void alterTable() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int)");
            s.execute("set schema splice");
            s.executeUpdate("ALTER TABLE ForeignKey_Define_IT.C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES ForeignKey_Define_IT.P(id)");
            s.execute("set schema ForeignKey_Define_IT");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void alterTable_noConstraintName() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int)");
            s.executeUpdate("ALTER TABLE C ADD FOREIGN KEY (a_id) REFERENCES P(id)");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void alterTable_implicitReferencedColumn() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id))");
            s.executeUpdate("create table C (a int, a_id int)");
            s.executeUpdate("ALTER TABLE C ADD FOREIGN KEY (a_id) REFERENCES P");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void alterTable_referencingUnique() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int unique, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int)");
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
            verifyForeignKey(conn);
        }
    }

    @Test
    public void alterTable_referencingNonUniqueFails() throws Exception{
        try(Statement s=conn.createStatement()){
            expectedException.expect(Exception.class);
            expectedException.expectMessage("Constraint 'ID_FK' is invalid: there is no unique or primary key constraint on table '\"FOREIGNKEY_DEFINE_IT\".\"P\"' that matches the number and types of the columns in the foreign key.");

            s.executeUpdate("create table P (id int, a int, b int, c int)");
            s.executeUpdate("create table C (a int, a_id int)");
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT id_fk FOREIGN KEY (a_id) REFERENCES P(id)");
        }
    }

    @Test
    public void alterTable_composite() throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("create table P (id int, a int, b int, c int, primary key(id), unique(id, a))");
            s.executeUpdate("create table C (a int, a_id int)");
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id, a) REFERENCES P(id, a)");
            verifyForeignKey(conn);
        }
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // via alter table - existing data
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test
    @SuppressWarnings("unchecked")
    public void alterTable_existingData_success() throws Exception{
        new TableCreator(conn)
                .withCreate("create table P (id int, a int, b int, c int, primary key(id))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1,1,1,1),row(2,2,2,2),row(3,3,3,3))).create();
        new TableCreator(conn)
                .withCreate("create table C (a int, a_id int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1,1),row(2,2),row(3,3))).create();

        try(Statement s=conn.createStatement()){
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void alterTable_existingData_success_withNulls() throws Exception{
        new TableCreator(conn)
                .withCreate("create table P (a int, b int, c int, d int, primary key(b,c))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1,1,1,1),row(2,2,2,2),row(3,3,3,3))).create();

        new TableCreator(conn)
                .withCreate("create table C (a int, b int, c int)")
                .withInsert("insert into C values(?,?,?)")
                .withRows(rows(row(1,1,1),row(2,2,2),row(null,-1,-1),row(-1,2,null))).create();

        try(Statement s=conn.createStatement()){
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a,c) REFERENCES P(b,c)");
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void alterTable_existingData_fails_ifFkConstraintViolatedByExistingData() throws Exception{
        new TableCreator(conn)
                .withCreate("create table P (id int, a int, b int, c int, primary key(id))")
                .withInsert("insert into P values(?,?,?,?)")
                .withRows(rows(row(1,1,1,1),row(2,2,2,2),row(3,3,3,3))).create();

        new TableCreator(conn)
                .withCreate("create table C (a int, a_id int)")
                .withInsert("insert into C values(?,?)")
                .withRows(rows(row(1,1),row(2,2),row(-1,-1))).create();

        try(Statement s=conn.createStatement()){
            s.executeUpdate("ALTER TABLE C ADD CONSTRAINT FK_1 FOREIGN KEY (a_id) REFERENCES P(id)");
            fail("expected exception");
        }catch(Exception e){
            assertEquals("Foreign key constraint 'FK_1' cannot be added to or enabled on table \"FOREIGNKEY_DEFINE_IT\".\"C\" because one or more foreign keys do not have matching referenced keys.",e.getMessage());
        }

        // This should succeed, the FK should not be in place.
        try(Statement s=conn.createStatement()){
            s.executeUpdate("insert into C values(100,100)");
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // misc other FK definition tests
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    /* DB-3345 */
    @Test
    public void createTable_selfReferencingVerifySysTables() throws Exception{
        try(Statement s=conn.createStatement()){
            // given - self referencing PK
            s.executeUpdate("create table P(a int primary key, b int references P(a), c int unique)");

            // when - we select all constraints
            ConstraintDAO constraintDAO=new ConstraintDAO(conn);

            // then - we should find three constraints
            List<Constraint> constraintList=constraintDAO.getAllConstraints("P");
            assertEquals("constraints: "+constraintList,3,constraintList.size());

            // then - one should be FK
            assertNotNull(Iterables.find(constraintList,Constraint.constraintTypePredicate("F")));
            // then - one should be primary key
            assertNotNull(Iterables.find(constraintList,Constraint.constraintTypePredicate("P")));
            // then - one should be unique
            assertNotNull(Iterables.find(constraintList,Constraint.constraintTypePredicate("U")));
        }
    }


    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    //
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -


    private void verifyForeignKey(TestConnection conn) throws Exception{
        try(Statement s=conn.createStatement()){
            s.executeUpdate("insert into P values (100,1,0,0),(200,2,0,0),(300,3,0,0),(400,4,0,0),(500,5,0,0),(600,6,0,0)");
            s.executeUpdate("insert into C values (1,100),(2,200),(3,300)");
            assertEquals(6L,getCount(s,"select count(*) from P"));
            assertEquals(3L,getCount(s,"select count(*) from C"));
            try{
                s.executeUpdate("insert into C values (-1,-1)");
                fail("Expected exception from FK violation");
            }catch(SQLIntegrityConstraintViolationException e){
                assertTrue(e.getMessage(),e.getMessage().startsWith("Operation on table 'C' caused a violation of foreign key constraint"));
            }
            assertEquals(6L,getCount(s,"select count(*) from P"));
            assertEquals(3L,getCount(s,"select count(*) from C"));
        }
    }

    private long getCount(Statement s,String countQuery) throws SQLException{
        try(ResultSet rs=s.executeQuery(countQuery)){
            Assert.assertTrue("no rows returned!",rs.next());
            long c=rs.getLong(1);
            Assert.assertFalse("Too many rows returned!",rs.next());
            return c;
        }
    }

}
