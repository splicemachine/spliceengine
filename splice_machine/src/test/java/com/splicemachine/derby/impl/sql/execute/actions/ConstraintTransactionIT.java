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

package com.splicemachine.derby.impl.sql.execute.actions;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;

/**
 * @author Jeff Cunningham
 *         Date: 5/1/15
 */
public class ConstraintTransactionIT {
    private static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(ConstraintTransactionIT.class.getSimpleName().toUpperCase());
    private static final SpliceWatcher classWatcher = new SpliceWatcher();

    private TableDAO tableDAO;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
                                            .around(schemaWatcher);

    @Before
    public void setUp() throws Exception {
        tableDAO = new TableDAO(classWatcher.getOrCreateConnection());
    }

    @Test
    public void testAddColumn() throws Exception {
        String tableName = "testAddColumn".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s(num int, addr varchar(50), zip char(5))", tableRef));

        c1.commit();

        s1.execute(String.format("insert into %s values(100, '100F: 101 California St', '94114')", tableRef));
        s1.execute(String.format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')", tableRef));
        s1.execute(String.format("insert into %s values(300, '300F: my addr', '34166')", tableRef));
        s1.execute(String.format("insert into %s values(400, '400F: 182 Second St.', '94114')", tableRef));
        s1.execute(String.format("insert into %s(num) values(500)", tableRef));

        c1.commit();

        s1.execute(String.format("Alter table %s add column salary float default 0.0", tableRef));

        c1.commit();

        Connection c2 = classWatcher.createConnection();
        c2.setAutoCommit(false);
        Statement s2 = c2.createStatement();

        s1.execute(String.format("update %s set salary=1000.0 where zip='94114'", tableRef));
        s1.execute(String.format("update %s set salary=5000.85 where zip='94509'", tableRef));

        ResultSet rs = s1.executeQuery(String.format("select zip, salary from %s where salary > 0", tableRef));
        int count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull("Salary is null!",rs.getFloat(2));
        }
        Assert.assertEquals("Should only see 3 rows", 3,count);

        rs = s2.executeQuery(String.format("select zip, salary from %s where salary > 0", tableRef));
        count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull("Salary is null!",rs.getFloat(2));
        }
        Assert.assertEquals("Should see no rows; txn not committed.", 0, count);

        // updates will not be seen by c2 until both txns have committed
        c1.commit();
        c2.commit();
        rs = s2.executeQuery(String.format("select zip, salary from %s where salary > 0", tableRef));
        count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull("Salary is null!",rs.getFloat(2));
        }
        Assert.assertEquals("Should only see 3 rows.", 3,count);
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropUniqueConstraint() throws Exception {
        String tableName = "testDropUniqueConstraint".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int)", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(-1, null)", tableRef));
        s1.execute(String.format("insert into %s values(-2, null)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        s1.execute(String.format("alter table %s add constraint C2_UNIQUE UNIQUE (c2)", tableRef));
        c1.commit();

        try {
            s1.execute(String.format("insert into %s values(1, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s1.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c1.commit();

        // Now should be able to insert violating row
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testRollbackDropUniqueConstraint() throws Exception {
        String tableName = "testRollbackDropUniqueConstraint".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int)", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(-1, null)", tableRef));
        s1.execute(String.format("insert into %s values(-2, null)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        Connection c2 = classWatcher.createConnection();
        c2.setAutoCommit(false);
        Statement s2 = c2.createStatement();

        s2.execute(String.format("alter table %s add constraint C2_UNIQUE UNIQUE (c2)", tableRef));
        c2.commit();

        try {
            s1.execute(String.format("insert into %s values(1, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s1.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c1.rollback();

        // Rolled back drop constraint - should still not be able to insert violation
        try {
            s2.execute(String.format("insert into %s values(1, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s2.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c2.commit();

        // Now we can insert the row
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropPrimaryKey() throws Exception {
        String tableName = "testDropPrimaryKey".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (name char(14) not null primary key, age int)", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values('Ralph', 22)", tableRef));
        c1.commit();

        s1.execute(String.format("alter table %s add num char(11) not null default '000001'", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s (name, age) values('Fred', 23)", tableRef));
        s1.execute(String.format("insert into %s values('Joe', 24, '111111')", tableRef));
        c1.commit();

        try {
            s1.execute(String.format("insert into %s (name, age) values ('Fred', 30)", tableRef));
            Assert.fail("Expected primary key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s1.execute(String.format("alter table %s drop primary key", tableRef));
        c1.commit();

        // Now should be able to insert violating row
        s1.execute(String.format("insert into %s (name, age) values ('Fred', 30)", tableRef));

        ResultSet rs = s1.executeQuery("select * from " + tableRef);
        int count =0;
        while (rs.next()) {
            String name = rs.getString(1);
            Assert.assertNotNull("NAME is NULL!", name);
            if (name.trim().equals("Fred")) {
                count++;
            }
        }
        Assert.assertEquals("Should see 2 Freds.", 2, count);

        rs = s1.executeQuery("select * from " + tableRef+" where name = 'Fred'");
        count =0;
        while (rs.next()) {
            String name = rs.getString(1);
            Assert.assertNotNull("NAME is NULL!",name);
            count++;
        }
        Assert.assertEquals("Should see 2 Freds.", 2, count);
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testRollbackDropPrimaryKey() throws Exception {
        String tableName = "testRollbackDropPrimaryKey".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int not null)", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(-1, -1)", tableRef));
        s1.execute(String.format("insert into %s values(-2, -2)", tableRef));
        s1.execute(String.format("insert into %s values(2, 3)", tableRef));
        c1.commit();

        Connection c2 = classWatcher.createConnection();
        c2.setAutoCommit(false);
        Statement s2 = c2.createStatement();

        s2.execute(String.format("alter table %s add primary key (c2)", tableRef));
        c2.commit();

        try {
            s1.execute(String.format("insert into %s values(1, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s1.execute(String.format("alter table %s drop primary key", tableRef));
        c1.rollback();

        // Rolled back drop constraint - should still not be able to insert violation
        try {
            s2.execute(String.format("insert into %s values(1, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by 'SQL"));
        }

        s2.execute(String.format("alter table %s drop primary key", tableRef));
        c2.commit();

        // Now we can insert the row
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropUniqueConstraintCreatedWith() throws Exception {
        String tableName = "testDropUniqueConstraintCreatedWith".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int not null,constraint C2_UNIQUE unique (c2))", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(-1, 9)", tableRef));
        s1.execute(String.format("insert into %s values(-2, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        try {
            s1.execute(String.format("insert into %s values(2, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by '"));
        }

        s1.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c1.commit();

        // Now should be able to insert violating row
        s1.execute(String.format("insert into %s values(2, 3)", tableRef));
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropUniqueConstraintTableHasPK() throws Exception {
        String tableName = "testDropUniqueConstraintTableHasPK".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int not null primary key,c2 int not null,constraint C2_UNIQUE unique (c2))", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(2, 9)", tableRef));
        s1.execute(String.format("insert into %s values(3, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        try {
            s1.execute(String.format("insert into %s values(4, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by '"));
        }

        s1.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c1.commit();

        // Now should be able to insert violating row
        s1.execute(String.format("insert into %s values(4, 3)", tableRef));

        try {
            s1.execute(String.format("insert into %s values(2, 3)", tableRef));
            Assert.fail("Expected pirmary key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                                             "caused a " +
                                                                                             "duplicate key value in a unique or primary key " +
                                                                                             "constraint or unique index " +
                                                                                             "identified by '"));
        }
    }

    @Test
    @Ignore("DB-4004: Adding/dropping keyed columns not working")
    public void testDropUniqueConstraintTableHasTwo() throws Exception {
        String tableName = "testDropUniqueConstraintTableHasTwo".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int not null,c2 int not null,constraint C1_UNIQUE unique (c1),constraint C2_UNIQUE unique (c2))", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(-1, 9)", tableRef));
        s1.execute(String.format("insert into %s values(-2, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        try {
            // C2 does not allow duplicates
            s1.execute(String.format("insert into %s values(2, 3)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                     "caused a " +
                                                                     "duplicate key value in a unique or primary key " +
                                                                     "constraint or unique index " +
                                                                     "identified by '"));
        }

        s1.execute(String.format("alter table %s drop constraint C2_UNIQUE", tableRef));
        c1.commit();

        // Now should be able to insert previously violating row into C2
        s1.execute(String.format("insert into %s values(2, 3)", tableRef));

        try {
            // C1 still does not allow duplicates
            s1.execute(String.format("insert into %s values(1, 6)", tableRef));
            Assert.fail("Expected unique key violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith("The statement was aborted because it would have " +
                                                                                             "caused a " +
                                                                                             "duplicate key value in a unique or primary key " +
                                                                                             "constraint or unique index " +
                                                                                             "identified by '"));
        }
    }

    @Test
    public void testDropCheckConstraint() throws Exception {
        // DB-391
        String tableName = "testDropCheckConstraint".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int not null,constraint delme check (c1 > 0))", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(2, 9)", tableRef));
        s1.execute(String.format("insert into %s values(3, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        Connection c2 = classWatcher.createConnection();
        c2.setAutoCommit(false);
        Statement s2 = c2.createStatement();

        s2.execute(String.format("alter table %s drop constraint delme", tableRef));
        c2.commit();
    }

    @Test @Ignore("DB-5052: Adding not-null constraint to a table is not transactional")
    public void testNotNullConstraint() throws Exception {
        // DB-5052
        // DB-5039 follow-on to check to see that one thread adding not-null constraint is not hosed by another adding null value.
        String tableName = "testNotNullConstraint".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int)", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(2, 9)", tableRef));
        s1.execute(String.format("insert into %s values(3, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        Connection c2 = classWatcher.createConnection();
        c2.setAutoCommit(false);
        Statement s2 = c2.createStatement();

        // connection2 creates not-null constraint, but doesn't commit
        s2.execute(String.format("alter table %s alter column c2 not null", tableRef));
        // connection1 inserts a row that will yield null value in not-null constrained col
        s1.execute(String.format("insert into %s values(9, null)", tableRef));
        c1.commit();
        c2.commit();
    }

    @Test
//    @Ignore("DB-4641: failing when in Jenkins when run under the mem DB profile")
    public void testVerifyCheckConstraint() throws Exception {
        String tableName = "testVerifyCheckConstraint".toUpperCase();
        String tableRef = schemaWatcher.schemaName+"."+tableName;
        tableDAO.drop(schemaWatcher.schemaName, tableName);

        Connection c1 = classWatcher.createConnection();
        c1.setAutoCommit(false);
        Statement s1 = c1.createStatement();

        s1.execute(String.format("create table %s (c1 int,c2 int not null,constraint delme check (c1 > 0))", tableRef));
        c1.commit();

        s1.execute(String.format("insert into %s values(2, 9)", tableRef));
        s1.execute(String.format("insert into %s values(3, 8)", tableRef));
        s1.execute(String.format("insert into %s values(1, 3)", tableRef));
        c1.commit();

        try {
            s1.execute(String.format("insert into %s values(-1, 3)", tableRef));
            Assert.fail("Expected check constraint violation");
        } catch (SQLException e) {
            Assert.assertTrue(e.getLocalizedMessage(),e.getLocalizedMessage().startsWith(
            	"The check constraint 'DELME' was violated while performing an INSERT or UPDATE on table"));
        }

        s1.execute(String.format("alter table %s drop constraint delme", tableRef));
        c1.commit();

        // Now should be able to insert violating row
        s1.execute(String.format("insert into %s values(-1, 3)", tableRef));
    }
    
}
