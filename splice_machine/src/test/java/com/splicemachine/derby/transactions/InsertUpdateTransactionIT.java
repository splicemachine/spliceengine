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

package com.splicemachine.derby.transactions;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;

import com.splicemachine.test.Transactions;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * @author Scott Fines
 * Date: 9/3/14
 */
@Category({Transactions.class})
public class InsertUpdateTransactionIT {
    public static final SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(InsertUpdateTransactionIT.class.getSimpleName().toUpperCase());

    public static final SpliceTableWatcher table = new SpliceTableWatcher("A",schemaWatcher.schemaName,"(a int, b int, primary key (a))");

    public static final SpliceWatcher classWatcher = new SpliceWatcher();
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(schemaWatcher)
            .around(table);

    private static TestConnection conn1;
    private static TestConnection conn2;

    private long conn1Txn;
    private long conn2Txn;

    @BeforeClass
    public static void setUpClass() throws Exception {
        conn1 = classWatcher.getOrCreateConnection();
        conn2 = classWatcher.createConnection();
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        conn1.close();
        conn2.close();
    }

    @After
    public void tearDown() throws Exception {
        conn1.rollback();
        conn1.reset();
        conn2.rollback();
        conn2.reset();
    }

    @Before
    public void setUp() throws Exception {
        conn1.setAutoCommit(false);
        conn2.setAutoCommit(false);
        conn1Txn = conn1.getCurrentTransactionId();
        conn2Txn = conn2.getCurrentTransactionId();
    }

    @Test
    public void testWritesDoNotConflictWithinSameTransactionInsertUpdate() throws Exception {
        int a = 1;
        int b = 1;
        PreparedStatement ps = conn1.prepareStatement("insert into "+ table+" (a,b) values (?,?)");
        ps.setInt(1,a);ps.setInt(2,b);ps.execute();

        long count = conn1.count("select * from "+ table+" where a = "+ a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,count);

        ps = conn1.prepareStatement("update "+ table+" set b = ? where a = ?");
        ps.setInt(1,b+1); ps.setInt(2,a); ps.execute(); //should not throw WWConflict

        count = conn1.count("select * from "+ table+" where a = "+a+" and b = "+ (b+1));
        Assert.assertEquals("Update rows are not visible to my own transaction!",1l,count);
    }

    @Test
    public void testInsertsAreVisibleToUpdatesSnapshotIsolation() throws Exception {
        int a = 3;
        int b = 3;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        ps.setInt(1, a); ps.setInt(2, b); ps.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);

        long conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);

        //rows won't be visible until conn1 commits AND conn2 commits
        conn1.commit();
        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);
        conn2.commit();

        conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 1, conn2Count);

        //issue an update with conn2
        ps = conn2.prepareStatement("update "+ table+" set b = ? where a = ?");
        ps.setInt(1,b+1); ps.setInt(2,a);
        int updated = ps.executeUpdate(); //should not throw WWConflict
        Assert.assertEquals("Update did not update proper row!",1,updated);

        //make sure the update isn't visible to conn1 yet
        conn1Count = conn1.count("select * from "+ table+ " where a = "+ a +" and b = "+ (b+1));
        Assert.assertEquals("Update is visible to other transaction!",0l,conn1Count);

        conn2Count = conn2.count("select * from "+ table+ " where a = "+ a +" and b = "+ (b+1));
        Assert.assertEquals("Update is not visible to my own transaction!",1l,conn2Count);
        conn2.commit();

        //update is still not visible to conn1
        conn1Count = conn1.count("select * from "+ table+ " where a = "+ a +" and b = "+ (b+1));
        Assert.assertEquals("Update is visible to other transaction!",0l,conn1Count);

        //commit conn1 to gain visibility
        conn1.commit();
        conn1Count = conn1.count("select * from "+ table+ " where a = "+ a +" and b = "+ (b+1));
        Assert.assertEquals("Update is still not visible to other transaction!", 1l, conn1Count);
    }

    @Test
    public void testUpdatesAreVisibleToInsertsSnapshotIsolation() throws Exception {
        int a = 4;
        int b = 4;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        ps.setInt(1, a); ps.setInt(2, b); ps.execute();
        conn1.commit(); conn2.commit(); //roll both connections forward so data is visible

        /*
         * Update a record, then attempt to insert the new record, and watch for a PK violation. If we
         * don't get a PK violation, it didn't work correctly.
         */
        ps = conn1.prepareStatement("update "+ table+" set a = ? where a = ?");
        ps.setInt(1,a+1);
        ps.setInt(2,a);
        int updated = ps.executeUpdate();
        Assert.assertEquals("Did not update the proper number of rows!",1l,updated);

        //commit conn1, then use conn2 to ensure that it's visible
        conn1.commit(); conn2.commit();

        ps = conn2.prepareStatement("insert into "+ table +" (a,b) values (?,?)");
        ps.setInt(1,a+1);ps.setInt(2,b);
        try{
            ps.executeUpdate();
            Assert.fail("Should have received a PK violation!");
        }catch(SQLException se){
            System.out.printf("%s:%s%n",se.getSQLState(),se.getMessage());
            Assert.assertEquals("Did not get PK violation", SQLState.LANG_DUPLICATE_KEY_CONSTRAINT,se.getSQLState());
        }
    }

    @Test
    public void testUpdateRollbacksAreNeverVisible() throws Exception {
        int a = 6;
        int b = 6;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        ps.setInt(1, a); ps.setInt(2, b); ps.execute();
        conn1.commit(); conn2.commit(); //roll both connections forward so data is visible

        //issue update, then roll it back, and see who's visible
        ps = conn1.prepareStatement("update "+ table+" set b = ? where a = ?");
        ps.setInt(1,b+1);ps.setInt(2,a); ps.execute();

        conn1.rollback();

        long count = conn1.count("select * from "+ table+ " where a = "+ a +" and b = "+ (b+1));
        Assert.assertEquals("rolled-back Update is still visible!",0l,count);
    }

    @Test
    public void testInsertConflictsWithUpdate() throws Exception {
        int a = 8;
        int b = 8;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        ps.setInt(1, a); ps.setInt(2, b); ps.execute();

        long conn1Count = conn1.count("select * from "+ table+" where a = "+a);
        Assert.assertEquals("Rows are not visible to my own transaction!",1l,conn1Count);

        long conn2Count = conn2.count("select * from " + table + " where a = "+a);
        Assert.assertEquals("Rows are visible that aren't supposed to be!", 0, conn2Count);

        //issue an update with conn2
        ps = conn2.prepareStatement("update "+ table+" set b = ? where a = ?");
        ps.setInt(1,b+1); ps.setInt(2,a);
        int updated =ps.executeUpdate(); //should not throw a write/write conflict--should just report 0 rows updated
        Assert.assertEquals("Update found rows!",0,updated);
    }

    @Test
    public void testUpdateConflictsWithInsert() throws Exception {
        int a = 10;
        int b = 10;
        PreparedStatement ps = conn1.prepareStatement("insert into " + table + " (a,b) values (?,?)");
        ps.setInt(1, a); ps.setInt(2, b); ps.execute();
        conn1.commit(); conn2.commit(); //make insert visible


        PreparedStatement insertPs = ps;
        //issue an update with conn2
        ps = conn2.prepareStatement("update "+ table+" set b = ? where a = ?");
        ps.setInt(1,b+1); ps.setInt(2,a);
        int updateCount = ps.executeUpdate(); //should not throw WWConflict
        Assert.assertEquals("Incorrect update count",1l,updateCount);

        //attempt insert
        insertPs.setInt(1,a);
        insertPs.setInt(2,b+1);

        try{
            insertPs.executeUpdate(); //should throw WWConflict
            Assert.fail("Did not see a write write conflict");
        }catch(SQLException se){
            System.out.printf("%s:%s%n",se.getSQLState(),se.getMessage());
            //SE014 = ErrorState.WRITE_WRITE_CONFLICT
            Assert.assertEquals("Incorrect error message!","SE014",se.getSQLState());
        }
    }

}
