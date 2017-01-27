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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.ResultSet;
import java.sql.Savepoint;
import java.sql.Statement;

/**
 *
 * @author Jeff Cunningham
 *         Date: 7/17/13
 */
public class SavepointConstantOperationIT { 
    public static final String CLASS_NAME = SavepointConstantOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher classWatcher = new SpliceWatcher();
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    public static final String TABLE_NAME_1 = "B";

    private static String tableDef = "(TaskId INT NOT NULL)";
    protected static SpliceTableWatcher b= new SpliceTableWatcher(TABLE_NAME_1, CLASS_NAME, tableDef);
    protected static SpliceTableWatcher t= new SpliceTableWatcher("T", CLASS_NAME, "(a int)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(classWatcher)
            .around(spliceSchemaWatcher)
            .around(b)
            .around(t);

    private static TestConnection conn1;
    private static TestConnection conn2;

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
    }

    @Test
    public void testCanCommitOverActiveSavepoint() throws Exception{
        Savepoint s = conn1.setSavepoint("pish");
        try(Statement statement =conn1.createStatement()){
            statement.executeUpdate("insert into "+t+" values 1,2,3");
        }
        conn1.commit();
        boolean[] found = new boolean[3];
        try(Statement statement = conn1.createStatement()){
            try(ResultSet rs = statement.executeQuery("select * from "+t)){
                while(rs.next()){
                    int i=rs.getInt(1);
                    Assert.assertFalse("Returned a null value!",rs.wasNull());
                    Assert.assertFalse("Already seen value:"+i,found[i-1]);
                    found[i-1]=true;
                }
            }
        }

        for(int i=0;i<found.length;i++){
            Assert.assertTrue("row:"+(i+1)+" is missing!",found[i]);
        }
    }

    @Test
    public void testCanSetAndReleaseASavepoint() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 1;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        conn1.releaseSavepoint(savepoint);
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);
    }

    @Test
    public void testReleasingASavepointDoesNotCommitData() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 6;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));

        long count = conn2.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Data is visible to another transaction!",0l,count);

        conn1.releaseSavepoint(savepoint);
        count = conn2.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Data was committed during savepoint release!",0l,count);
    }

    @Test
    public void testRollingBackASavepointMakesDataInvisibleToMyself() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 2;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",0l,count);
    }

    @Test
    public void testCanReleaseNonImmediateSavepoint() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 3;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        //try releasing the first savepoint without first releasing the second, and make sure that it still works
        conn1.releaseSavepoint(s1);
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisible() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testRollingBackANonImmediateSavepointMakesDataInvisibleEvenIfOtherSavepointIsReleased() throws Exception {
        Savepoint s1 = conn1.setSavepoint("test");
        int value = 4;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        Savepoint s2 = conn1.setSavepoint("test2");
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b, value));

        conn1.releaseSavepoint(s2);
        //make sure data looks like what we expect
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 2l, count);

        //rollback s1 and make sure that all data is invisible
        conn1.rollback(s1);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!", 0l, count);
    }

    @Test
    public void testCanRollbackThenReleaseASavepointAndDataIsCorrect() throws Exception {
        Savepoint savepoint = conn1.setSavepoint("test");
        int value = 7;
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));
        long count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);

        conn1.rollback(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",0l,count);

        //insert some data again
        conn1.execute(String.format("insert into %s (taskid) values (%d)",b,value));

        //now release the savepoint
        conn1.releaseSavepoint(savepoint);
        count = conn1.count(String.format("select * from %s where taskid=%d",b,value));
        Assert.assertEquals("Incorrect count after savepoint release!",1l,count);


    }
}
