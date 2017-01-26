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

package com.splicemachine.derby.test;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_dao.TableDAO;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Transaction operation test cases
 * 
 * @author jessiezhang
 */
public class TransactionIT extends SpliceUnitTest { 

    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    private static final String CLASS_NAME = TransactionIT.class.getSimpleName().toUpperCase()+ "_1";
    private static final String TABLE_NAME_1 = "A";
    private static final String TABLE_NAME_2 = "B";
    private static final String TABLE_NAME_3 = "C";
    private static final String TABLE_NAME_4 = "D";
    private static final String TABLE_NAME_5 = "E";
    private static final String TABLE_NAME_6 = "F";
    private static final String TABLE_NAME_7 = "G";
    private static final String TABLE_NAME_8 = "H";
    private static final String TABLE_NAME_9 = "I";
    private static final String TABLE_NAME_10 = "J";
    private static final String TABLE_NAME_11 = "K";
    private static final String TABLE_NAME_12 = "L";
    private static final String TABLE_NAME_13 = "M";
    private static final String TABLE_NAME_14 = "N";
    private static final String TABLE_NAME_15 = "O";
    private static final String TABLE_NAME_16 = "P";
    private static final String TABLE_NAME_17 = "Q";
    private static final String TABLE_NAME_18 = "R";
    private static final String TABLE_NAME_19 = "S";

	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    public String getTableReference(String tableName) {
        return CLASS_NAME + "." + tableName;
    }

    @ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private TableDAO tableDAO;
    
    @Before
    public void initTableDAO() throws Exception {
        tableDAO = new TableDAO(methodWatcher.getOrCreateConnection());
    }
    
	@Test
	public void testPreparedStatementAutoCommitOn() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_1);
		methodWatcher.setAutoCommit(true);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (i int, j varchar(10))", this.getTableReference(TABLE_NAME_1)));
		PreparedStatement psc = methodWatcher.prepareStatement(format("insert into %s values (?,?)",this.getTableReference(TABLE_NAME_1)));
		for (int i =0; i< 2; i++) {
			psc.setInt(1, i);
			psc.setString(2, "i");
			psc.executeUpdate();
		}
		ResultSet rs = methodWatcher.executeQuery(format("select count(*) from %s", this.getTableReference(TABLE_NAME_1)));
		rs.next();
		Assert.assertEquals(2, rs.getInt(1));	
	}
	
	@Test
	public void testPreparedStatementAutoCommitOff() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_2);
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (i int, j varchar(10))", this.getTableReference(TABLE_NAME_2)));
		methodWatcher.commit();
		PreparedStatement psc = methodWatcher.prepareStatement(format("insert into %s values (?,?)",this.getTableReference(TABLE_NAME_2)));
		for (int i =0; i< 2; i++) {
			psc.setInt(1, i);
			psc.setString(2, "i");
			psc.executeUpdate();
		}
		methodWatcher.commit();
		ResultSet rs = s.executeQuery(format("select count(*) from %s",this.getTableReference(TABLE_NAME_2)));
		rs.next();
		Assert.assertEquals(2, rs.getInt(1));	
		methodWatcher.commit();
	}

    /*
        This test is specifically testing for the ability to drop a table in a transaction,
        which requires transactional DDL and needs to be revisited once we support it.
     */
	@Test
	public void testCreateDrop() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_3);
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (i int, j varchar(10))", this.getTableReference(TABLE_NAME_3)));

        methodWatcher.commit();

        s.execute(format("drop table %s", this.getTableReference(TABLE_NAME_3)));

        methodWatcher.commit();

        ResultSet rs = methodWatcher.createConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_3, null);
		if (rs.next())
			Assert.assertTrue("The rolled back table exists in the dictionary!",false);
	}

	@Test
	public void testCommitCreate() throws Exception { // TODO What is it that we are testing?
		tableDAO.drop(CLASS_NAME, TABLE_NAME_4);
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_4)));
		methodWatcher.commit();
		ResultSet rs = methodWatcher.createConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_4, null);
		if (!rs.next())
			Assert.assertTrue("The table does not exist in the dictionary!",false);
	}	
	
	@Test
	public void testCommitNonCommitInsert() throws Exception {
			tableDAO.drop(CLASS_NAME, TABLE_NAME_5);
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_5)));

            methodWatcher.commit();

            s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_5)));
			s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_5)));
			s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_5)));
			s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_5)));
			s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_5)));
			s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_5)));
			methodWatcher.commit();
			s.execute(format("insert into %s(num) values(700)",this.getTableReference(TABLE_NAME_5)));			
			ResultSet rs = s.executeQuery(format("select * from %s",this.getTableReference(TABLE_NAME_5)));
			int i = 0;
			while (rs.next()) {
				i++;
			}	
			Assert.assertEquals(7, i);					
	}

    @Test
    public void testTransactionDDLCommitNonCommitInsert() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_6);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_6)));
        s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_6)));
        methodWatcher.commit();
        s.execute(format("insert into %s(num) values(700)",this.getTableReference(TABLE_NAME_6)));
        ResultSet rs = s.executeQuery(format("select * from %s",this.getTableReference(TABLE_NAME_6)));
        int i = 0;
        while (rs.next()) {
            i++;
        }
        Assert.assertEquals(7, i);
    }


    @Test(expected=SQLException.class)
	public void testRollbackCreate() throws Exception { 
		tableDAO.drop(CLASS_NAME, TABLE_NAME_7);

			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s (num int)",this.getTableReference(TABLE_NAME_7)));
			methodWatcher.rollback();
			ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_7, null);
			if (rs.next())
				Assert.assertTrue("The rolled back table exists in the dictionary!",false);
			s.execute(format("insert into %s values(100)",this.getTableReference(TABLE_NAME_7)));
	}

    @Test
    public void testUpdateRollback() throws Exception {
        tableDAO.drop(CLASS_NAME, TABLE_NAME_8);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_8)));

        methodWatcher.commit();

        s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_8)));
        s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_8)));
        s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_8)));
        s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_8)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_8)));
        s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_8)));
        methodWatcher.commit();

        s.executeUpdate(format("update %s set addr='rolled back address' where num=400",this.getTableReference(TABLE_NAME_8)));
        ResultSet rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_8)));
        if (rs.next()) {
            Assert.assertEquals("rolled back address", rs.getString(1));
        }
        methodWatcher.rollback();
        rs = methodWatcher.executeQuery(format("select num, addr from %s where num=400",this.getTableReference(TABLE_NAME_8)));
        if (rs.next()) {
            Assert.assertEquals(400, rs.getInt(1));
            Assert.assertEquals("400: 182 Second St.", rs.getString(2));
        }
    }

    @Test
    public void testTransactionalDDLUpdateRollback() throws Exception {
        tableDAO.drop(CLASS_NAME, TABLE_NAME_9);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_9)));
        methodWatcher.commit();
        s.executeUpdate(format("update %s set addr='rolled back address' where num=400",this.getTableReference(TABLE_NAME_9)));
        ResultSet rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_9)));
        if (rs.next()) {
            Assert.assertTrue("rolled back address".equals(rs.getString(1)));
        }
        methodWatcher.rollback();
        rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_9)));
        if (rs.next()) {
            Assert.assertTrue(!"rolled back address".equals(rs.getString(1)));
        }
    }

    @Test
    public void testRollbackCreateInsertDelete() throws Exception {
        tableDAO.drop(CLASS_NAME, TABLE_NAME_19);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50))",this.getTableReference(TABLE_NAME_19)));
        s.execute(format("insert into %s values(100, '100RB: 101 California St')",this.getTableReference(TABLE_NAME_19)));

        methodWatcher.setAutoCommit(false);
        s.execute(format("delete from %s",this.getTableReference(TABLE_NAME_19)));
        ResultSet rs = s.executeQuery(format("select num from %s",this.getTableReference(TABLE_NAME_19)));
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getInt(1));
        }
        Assert.assertEquals(0, j);
        methodWatcher.rollback();

        rs = s.executeQuery(format("select num from %s",this.getTableReference(TABLE_NAME_19)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getInt(1));
        }
        Assert.assertEquals(1, j);
    }

    @Test
    public void testRollbackCreateInsert() throws Exception {
        tableDAO.drop(CLASS_NAME, TABLE_NAME_10);

        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50))",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values(100, '100RB: 101 Califronia St')",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values(200, '200RB: 908 Glade Ct.')",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values(300, '300RB: my addr')",this.getTableReference(TABLE_NAME_10)));;
        s.execute(format("insert into %s values(400, '400RB: 182 Second St.')",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values(600, 'new addr')",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s(num) values(700)",this.getTableReference(TABLE_NAME_10)));
        methodWatcher.rollback();
        ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_10, null);
        if (rs.next())
            Assert.assertTrue("The rolled back table exists in the dictionary!",false);
    }

    @Test
	public void testTransactionalSelectString() throws Exception {	
		tableDAO.drop(CLASS_NAME, TABLE_NAME_11);
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_11)));

        methodWatcher.commit();

        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_11)));
		s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_11)));	
		methodWatcher.commit();
		s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)",this.getTableReference(TABLE_NAME_11)));
		ResultSet rs = s.executeQuery(format("select name from %S",this.getTableReference(TABLE_NAME_11)));
		
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(9, j);
		methodWatcher.rollback();	
		rs = s.executeQuery(format("select name from %s",this.getTableReference(TABLE_NAME_11)));
		j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(8, j);
	}

    @Test
    public void testTransactionaDDLlSelectString() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_12);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_12)));
        methodWatcher.commit();
        s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)",this.getTableReference(TABLE_NAME_12)));
        ResultSet rs = s.executeQuery(format("select name from %S",this.getTableReference(TABLE_NAME_12)));

        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(9, j);
        methodWatcher.rollback();
        rs = s.executeQuery(format("select name from %s",this.getTableReference(TABLE_NAME_12)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(8, j);
    }

    @Test
	public void testTransactionalDDLSinkOperationResultSets() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_13);
			methodWatcher.setAutoCommit(false);
            Statement s = methodWatcher.getStatement();

            s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_13)));
			s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_13)));
			methodWatcher.commit();
			s = methodWatcher.getStatement();
			s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)", this.getTableReference(TABLE_NAME_13)));
			ResultSet rs = s.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_13)));
			int j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(8, j);
			s.execute(format("insert into %s values('Nimoy, Leonard', 10)",this.getTableReference(TABLE_NAME_13)));
			rs = s.executeQuery(format("select name, count(empId) from %s group by name",this.getTableReference(TABLE_NAME_13)));
			j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
				if ("Nimoy, Leonard".equals(rs.getString(1)))
					Assert.assertEquals(3, rs.getInt(2));
			}	
			Assert.assertEquals(8, j);
			methodWatcher.rollback();			
			rs = methodWatcher.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_13)));
			j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(7, j);			
			rs = s.executeQuery(format("select name, count(empId) from %s group by name", this.getTableReference(TABLE_NAME_13)));
			j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
				if ("Nimoy, Leonard".equals(rs.getString(1)))
					Assert.assertEquals(2, rs.getInt(2));
			}	
			Assert.assertEquals(7, j);
	}

    @Test
    public void testTransactionalSinkOperationResultSets() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_14);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();

        s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_14)));

        //Needed because the inserts don't see the created table (inside the same transaction)
        //This should go away once we support transactional DDL
        methodWatcher.commit();

        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_14)));
        s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_14)));
        methodWatcher.commit();

        s = methodWatcher.getStatement();
        s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)", this.getTableReference(TABLE_NAME_14)));
        ResultSet rs = s.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_14)));
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(8, j);
        s.execute(format("insert into %s values('Nimoy, Leonard', 10)",this.getTableReference(TABLE_NAME_14)));
        rs = s.executeQuery(format("select name, count(empId) from %s group by name",this.getTableReference(TABLE_NAME_14)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if ("Nimoy, Leonard".equals(rs.getString(1)))
                Assert.assertEquals(3, rs.getInt(2));
        }
        Assert.assertEquals(8, j);
        methodWatcher.rollback();

        rs = methodWatcher.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_14)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(7, j);
        rs = s.executeQuery(format("select name, count(empId) from %s group by name", this.getTableReference(TABLE_NAME_14)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if ("Nimoy, Leonard".equals(rs.getString(1)))
                Assert.assertEquals(2, rs.getInt(2));
        }
        Assert.assertEquals(7, j);
    }

	@Test
	public void testTrasactionalDDLFailedInsert() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_15);
		try {
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_15)));	
			s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_15)));
			s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_15)));
			s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_15)));
			s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_15)));
			s.execute(format("insert into %s(num) values('500c')",this.getTableReference(TABLE_NAME_15)));
		} catch (SQLException e) {
			methodWatcher.rollback();
		}
	}

    @Test
    public void testFailedInsert() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_16);
        try {
            methodWatcher.setAutoCommit(false);
            Statement s = methodWatcher.getStatement();
            s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_16)));

            methodWatcher.commit();

            s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_16)));
            s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_16)));
            s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_16)));
            s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_16)));
            s.execute(format("insert into %s(num) values('500c')",this.getTableReference(TABLE_NAME_16)));
        } catch (SQLException e) {
            methodWatcher.rollback();
        }
    }

    @Test
	public void testAlterTableTrasactionalDDLAddColumn() throws Exception {
		tableDAO.drop(CLASS_NAME, TABLE_NAME_17);
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_17)));	
			s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_17)));
			s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_17)));
			s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_17)));
			s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_17)));
			s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_17)));
			methodWatcher.commit();
			s.execute(format("Alter table %s add column salary float default 0.0",this.getTableReference(TABLE_NAME_17)));	
			s.execute(format("update %s set salary=1000.0 where zip='94114'",this.getTableReference(TABLE_NAME_17)));	
			s.execute(format("update %s set salary=5000.85 where zip='94509'",this.getTableReference(TABLE_NAME_17)));	
			ResultSet rs = s.executeQuery(format("select zip, salary from %s where salary > 0",this.getTableReference(TABLE_NAME_17)));
			int count = 0;
			while (rs.next()) {
				count++;
				Assert.assertNotNull("Salary is null!",rs.getFloat(2));
			}
			Assert.assertEquals("Salary Cannot Be Queried after added!", 3,count);
	}


    @Test
    public void testAlterTableAddColumn() throws Exception {
        tableDAO.drop(CLASS_NAME, TABLE_NAME_18);
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_18)));

        methodWatcher.commit();

        s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_18)));
        s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_18)));
        s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_18)));
        s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_18)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_18)));

        methodWatcher.commit();

        s.execute(format("Alter table %s add column salary float default 0.0",this.getTableReference(TABLE_NAME_18)));

        methodWatcher.commit();

        s.execute(format("update %s set salary=1000.0 where zip='94114'",this.getTableReference(TABLE_NAME_18)));
        s.execute(format("update %s set salary=5000.85 where zip='94509'",this.getTableReference(TABLE_NAME_18)));
        ResultSet rs = s.executeQuery(format("select zip, salary from %s where salary > 0",this.getTableReference(TABLE_NAME_18)));
        int count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull("Salary is null!",rs.getFloat(2));
        }
        Assert.assertEquals("Salary Cannot Be Queried after added!", 3,count);
    }

}
