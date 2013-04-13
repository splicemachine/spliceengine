package com.splicemachine.derby.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

/**
 * Transaction operation test cases
 * 
 * @author jessiezhang
 */

public class TransactionTest extends SpliceUnitTest {
	protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
	public static final String CLASS_NAME = TransactionTest.class.getSimpleName().toUpperCase() + "_1";
	public static final String TABLE_NAME_1 = "A";
	public static final String TABLE_NAME_2 = "B";
	public static final String TABLE_NAME_3 = "C";
	public static final String TABLE_NAME_4 = "D";
	public static final String TABLE_NAME_5 = "E";
	public static final String TABLE_NAME_6 = "F";
	public static final String TABLE_NAME_7 = "G";
	public static final String TABLE_NAME_8 = "H";
	public static final String TABLE_NAME_9 = "I";
	public static final String TABLE_NAME_10 = "J";
	public static final String TABLE_NAME_11 = "K";
	public static final String TABLE_NAME_12 = "L";
	
	
	protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    public String getTableReference(String tableName) {
        return CLASS_NAME + "." + tableName;
    }


    @ClassRule
	public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
		.around(spliceSchemaWatcher);
	
	@Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

	private static Logger LOG = Logger.getLogger(TransactionTest.class);
	
	@Test
	public void testPreparedStatementAutoCommitOn() throws Exception {
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

    @Ignore("Waiting for transactional DDL - bug 349")
    @Test
    public void testTransactionDDLCommitNonCommitInsert() throws Exception {
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_5)));
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


    @Test(expected=SQLException.class)
	@Ignore("Bug 337")
	public void testRollbackCreate() throws Exception { 
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s (num int)",this.getTableReference(TABLE_NAME_6)));
			methodWatcher.rollback();
			ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_6, null);
			if (rs.next())
				Assert.assertTrue("The rolled back table exists in the dictionary!",false);
			s.execute(format("insert into %s values(100)",this.getTableReference(TABLE_NAME_6)));
	}

	@Test
	public void testUpdateRollback() throws Exception {
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_7)));

            methodWatcher.commit();

			s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_7)));
			s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_7)));
			s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_7)));
			s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_7)));
			s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_7)));
			s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_7)));
			methodWatcher.commit();
			s.executeUpdate(format("update %s set addr='rolled back address' where num=400",this.getTableReference(TABLE_NAME_7)));
			ResultSet rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_7)));
			if (rs.next()) {
				Assert.assertEquals("rolled back address", rs.getString(1));
			}	
			methodWatcher.rollback();
			rs = methodWatcher.executeQuery(format("select num, addr from %s where num=400",this.getTableReference(TABLE_NAME_7)));
			if (rs.next()) {
                Assert.assertEquals(400, rs.getInt(1));
                Assert.assertEquals("400: 182 Second St.", rs.getString(2));
			}	
	}

    @Ignore("Waiting for transactional DDL - bug 349")
    @Test
    public void testTransactionalDDLUpdateRollback() throws Exception {
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (num int, addr varchar(50), zip char(5))", this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s values(100, '100: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s values(200, '200: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s values(300, '300: my addr', '34166')",this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s values(400, '400: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_7)));
        s.execute(format("insert into %s values(600, 'new addr', '34166')",this.getTableReference(TABLE_NAME_7)));
        methodWatcher.commit();
        s.executeUpdate(format("update %s set addr='rolled back address' where num=400",this.getTableReference(TABLE_NAME_7)));
        ResultSet rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_7)));
        if (rs.next()) {
            Assert.assertTrue("rolled back address".equals(rs.getString(1)));
        }
        methodWatcher.rollback();
        rs = methodWatcher.executeQuery(format("select addr from %s where num=400",this.getTableReference(TABLE_NAME_7)));
        if (rs.next()) {
            Assert.assertTrue(!"rolled back address".equals(rs.getString(1)));
        }
    }


	@Test
	@Ignore("Bug 337")
	public void testRollbackCreateInsert() throws Exception { 
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s (num int, addr varchar(50))",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s values(100, '100RB: 101 Califronia St')",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s values(200, '200RB: 908 Glade Ct.')",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s values(300, '300RB: my addr')",this.getTableReference(TABLE_NAME_8)));;
			s.execute(format("insert into %s values(400, '400RB: 182 Second St.')",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s values(600, 'new addr')",this.getTableReference(TABLE_NAME_8)));
			s.execute(format("insert into %s(num) values(700)",this.getTableReference(TABLE_NAME_8)));
			methodWatcher.rollback();
			ResultSet rs = methodWatcher.getOrCreateConnection().getMetaData().getTables(null, CLASS_NAME, TABLE_NAME_8, null);
			if (rs.next())
				Assert.assertTrue("The rolled back table exists in the dictionary!",false);
	}	

	@Test
	public void testTransactionalSelectString() throws Exception {	
		methodWatcher.setAutoCommit(false);
		Statement s = methodWatcher.getStatement();
		s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_9)));

        methodWatcher.commit();

        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_9)));
		s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_9)));	
		methodWatcher.commit();
		s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)",this.getTableReference(TABLE_NAME_9)));
		ResultSet rs = s.executeQuery(format("select name from %S",this.getTableReference(TABLE_NAME_9)));
		
		int j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(9, j);
		methodWatcher.rollback();	
		rs = s.executeQuery(format("select name from %s",this.getTableReference(TABLE_NAME_9)));
		j = 0;
		while (rs.next()) {
			j++;
			Assert.assertNotNull(rs.getString(1));
		}	
		Assert.assertEquals(8, j);
	}

    @Ignore("Waiting for transactional DDL - bug 349")
    @Test
    public void testTransactionaDDLlSelectString() throws Exception {
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_9)));
        s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_9)));
        methodWatcher.commit();
        s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)",this.getTableReference(TABLE_NAME_9)));
        ResultSet rs = s.executeQuery(format("select name from %S",this.getTableReference(TABLE_NAME_9)));

        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(9, j);
        methodWatcher.rollback();
        rs = s.executeQuery(format("select name from %s",this.getTableReference(TABLE_NAME_9)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(8, j);
    }

    @Ignore("Waiting for transactional DDL - bug 349")
    @Test
	public void testTransactionalDDLSinkOperationResultSets() throws Exception {
			methodWatcher.setAutoCommit(false);
            Statement s = methodWatcher.getStatement();

            s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_10)));
			s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_10)));
			methodWatcher.commit();
			s = methodWatcher.getStatement();
			s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)", this.getTableReference(TABLE_NAME_10)));
			ResultSet rs = s.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_10)));
			int j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(8, j);
			s.execute(format("insert into %s values('Nimoy, Leonard', 10)",this.getTableReference(TABLE_NAME_10)));
			rs = s.executeQuery(format("select name, count(empId) from %s group by name",this.getTableReference(TABLE_NAME_10)));
			j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
				if ("Nimoy, Leonard".equals(rs.getString(1)))
					Assert.assertEquals(3, rs.getInt(2));
			}	
			Assert.assertEquals(8, j);
			methodWatcher.rollback();			
			rs = methodWatcher.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_10)));
			j = 0;
			while (rs.next()) {
				j++;
				Assert.assertNotNull(rs.getString(1));
			}	
			Assert.assertEquals(7, j);			
			rs = s.executeQuery(format("select name, count(empId) from %s group by name", this.getTableReference(TABLE_NAME_10)));
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
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();

        s.execute(format("create table %s (name varchar(40), empId int)",this.getTableReference(TABLE_NAME_10)));

        //Needed because the inserts don't see the created table (inside the same transaction)
        //This should go away once we support transactional DDL
        methodWatcher.commit();

        s.execute(format("insert into %s values('Mulgrew, Kate', 1)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Shatner, William', 2)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 3)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Stewart, Patrick', 4)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Spiner, Brent', 5)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Duncan, Rebort', 6)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Nimoy, Leonard', 7)",this.getTableReference(TABLE_NAME_10)));
        s.execute(format("insert into %s values('Ryan, Jeri', 8)",this.getTableReference(TABLE_NAME_10)));
        methodWatcher.commit();

        s = methodWatcher.getStatement();
        s.execute(format("insert into %s values('Noncommitted, Noncommitted', 9)", this.getTableReference(TABLE_NAME_10)));
        ResultSet rs = s.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_10)));
        int j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(8, j);
        s.execute(format("insert into %s values('Nimoy, Leonard', 10)",this.getTableReference(TABLE_NAME_10)));
        rs = s.executeQuery(format("select name, count(empId) from %s group by name",this.getTableReference(TABLE_NAME_10)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
            if ("Nimoy, Leonard".equals(rs.getString(1)))
                Assert.assertEquals(3, rs.getInt(2));
        }
        Assert.assertEquals(8, j);
        methodWatcher.rollback();

        rs = methodWatcher.executeQuery(format("select distinct name from %s",this.getTableReference(TABLE_NAME_10)));
        j = 0;
        while (rs.next()) {
            j++;
            Assert.assertNotNull(rs.getString(1));
        }
        Assert.assertEquals(7, j);
        rs = s.executeQuery(format("select name, count(empId) from %s group by name", this.getTableReference(TABLE_NAME_10)));
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
		try {
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_11)));	
			s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_11)));
			s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_11)));
			s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_11)));
			s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_11)));
			s.execute(format("insert into %s(num) values('500c')",this.getTableReference(TABLE_NAME_11)));
		} catch (SQLException e) {
			methodWatcher.rollback();
		}
	}

    @Test
    public void testFailedInsert() throws Exception {
        try {
            methodWatcher.setAutoCommit(false);
            Statement s = methodWatcher.getStatement();
            s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_11)));

            methodWatcher.commit();

            s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_11)));
            s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_11)));
            s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_11)));
            s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_11)));
            s.execute(format("insert into %s(num) values('500c')",this.getTableReference(TABLE_NAME_11)));
        } catch (SQLException e) {
            methodWatcher.rollback();
        }
    }

    @Ignore("Waiting for transactional DDL - bug 349")
    @Test
	public void testAlterTableTrasactionalDDLAddColumn() throws Exception {
			methodWatcher.setAutoCommit(false);
			Statement s = methodWatcher.getStatement();
			s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_12)));	
			s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_12)));
			s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_12)));
			s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_12)));
			s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_12)));
			s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_12)));
			methodWatcher.commit();
			s.execute(format("Alter table %s add column salary float default 0.0",this.getTableReference(TABLE_NAME_12)));	
			s.execute(format("update %s set salary=1000.0 where zip='94114'",this.getTableReference(TABLE_NAME_12)));	
			s.execute(format("update %s set salary=5000.85 where zip='94509'",this.getTableReference(TABLE_NAME_12)));	
			ResultSet rs = s.executeQuery(format("select zip, salary from %s where salary > 0",this.getTableReference(TABLE_NAME_12)));
			int count = 0;
			while (rs.next()) {
				count++;
				Assert.assertNotNull("Salary is null!",rs.getFloat(2));
			}
			Assert.assertEquals("Salary Cannot Be Queried after added!", 3,count);
	}


    @Test
    public void testAlterTableAddColumn() throws Exception {
        methodWatcher.setAutoCommit(false);
        Statement s = methodWatcher.getStatement();
        s.execute(format("create table %s(num int, addr varchar(50), zip char(5))",this.getTableReference(TABLE_NAME_12)));

        methodWatcher.commit();

        s.execute(format("insert into %s values(100, '100F: 101 Califronia St', '94114')",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values(200, '200F: 908 Glade Ct.', '94509')",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values(300, '300F: my addr', '34166')",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s values(400, '400F: 182 Second St.', '94114')",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("insert into %s(num) values(500)",this.getTableReference(TABLE_NAME_12)));

        methodWatcher.commit();

        s.execute(format("Alter table %s add column salary float default 0.0",this.getTableReference(TABLE_NAME_12)));

        methodWatcher.commit();

        s.execute(format("update %s set salary=1000.0 where zip='94114'",this.getTableReference(TABLE_NAME_12)));
        s.execute(format("update %s set salary=5000.85 where zip='94509'",this.getTableReference(TABLE_NAME_12)));
        ResultSet rs = s.executeQuery(format("select zip, salary from %s where salary > 0",this.getTableReference(TABLE_NAME_12)));
        int count = 0;
        while (rs.next()) {
            count++;
            Assert.assertNotNull("Salary is null!",rs.getFloat(2));
        }
        Assert.assertEquals("Salary Cannot Be Queried after added!", 3,count);
    }

}
