package com.splicemachine.derby.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * This test class is to emulate multiple user case in embeded env.
 * 
 * @author jessiezhang
 */

public class MultiThreadTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(MultiThreadTest.class);

	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();		
		conn.setAutoCommit(true);

		Statement  s = null;
		try {
			s = conn.createStatement();
			s.execute("create table foo(si varchar(40), id int)");	
			for (int i =1; i<= 10; i++) {
				if (i%3 == 0)
					s.execute("insert into foo values('3'," + i+")");
				else if (i%4 == 0)
					s.execute("insert into foo values('4'," + i+")");
				else
					s.execute("insert into foo values('" + i + "',"+i+")");
			}

			s.execute("create table foobar (name varchar(40), id int)");
			s.execute("insert into foobar values('Mulgrew, Kate', 1)");
			s.execute("insert into foobar values('Shatner, William', 2)");
			s.execute("insert into foobar values('Nimoy, Leonard', 3)");
			s.execute("insert into foobar values('Stewart, Patrick', 4)");
			s.execute("insert into foobar values('Spiner, Brent', 5)");
			s.execute("insert into foobar values('Duncan, Rebort', 6)");
			s.execute("insert into foobar values('Nimoy, Leonard', 7)");
			s.execute("insert into foobar values('Ryan, Jeri', 8)");
			LOG.info("finish creating tables");
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (s != null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}

	@Test
	@Ignore
	public void testConcurrentStatements() throws SQLException {
		Thread thread1 = new SelectRecords(conn);
		thread1.start();

		Thread thread2 = new JoinRecords(conn);
		thread2.start();
	}

	@Test
	@Ignore
	public void testConcurrentConnections() throws SQLException {
		Thread thread1 = new SelectRecords();
		thread1.start();

		Thread thread2 = new JoinRecords();
		thread2.start();
	}

	public static class SelectRecords extends Thread
	{
		Connection conn1 = null;
		boolean newConn = false;
		public SelectRecords() {
			try {
				conn1 = DriverManager.getConnection(protocol + dbName + ";create=true", props);
				newConn = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public SelectRecords(Connection connection) {
			conn1 = connection;
		}
		public void run()
		{
			LOG.info("start running thread to SelectRecords");
			Statement s = null;
			ResultSet rs = null;
			try {
				LOG.info("start creating statement for SelectRecords");
				s = conn1.createStatement();
				LOG.info("start doing select");
				rs = s.executeQuery("select id, name from foobar order by name");
				while(rs.next()) {
					LOG.info("in Select Records, id="+rs.getInt(1)+",name="+rs.getString(2));
				}
				LOG.info("finish selecting");
			} catch (SQLException e) {
				LOG.error("error during select records-"+e.getMessage(), e);
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					s.close();
					if (newConn && conn1 != null)
						conn1.close();
				} catch (SQLException e) {
					//no need to print out
				}
			}
		}
	}

	public static class JoinRecords extends Thread
	{
		Connection conn1 = null;
		boolean newConn = false;
		public JoinRecords() {
			try {
				conn1 = DriverManager.getConnection(protocol + dbName + ";create=true", props);
				newConn = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public JoinRecords(Connection connection) {
			conn1 = connection;
		}
		public void run()
		{
			LOG.info("start running thread to JoinRecords");
			Statement s = null;
			ResultSet rs = null;
			try {
				LOG.info("start creating statement for JoinRecords");
				s = conn1.createStatement();
				LOG.info("start left outer join");
				rs = s.executeQuery("select foo.id, foobar.name from foo left outer join foobar on foo.id=foobar.id");
				while(rs.next()) {
					LOG.info("in Join Records, id="+rs.getInt(1)+",name="+rs.getString(2));
				}
				LOG.info("finish joining");
			} catch (SQLException e) {
				LOG.error("error during join tables-"+e.getMessage(), e);
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					s.close();
					if (newConn && conn1 != null)
						conn1.close();
				} catch (SQLException e) {
					//no need to print out
				}
			}
		}
	}

	public static class UpdateRecords extends Thread
	{
		Connection conn1 = null;
		boolean newConn = false;
		public UpdateRecords() {
			try {
				conn1 = DriverManager.getConnection(protocol + dbName + ";create=true", props);
				newConn = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public UpdateRecords(Connection connection) {
			conn1 = connection;
		}
		public void run()
		{
			LOG.info("start running thread to UpdateRecords");
			Statement s = null;
			try {
				LOG.info("start creating statement for UpdateRecords");
				s = conn1.createStatement();
				LOG.info("start doing update");
				s.execute("update foobar set name='Duncan, Data' where id=6");
				LOG.info("finish updating");
			} catch (SQLException e) {
				LOG.error("error during updating records-"+e.getMessage(), e);
				e.printStackTrace();
			} finally {
				try {
					s.close();
					if (newConn && conn1 != null)
						conn1.close();
				} catch (SQLException e) {
					//no need to print out
				}
			}
		}
	}

	public static class InsertRecords extends Thread
	{
		Connection conn1 = null;
		boolean newConn = false;
		public InsertRecords() {
			try {
				conn1 = DriverManager.getConnection(protocol + dbName + ";create=true", props);
				newConn = true;
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		public InsertRecords(Connection connection) {
			conn1 = connection;
		}
		public void run()
		{
			LOG.info("start running thread to InsertRecords");
			Statement s = null;
			ResultSet rs = null;
			try {
				LOG.info("start creating statement for InsertRecords");
				s = conn1.createStatement();
				LOG.info("start inserting");
				s.execute("insert into foobar values('Ricker, Will', 9)");
				LOG.info("finish inserting");
			} catch (SQLException e) {
				LOG.error("error during inserting-"+e.getMessage(), e);
				e.printStackTrace();
			} finally {
				try {
					rs.close();
					s.close();
					if (newConn && conn1 != null)
						conn1.close();
				} catch (SQLException e) {
					//no need to print out
				}
			}
		}
	}


	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("foo") ;
		dropTable("foobar") ;
		stopConnection();		
	}

}
