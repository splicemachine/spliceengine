package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.test.SpliceDerbyTest;
import com.splicemachine.derby.utils.SpliceUtils;

@Ignore("Ignored until implemented")
public class RowCountOperationTest extends SpliceDerbyTest{
	private static final Logger LOG = Logger.getLogger(RowCountOperationTest.class);

	@BeforeClass
	public static void startup() throws Exception {
		startConnection();
		setupTest();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		tearDownTest();
		stopConnection();
	}
	
	public static void setupTest() throws Exception {
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("create table t (i int)");			//			conn.commit();
		
			//get the conglomerate ID from ZooKeeper
			long conglomID = SpliceUtils.getHighestConglomSequence();
			String name = Bytes.toString(SpliceAccessManager.getHTable(conglomID).getTableName());
			System.err.println("table id="+name);
			
			s = conn.createStatement();
			s.execute("insert into t values 1");
			s.execute("insert into t values 2");
			s.execute("insert into t values 3");
			s.execute("insert into t values 4");
			s.execute("insert into t values 5");
			s.execute("insert into t values 6");
			s.execute("insert into t values 7");
			s.execute("insert into t values 8");
			s.execute("insert into t values 9");
			s.execute("insert into t values 10");
			
		}catch(SQLException se){
			se.printStackTrace();
			Assert.fail(se.getMessage());
		}finally{
			try {
				if(s !=null);
					s.close();
			} catch(SQLException e){ /*do nothing*/ }
		}
		
	}

	public static void tearDownTest() throws Exception {
		Statement s = null;
		try{
			s = conn.createStatement();
			s.execute("drop table t");
		}catch(SQLException se){
			se.printStackTrace();
			Assert.fail(se.getMessage());
		}finally{
			if(s !=null)
				s.close();
		}
	}
	
	/*  Per RowCountNode.java, the RowCounts are for the following types of conditions:
	    SELECT * FROM T FETCH FIRST ROW ONLY
		SELECT * FROM T ORDER BY I OFFSET 10 ROWS FETCH NEXT 10 ROWS ONLY
		SELECT * FROM T OFFSET 100 ROWS
	 */
	
	@Test
	public void testCountOffsetFirstRow() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = conn.createStatement();
			rs = s.executeQuery("select * from t fetch first row only");
			int i=0;
			while(rs.next()){
				i++;
				Assert.assertEquals(i,rs.getInt(1));
			}
			//ensure that only a single row comes back
			Assert.assertEquals(1, i);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	
	}

	
	@Test
	public void testCountOffset() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = conn.createStatement();
			rs = s.executeQuery("select * from t offset 5 rows");
			int i=5;
			while(rs.next()){
				i++;
				Assert.assertEquals(i,rs.getInt(1));
			}
			// assert that only rows 6-10 come back
			Assert.assertEquals(10, i);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	
	}
	
	@Test
	public void testOffsetFetchNext() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = conn.createStatement();
			rs = s.executeQuery("select * from t offset 3 rows fetch next 2 rows only");
			int i=3;
			while(rs.next()){
				i++;
				Assert.assertEquals(i,rs.getInt(1));
			}
			//ensure that only rows 4 and 5 come back
			Assert.assertEquals(5, i);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	
}
