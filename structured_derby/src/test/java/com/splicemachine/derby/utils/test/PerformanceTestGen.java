package com.splicemachine.derby.utils.test;

import com.splicemachine.derby.test.SpliceDerbyTest;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.log4j.Logger;
import org.junit.*;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

@Ignore("Takes too long to run")
public class PerformanceTestGen extends SpliceDerbyTest{
	private static final Logger LOG = Logger.getLogger(PerformanceTestGen.class);
	protected static List<String> names = Arrays.asList("John","Monte","Gene","Scott","Jessie");
 
	@BeforeClass
	public static void startup() throws Exception {
		startConnection();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		stopConnection();
	}
	@Test
	public void loadTest() throws Exception {
		startConnection();
		Statement s = null;
		try {
			conn.setAutoCommit(false);
//			s = conn.createStatement();
//			s.execute("create table t (a int, b int, c varchar(40))");
//			s.execute("create table j (a int, b int, c varchar(40))");
			conn.commit();	
			PreparedStatement ps = conn.prepareStatement("insert into t (a,b,c) values (?,?,?)");
			PreparedStatement ps2 = conn.prepareStatement("insert into j (a,b,c) values (?,?,?)");
			for (int i = 0; i <= 1000000;i++) {
				ps.setInt(1, i);
				ps.setInt(2, i%50);
				ps.setString(3, names.get(i%5));
				ps2.setInt(1, i);
				ps2.setInt(2, i%50);
				ps2.setString(3, names.get(i%5));				
				ps.executeUpdate();
				ps2.executeUpdate();
			}			
			HTableInterface face;
			conn.commit();
		}catch(SQLException se){
			conn.rollback();
			se.printStackTrace();
			Assert.fail(se.getMessage());
		}finally{
			try {
				if(s !=null);
					s.close();
			} catch(SQLException e){ /*do nothing*/ }
		}
		
	}
}
