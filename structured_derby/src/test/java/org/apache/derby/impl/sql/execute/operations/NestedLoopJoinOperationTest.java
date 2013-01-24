package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.splicemachine.derby.test.SpliceDerbyTest;

public class NestedLoopJoinOperationTest extends SpliceDerbyTest {
	
	private int size = 10;
	private List<String[]> correctJoins;
	
	@BeforeClass
	public static void setup() throws Exception {
		startConnection();
	}
	
	@AfterClass
	public static void shutdown() throws Exception {
		stopConnection();
	}
	
	@Before
	public void setupTest() throws Exception {
		createTable("t_one","(name varchar(40),value varchar(255))");
		createTable("t_two","(name varchar(40),value2 varchar(255))");
		
		correctJoins = insertData();
		
		Collections.sort(correctJoins,nameComparator);
	}
	
	@After
	public void tearDown() throws Exception {
		dropTable("t_one","t_two");
	}
	
	@Test
	@Ignore("Probably not even using a NestedLoop Join")
	public void testNestedLoopJoin() throws Exception {
		ResultSet rs = null;
		try{
			rs = executeQuery("select a.name,a.value,b.name from t_one a inner join t_two b --DERBY-PROPERTIES joinStrategy=NESTEDLOOP \n"+
							  "on a.name = b.name");
			List<String[]> joins = new ArrayList<String[]>();
			while(rs.next()){
                String aName = rs.getString(1);
                String value = rs.getString(2);
                String bName = rs.getString(3);

                Assert.assertEquals("a.name!=b.name",aName,bName);
                Assert.assertNotNull("value is null!",value);
                joins.add(new String[]{aName,value,bName});
			}
			
			Collections.sort(joins,nameComparator);
            for(String[] correctJoin:correctJoins){
                boolean found= false;
                for(String[] join:joins){
                   if(correctJoin[0].equals(join[0])){
                       found=true;
                       break;
                   }
                }
                Assert.assertTrue("Could not find entry with name "+correctJoin[0],found);
            }
			Assert.assertEquals("Join returned incorrect number of entries!",correctJoins.size(), joins.size());
		}finally{
			if(rs!=null) rs.close();
		}
	}
	

	private static Comparator<String[]> nameComparator = new Comparator<String[]>(){
		@Override
		public int compare(String[] first, String[] second){
			return first[0].compareTo(second[0]);
		}
	};
	
	private List<String[]> insertData() throws Exception {
		List<String[]> rows = new ArrayList<String[]>(size);
		Statement s = null;
		try{
			for(int i=0;i<size;i++){
				s = conn.createStatement();
				String name = "jzhang";
				String v1 = Integer.toString(i);
				String v2 = Integer.toString(i*10); 
				s.execute("insert into t_one (name, value) values('"+name+"','"+v1+"')");
				s.execute("insert into t_two (name, value2) values('"+name+"','"+v2+"')");
				rows.add(new String[]{name,v1,v2});
			}
		}finally{
			if(s!=null) s.close();
		}
		return rows;
	}
	
	private static void dropTable(String... tableNames) throws Exception {
		Statement s=  null;
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			for(String tableName: tableNames){
				s.execute("drop table "+tableName);
			}
			conn.commit();
		}finally{
			if(s!=null)s.close();
		}
	}
	
	private static void createTable(String name, String schema) throws Exception{
		Statement s = null;
		try{
			conn.setAutoCommit(false);
			s = conn.createStatement();
			System.err.println("create table "+name +" "+schema);
			s.execute("create table "+ name+" "+schema);
			conn.commit();
		}finally{
			if(s!=null) s.close();
		}
	}
}
