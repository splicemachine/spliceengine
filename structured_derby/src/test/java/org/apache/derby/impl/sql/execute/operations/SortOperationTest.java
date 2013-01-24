package org.apache.derby.impl.sql.execute.operations;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import com.splicemachine.derby.test.SpliceDerbyTest;
import com.splicemachine.utils.SpliceLogUtils;


public class SortOperationTest {
	private static final Logger LOG = Logger.getLogger(SortOperationTest.class);
	
	private static Map<String,String> tableSchemaMap = Maps.newHashMap();
	static{
		tableSchemaMap.put("food", "name varchar(255),value1 varchar(255),value2 varchar(255)");
	}
	
	@Rule public static DerbyTestRule  rule = new DerbyTestRule(tableSchemaMap,false,LOG);
	
	private static List<Triplet> correctByValue1 = Lists.newArrayList();
 	private static List<Triplet> distinctCorrectByValue1 = Lists.newArrayList();
	private static List<Triplet> correctByAscDesc = Lists.newArrayList();
	private static List<Triplet> correctByDescAsc = Lists.newArrayList();
	private static List<Triplet> correctByName = Lists.newArrayList();
	private static List<Triplet> distinctCorrectByName = Lists.newArrayList();
	
	private static final Comparator<Triplet>k1Comparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2) {
			return o1.k1.compareTo(o2.k1);
		}
	};
	private static final Comparator<Triplet>k2Comparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2) {
			return o1.k2.compareTo(o2.k2);
		}
	};

	private static final Comparator<Triplet> ascDescComparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2){
			int compare = o1.k1.compareTo(o2.k1);
			if(compare ==0)
				compare = -1*o1.k2.compareTo(o2.k2);
			return compare;
		}
	};

	private static final Comparator<Triplet> descAscComparator = new Comparator<Triplet>(){
		@Override
		public int compare(Triplet o1, Triplet o2){
			int compare = -1*o1.k1.compareTo(o2.k1);
			if(compare ==0)
				compare = o1.k2.compareTo(o2.k2);
			return compare;
		}
	};
	

	@BeforeClass
	public static void startup() throws Exception {
		DerbyTestRule.start();
		rule.createTables();
		insertData();
	}

	@AfterClass
	public static void shutdown() throws Exception {
		rule.dropTables();
		DerbyTestRule.shutdown();
	}

	private static void insertData() throws SQLException {
		rule.setAutoCommit(true);
		Triplet triple = new Triplet("jzhang","pickles","greens");
		PreparedStatement ps = rule.prepareStatement("insert into food (name,value1,value2) values (?,?,?)");
		ps.setString(1, triple.k1);
		ps.setString(2, triple.k2);
		ps.setString(3, triple.k3);
		ps.executeUpdate();
		correctByValue1.add(triple);
		correctByAscDesc.add(triple);
		correctByDescAsc.add(triple);
		correctByName.add(triple);
		if(!distinctCorrectByValue1.contains(triple)) distinctCorrectByValue1.add(triple); 
		if(!distinctCorrectByName.contains(triple)) distinctCorrectByName.add(triple);

		Triplet t2 = new Triplet("sfines","turkey","apples");
		ps.setString(1, t2.k1);
		ps.setString(2, t2.k2);
		ps.setString(3, t2.k3);
		ps.executeUpdate();
		correctByValue1.add(t2);
		correctByAscDesc.add(t2);
		correctByDescAsc.add(t2);
		correctByName.add(t2);
		if(!distinctCorrectByValue1.contains(t2)) distinctCorrectByValue1.add(t2); 
		if(!distinctCorrectByName.contains(t2)) distinctCorrectByName.add(t2);

		Triplet t3 = new Triplet("jleach","roast beef","tacos");
		ps.setString(1, t3.k1);
		ps.setString(2, t3.k2);
		ps.setString(3, t3.k3);
		ps.executeUpdate();
		correctByValue1.add(t3);
		correctByAscDesc.add(t3);
		correctByDescAsc.add(t3);
		correctByName.add(t3);
		if(!distinctCorrectByValue1.contains(t3)) distinctCorrectByValue1.add(t3); 
		if(!distinctCorrectByName.contains(t3)) distinctCorrectByName.add(t3);
		Collections.sort(correctByValue1,k2Comparator);
		Collections.sort(correctByName,k1Comparator);
		Collections.sort(distinctCorrectByValue1,k2Comparator);
		Collections.sort(distinctCorrectByName,k1Comparator);
		Collections.sort(correctByAscDesc,ascDescComparator);
		Collections.sort(correctByDescAsc,descAscComparator);
		rule.commit();
	}

	@Test
	public void testSortOperationByKey1Ascending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
//		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by name");
			//need to check that elements are in order

			List<Triplet> returnedByName = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByName.add(triple);
			}
			SpliceLogUtils.info(LOG, "returnedByName=%s",returnedByName);
			Assert.assertEquals("Incorrect name ordering!",correctByName,returnedByName);
//		}finally{
//			if(rs !=null) rs.close();
//			if(s !=null) s.close();
//		}
	}

	@Test
	public void testSortOperationByKey2Ascending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by value1");
			//need to check that elements are in order

			List<Triplet> returnedByValue = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByValue.add(triple);
			}
			SpliceLogUtils.info(LOG, "returnedByValue=%s",returnedByValue);
			Assert.assertEquals("Incorrect name ordering!",correctByValue1,returnedByValue);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	@Test
	public void testSortOperationByKey1Descending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by name desc");
			//need to check that elements are in order

			List<Triplet> returnedByName = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByName.add(triple);
			}
			List<Triplet> reversedByName = new ArrayList<Triplet>(correctByName.size());
			for(int i=correctByName.size()-1;i>=0;i--){
				reversedByName.add(correctByName.get(i));
			}
			SpliceLogUtils.info(LOG, "returnedByName=%s",returnedByName);
			Assert.assertEquals("Incorrect name ordering!",reversedByName,returnedByName);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	@Test
	public void testSortOperationByKey2Descending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by value1 desc");
			//need to check that elements are in order

			List<Triplet> returnedByValue = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByValue.add(triple);
			}
			List<Triplet> reversedByValue = new ArrayList<Triplet>(correctByValue1.size());
			for(int i=correctByValue1.size()-1;i>=0;i--){
				reversedByValue.add(correctByValue1.get(i));
			}
			SpliceLogUtils.info(LOG, "returnedByValue=%s",returnedByValue);
			Assert.assertEquals("Incorrect name ordering!",reversedByValue,returnedByValue);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	@Test
	public void testSortOperationByKey1DescendingKey2Ascending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by name desc, value1 asc");
			//need to check that elements are in order

			List<Triplet> returnedByName = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByName.add(triple);
			}
			List<Triplet> reversedByName = new ArrayList<Triplet>(correctByName.size());
			for(int i=correctByName.size()-1;i>=0;i--){
				reversedByName.add(correctByName.get(i));
			}
			SpliceLogUtils.info(LOG, "returnedByName=%s",returnedByName);
			Assert.assertEquals("Incorrect name ordering!",reversedByName,returnedByName);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	@Test
	public void testSortOperationByKey1AscendingKey2Descending() throws Exception {
		Statement s = null;
		ResultSet rs = null;
		try{
			s = rule.getStatement();
			rs = s.executeQuery("select name,value1,value2 from food order by name asc, value1 desc");
			//need to check that elements are in order

			List<Triplet> returnedByValue = new ArrayList<Triplet>();
			while(rs.next()){
				Triplet triple = new Triplet(rs.getString(1),rs.getString(2),rs.getString(3));
				SpliceLogUtils.info(LOG, "triple=%s",triple);
				returnedByValue.add(triple);
			}
			SpliceLogUtils.info(LOG, "returnedByValue=%s",returnedByValue);
			Assert.assertEquals("Incorrect name ordering!",correctByAscDesc,returnedByValue);
		}finally{
			if(rs !=null) rs.close();
			if(s !=null) s.close();
		}
	}

	private static class Triplet implements Comparable<Triplet>{
		private final String k1;
		private final String k2;
		private final String k3;

		public Triplet(String k1, String k2,String k3){
			this.k1 = k1;
			this.k2 = k2;
			this.k3 = k3;
		}

		@Override
		public String toString() {
			return "("+k1+","+k2+","+k3+")";
		}

		@Override
		public int compareTo(Triplet other){
			int compare = k1.compareTo(other.k1);
			if(compare==0){
				compare = k2.compareTo(other.k2);
				if (compare ==0)
					compare = k3.compareTo(other.k3);
			}
			return compare;

		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + ((k1 == null) ? 0 : k1.hashCode());
			result = prime * result + ((k2 == null) ? 0 : k2.hashCode());
			result = prime * result + ((k3 == null) ? 0 : k3.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (!(obj instanceof Triplet))
				return false;
			Triplet other = (Triplet) obj;
			if (k1 == null) {
				if (other.k1 != null)
					return false;
			} else if (!k1.equals(other.k1))
				return false;
			if (k2 == null) {
				if (other.k2 != null)
					return false;
			} else if (!k2.equals(other.k2))
				return false;
			if (k3 == null) {
				if (other.k3 != null)
					return false;
			} else if (!k3.equals(other.k3))
				return false;
			return true;
		}
	}
	
	
}
