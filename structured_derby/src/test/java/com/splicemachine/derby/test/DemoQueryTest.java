package com.splicemachine.derby.test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * 
 * @author jessiezhang
 * 
 * Since the demo machine has the current code, we need to ensure demo queries continuously running fine.
 */

public class DemoQueryTest extends SpliceDerbyTest {
	private static Logger LOG = Logger.getLogger(DemoQueryTest.class);

	@BeforeClass 
	public static void startup() throws Exception {
		startConnection();	
		
		Statement s = null;
		try {
			s = conn.createStatement();
			s.execute("CREATE TABLE CUSTOMER (cst_id INT,cst_last_name VARCHAR(64),cst_first_name VARCHAR(64),"+
					"cst_gender_id	INT,cst_birthdate	TIMESTAMP,cst_email	VARCHAR(64),cst_address	VARCHAR(64),"+
					"cst_zipcode VARCHAR(16),cst_income_idINT,cst_city_id INT,cst_age_years	INT,cst_agerange_id	INT,"+
					"cst_maritalstatus_id INT,cst_education_id INT,cst_housingtype_id INT,cst_householdcount_id	INT,"+
					"cst_plan_id INT,cst_first_order TIMESTAMP,cst_last_order TIMESTAMP,cst_tenure INT,"+
					"cst_recency INT,cst_status_id INT)");
			importData("customer","customer_iso.csv");
			
			s.execute("CREATE TABLE ORD_LINE (orl_order_id VARCHAR(50),orl_item_id  INT,orl_amt INT,orl_date TIMESTAMP,"+
			        "orl_emp_id INT,orl_promotion_id INT,orl_qty_sold INT,orl_unit_price FLOAT,orl_unit_cost FLOAT,"+
			        "orl_discount FLOAT,orl_customer_id INT)");
			importData("ORD_LINE","order_line_small.csv");
			
		} catch (Exception e) {
			LOG.error("error creating table and inserting data-"+e.getMessage(), e);
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
	public void testCustomerOrderJoin() {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			result = s.executeQuery("select cst_zipcode, sum(orl_qty_sold*orl_unit_price) from CUSTOMER left outer join ORD_LINE " +
					"on orl_customer_id=cst_id group by cst_zipcode");
			while (result.next()) {
				count++;
			}	
			Assert.assertEquals(566, count);
		} catch (SQLException e) {
			LOG.error("error in testCustomerOrderJoin-"+e.getMessage(), e);
		} finally {
			try {
				if (result!=null)
					result.close();
				if(s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	@Test
	public void testTransactionDemoQueries() {
		int count = 0;
		Statement s = null;
		ResultSet result = null;
		try {
			conn.setAutoCommit(false);
			s = conn.createStatement();
			
			result = s.executeQuery("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
					"orl_customer_id as CUST_ID from ORD_LINE where orl_customer_id=5520");
			while (result.next()) {
				count++;
			}	
			Assert.assertEquals(10, count);
			
			s.execute("insert into ORD_LINE(ORL_ORDER_ID,ORL_ITEM_ID,ORL_QTY_SOLD,ORL_UNIT_PRICE,ORL_CUSTOMER_ID) values "+
			"('99999_999_1',310,1,100,5520), ('99999_999_1',311,1,100,5520), ('99999_999_1',312,1,100,5520), ('99999_999_1',313,1,100,5520), ('99999_999_1',314,1,100,5520), ('99999_999_1',315,1,100,5520), ('99999_999_1',316,1,100,5520), ('99999_999_1',317,1,100,5520), ('99999_999_1',318,1,100,5520), ('99999_999_1',319,1,100,5520),"+ 
			"('99999_999_1',320,1,100,5520), ('99999_999_1',321,1,100,5520), ('99999_999_1',322,1,100,5520), ('99999_999_1',323,1,100,5520), ('99999_999_1',324,1,100,5520), ('99999_999_1',325,1,100,5520), ('99999_999_1',326,1,100,5520), ('99999_999_1',327,1,100,5520), ('99999_999_1',328,1,100,5520), ('99999_999_1',329,1,100,5520),"+ 
			"('99999_999_1',330,1,100,5520), ('99999_999_1',331,1,100,5520), ('99999_999_1',332,1,100,5520), ('99999_999_1',333,1,100,5520), ('99999_999_1',334,1,100,5520), ('99999_999_1',335,1,100,5520), ('99999_999_1',336,1,100,5520), ('99999_999_1',337,1,100,5520), ('99999_999_1',338,1,100,5520), ('99999_999_1',339,1,100,5520), "+
			"('99999_999_1',340,1,100,5520), ('99999_999_1',341,1,100,5520), ('99999_999_1',342,1,100,5520), ('99999_999_1',343,1,100,5520),('99999_999_1',344,1,100,5520),('99999_999_1',345,1,100,5520)");

			count = 0;
			result = s.executeQuery("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
					"orl_customer_id as CUST_ID from ORD_LINE where orl_customer_id=5520");
			while (result.next()) {
				count++;
			}	
			Assert.assertEquals(46, count);
			conn.rollback();
			
			count = 0;
			result = s.executeQuery("select orl_order_id as ORDER_ID, orl_qty_sold as QTY, orl_unit_price as UNIT_PRICE, " +
					"orl_customer_id as CUST_ID from ORD_LINE where orl_customer_id=5520");
			while (result.next()) {
				count++;
			}	
			Assert.assertEquals(10, count);
			
			conn.setAutoCommit(true);
			
		} catch (SQLException e) {
			LOG.error("error in testTransactionDemoQueries-"+e.getMessage(), e);
		} finally {
			try {
				if (result!=null)
					result.close();
				if(s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}

	}

	@Test
	public void testTableauDemoQuery() {
		Statement s = null;
		ResultSet result = null;
		try {
			s = conn.createStatement();
			
			s.execute("insert into CUSTOMER(cst_id,cst_zipcode) values (99999,'57501')");
			result = s.executeQuery("select * from CUSTOMER where cst_id=99999, cst_zipcode='57501'");
			Assert.assertTrue(result.next());	
			
			s.execute("delete from CUSTOMER where cst_id=99999");
			result = s.executeQuery("select * from CUSTOMER where cst_id=99999");
			Assert.assertFalse(result.next());	
			
			s.execute("insert into ORD_LINE (orl_customer_id,orl_qty_sold) values (99999,100)");
			result = s.executeQuery("select * from ORD_LINE where orl_customer_id=99999, orl_qty_sold=100");
			Assert.assertTrue(result.next());
			
			s.execute("delete from ORD_LINE where orl_customer_id=9999");
			result = s.executeQuery("select * from ORD_LINE where orl_customer_id=99999");
			Assert.assertFalse(result.next());
			
		} catch (SQLException e) {
			LOG.error("error in testTableauDemoQuery-"+e.getMessage(), e);
		} finally {
			try {
				if (result!=null)
					result.close();
				if(s!=null)
					s.close();
			} catch (SQLException e) {
				//no need to print out
			}
		}
	}
	
	private static void importData(String table, String filename) throws SQLException {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby/";
        PreparedStatement ps = conn.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, ?, null,null,?,',',null,null)");
        ps.setString(1,table);
        ps.setString(2,userDir+"/src/test/resources/"+filename);
        ps.executeUpdate();
    }

	@AfterClass 
	public static void shutdown() throws SQLException {
		dropTable("CUSTOMER");
		dropTable("ORD_LINE");
		stopConnection();		
	}
}
