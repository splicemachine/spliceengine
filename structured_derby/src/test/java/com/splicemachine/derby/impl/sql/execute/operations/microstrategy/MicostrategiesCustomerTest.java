package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 2/24/13
 */
@Category(MicrostrategiesTests.class)
public class MicostrategiesCustomerTest {
    private static final Logger LOG = Logger.getLogger(MicostrategiesCustomerTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();

    private static final String CUSTOMER_SCHEMA = "customer_id int, " +
            "cust_last_name varchar(255), " +
            "cust_first_name varchar(255)," +
            "gender_id smallint, " +
            "cust_birthdate timestamp, " +
            "email varchar(255), " +
            "address varchar(255), " +
            "zipcode varchar(10), " +
            "income_id int, " +
            "cust_city_id int, " +
            "age_years int, " +
            "agerange_id int, " +
            "maritalstatus_id int, " +
            "education_id int, " +
            "housingtype_id int, " +
            "householdcount_id int," +
            "plan_id int, " +
            "first_order timestamp, " +
            "last_order timestamp, " +
            "tenure int, " +
            "recency int, " +
            "status_id int";

    static{
        tableSchemas.put("customer",CUSTOMER_SCHEMA);
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

    @BeforeClass
    public static void setup() throws Exception{
        DerbyTestRule.start();
        createTables();
    }

    private static void createTables() throws SQLException {
        ResultSet rs = rule.executeQuery("select tablename from sys.systables where tablename = 'CUSTOMER'");
        boolean hasSmall = false;
        while(rs.next()){
            String table =rs.getString(1);
            if("CUSTOMER".equalsIgnoreCase(table))hasSmall=true;
        }
        if(!hasSmall){
            rule.createTable("CUSTOMER",CUSTOMER_SCHEMA);
            importData("CUSTOMER","customer_iso.csv");
        }
    }

    private static void importData(String table, String filename) throws SQLException {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby/";
        PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, ?, null,null," +
                "?,',',null,null)");
        ps.setString(1,table);
        ps.setString(2,userDir+"/src/test/resources/"+filename);
        ps.executeUpdate();
    }

    @Test
    public void testSelectDistinctSelectsDistincts() throws Exception{
        /*
         * Regression Test for Bug #242. Confirm that no exceptions are thrown, and that
         * the returned data is in fact distinct
         */
        ResultSet rs = rule.executeQuery("select distinct cust_city_id from customer");
        Set<Integer> cityIds = Sets.newHashSet();
        while(rs.next()){
            int city = rs.getInt(1);
            Assert.assertTrue("City already found!",!cityIds.contains(city));
            cityIds.add(city);
        }
        Assert.assertTrue("No City ids found!",cityIds.size()>0);
    }
}
