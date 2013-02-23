package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Set;

/**
 * Tests against Microstrategy Order Detail table.
 *
 * These are tests intended to make sure that things work against a real, actual data
 * set. Ensuring Groupings are unique, that sort of thing.
 *
 * WARNING: Most of these tests are extremely slow. They are working with a large data set. Beware
 *
 * @author Scott Fines
 *         Created on: 2/23/13
 */
public class MsOrderDetailTest {
    private static final Logger LOG = Logger.getLogger(MsOrderDetailTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();

    private static final String ORDER_DETAIL_SCHEMA = "order_id varchar(50), item_id INT, order_amt INT," +
            "order_date TIMESTAMP, emp_id INT, promotion_id INT, qty_sold INT," +
            "unit_price FLOAT, unit_cost FLOAT, discount FLOAT, customer_id INT";
    static{
        tableSchemas.put("order_detail_small",ORDER_DETAIL_SCHEMA);
        tableSchemas.put("order_detail_med",ORDER_DETAIL_SCHEMA);
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

    @BeforeClass
    public static void setup() throws Exception{
        DerbyTestRule.start();
        createTables();
    }


    @AfterClass
    public static void shutdown() throws Exception{
//        rule.dropTables();
        DerbyTestRule.shutdown();
    }

    private static void createTables() throws SQLException {
        ResultSet rs = rule.executeQuery("select tablename from sys.systables where tablename = 'ORDER_DETAIL_SMALL' or tablename = 'ORDER_DETAIL_MED'");
        boolean hasSmall = false;
        boolean hasMed = false;
        while(rs.next()){
            String table =rs.getString(1);
            if("ORDER_DETAIL_SMALL".equalsIgnoreCase(table))hasSmall=true;
            else if("ORDER_DETAIL_MED".equalsIgnoreCase(table))hasMed=true;
        }
        if(!hasSmall){
            rule.createTable("ORDER_DETAIL_SMALL",ORDER_DETAIL_SCHEMA);
            importData("ORDER_DETAIL_SMALL","order_detail_small.csv");
        }
        if(!hasMed){
            rule.createTable("ORDER_DETAIL_MED",ORDER_DETAIL_SCHEMA);
            importData("ORDER_DETAIL_MED","order_detail_med.csv");
        }
    }

    private static void importData(String table, String filename) throws SQLException {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"structured_derby/";
        PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, ?, null,null," +
                "?,',',null,null)");
        ps.setString(1,table);
        ps.setString(2,userDir+"src/test/resources/"+filename);
        ps.executeUpdate();
    }

    @Test
    public void testGroupedAggregationsGroupUniquely() throws Exception{
        /*
         * Test for Bug #230. The idea is to make sure that
         * when grouping up by a specific key, that there is only one entry per key.
         */
        ResultSet groupedRs = rule.executeQuery("select customer_id, count(*) from order_detail_med group by customer_id");
        Set<String> uniqueGroups = Sets.newHashSet();
        while(groupedRs.next()){
            String groupKey = groupedRs.getString(1);
            int groupCount = groupedRs.getInt(2);
            Assert.assertTrue("empty group count!",groupCount>0);
            Assert.assertTrue("Already seen key "+ groupKey, !uniqueGroups.contains(groupKey));
        }
        Assert.assertTrue("No groups found!",uniqueGroups.size()>0);
    }
}
