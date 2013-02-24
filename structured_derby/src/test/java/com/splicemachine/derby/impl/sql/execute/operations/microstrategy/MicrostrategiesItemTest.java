package com.splicemachine.derby.impl.sql.execute.operations.microstrategy;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 2/24/13
 */
public class MicrostrategiesItemTest {
    private static final Logger LOG = Logger.getLogger(MicrostrategiesItemTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();

    private static final String ITEM_SCHEMA = "itm_id INT," +
            "itm_name VARCHAR(128)," +
            "itm_long_desc VARCHAR(32672)," +
            "itm_foreign_name VARCHAR(128)," +
            "itm_url VARCHAR(1024)," +
            "itm_disc_cd VARCHAR(64)," +
            "itm_upc VARCHAR(64)," +
            "itm_warranty VARCHAR(1)," +
            "itm_unit_price FLOAT," +
            "itm_unit_cost FLOAT," +
            "itm_subcat_id INT," +
            "itm_supplier_id INT," +
            "itm_brand_id INT," +
            "itm_name_de VARCHAR(128)," +
            "itm_name_fr VARCHAR(128)," +
            "itm_name_es VARCHAR(128)," +
            "itm_name_it VARCHAR(128)," +
            "itm_name_po VARCHAR(128)," +
            "itm_name_ja VARCHAR(128)," +
            "itm_name_sch VARCHAR(128)," +
            "itm_name_ko VARCHAR(128)," +
            "itm_long_desc_de VARCHAR(32672)," +
            "itm_long_desc_fr VARCHAR(32672)," +
            "itm_long_desc_es VARCHAR(32672)," +
            "itm_long_desc_it VARCHAR(32672)," +
            "itm_long_desc_po VARCHAR(32672)," +
            "itm_itm_long_desc_ja VARCHAR(32672)," +
            "itm_long_desc_sch VARCHAR(32672)," +
            "itm_long_desc_ko VARCHAR(32672)";

    static{
        tableSchemas.put("item",ITEM_SCHEMA);
    }

    @Rule
    public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

    @BeforeClass
    public static void setup() throws Exception{
        DerbyTestRule.start();
        createTables();
    }

    private static void createTables() throws SQLException {
        ResultSet rs = rule.executeQuery("select tablename from sys.systables where tablename = 'ITEM'");
        boolean hasSmall = false;
        while(rs.next()){
            String table =rs.getString(1);
            if("ITEM".equalsIgnoreCase(table))hasSmall=true;
        }
        if(!hasSmall){
            rule.createTable("ITEM",ITEM_SCHEMA);
            importData("ITEM","lu_item.csv");
        }
    }

    private static void importData(String table, String filename) throws SQLException {
        String userDir = System.getProperty("user.dir");
        if(!userDir.endsWith("structured_derby"))
            userDir = userDir+"/structured_derby/";
        PreparedStatement ps = rule.prepareStatement("call SYSCS_UTIL.SYSCS_IMPORT_DATA (null, ?, null,null," +
                "?,',',null,null)");
        ps.setString(1,table);
        ps.setString(2,userDir+"src/test/resources/microstrategy/"+filename);
        ps.executeUpdate();
    }

    @Test
    public void testOrderBySorts() throws Exception{
        /*
         * Regression test for Bug #241. Confirms that ORDER BY does not throw an exception and
         * correctly sorts data
         */
        ResultSet rs = rule.executeQuery("select itm_subcat_id from item order by itm_subcat_id");
        List<Integer> results = Lists.newArrayList();
        int count=0;
        while(rs.next()){
            if(rs.getObject(1)==null){
                Assert.assertTrue("Sort incorrect! Null entries are not in the front of the list",count==0);
            }
            results.add(rs.getInt(1));
            count++;
        }

        //check that sort order is maintained
        for(int i=0;i<results.size()-1;i++){
            Integer first = results.get(i);
            Integer second = results.get(i+1);
            Assert.assertTrue("Sort order incorrect!",first.compareTo(second)<=0);
        }
    }
}
