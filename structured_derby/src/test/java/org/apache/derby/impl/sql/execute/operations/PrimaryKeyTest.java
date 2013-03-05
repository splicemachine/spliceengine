package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class PrimaryKeyTest {
    private static final Logger LOG = Logger.getLogger(PrimaryKeyTest.class);

    private static final Map<String,String> tableMap = Maps.newHashMap();
    static{
        tableMap.put("t","name varchar(50),val int, PRIMARY KEY(name)");
//        tableMap.put("multi_pk","name varchar(50),fruit varchar(30),val int, PRIMARY KEY(name,fruit)");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,LOG);

    @BeforeClass
    public static void startup() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdown() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Test
    public void cannotInsertDuplicatePks() throws Exception{
        /*
         * Test to ensure that duplicate Primary Key combinations are not allowed.
         */
        PreparedStatement ps = rule.prepareStatement("insert into t (name, val) values (?,?)");

        //insert the first entry in the row
        ps.setString(1,"sfines");
        ps.setInt(2,1);
        ps.executeUpdate();

        LOG.info("First insertion completed without error, checking that it succeeded");
        //validate that a single row exists
        PreparedStatement validator = rule.prepareStatement("select * from t where name = ?");
        validator.setString(1,"sfines");
        ResultSet rs = validator.executeQuery();
        int matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);

        LOG.info("Attempting second insertion");
        //try and insert it again
        boolean failed=false;
        try{
            ps.executeUpdate();
        }catch(SQLException se){
            LOG.info(se.getMessage(),se);
            failed=true;
        }
        Assert.assertTrue("Insert of duplicate records succeeded!",failed);

        LOG.info("Validating second insertion failed");
        //make sure that it didn't get inserted anyway
        rs = validator.executeQuery();
        matchCount=0;
        while(rs.next()){
            if("sfines".equalsIgnoreCase(rs.getString(1))){
                matchCount++;
            }
        }
        Assert.assertEquals("Incorrect number of matching rows found!",1,matchCount);
        LOG.info("Validation succeeded");
    }

    @Test
    public void scanningPrimaryKeyTable() throws Exception{
        PreparedStatement test = rule.prepareStatement("select * from t where name = ?");
        test.setString(1,"sfines");
        ResultSet rs = test.executeQuery();
        if(rs.next());
    }

    @Test
    public void scanningPrimaryKeyTableByPk() throws Exception{
        PreparedStatement test = rule.prepareStatement("select name from t where name = ?");
        test.setString(1,"sfines");
        ResultSet rs = test.executeQuery();
        if(rs.next());
    }

}
