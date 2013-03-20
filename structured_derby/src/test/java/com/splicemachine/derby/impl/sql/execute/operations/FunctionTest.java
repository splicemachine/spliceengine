package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 2/22/13
 */
public class FunctionTest {

    private static final Logger LOG = Logger.getLogger(FunctionTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();
    static{
        tableSchemas.put("t","data double");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

    @BeforeClass
    public static void setupClass() throws Exception{
        DerbyTestRule.start();
        rule.createTables();
        insertData();
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        dropFunctions();
        rule.dropTables();
        DerbyTestRule.shutdown();
    }

    private static void dropFunctions() throws Exception{
        PreparedStatement ps = rule.prepareStatement("drop function SIN");
        ps.executeUpdate();
    }

    private static void insertData() throws SQLException {
        PreparedStatement ps = rule.prepareStatement("insert into t (data) values (?)");
        ps.setDouble(1,1.23d);
        ps.executeUpdate();

        PreparedStatement functionStatement = rule.prepareStatement("create function SIN( data double) returns double external name 'java.lang.Math.sin' language java parameter style java");
        functionStatement.executeUpdate();

    }

    @Test
    public void testSinFunction() throws SQLException{
        ResultSet funcRs = rule.executeQuery("select SIN(data) from t");
        int rows = 0;
        while(funcRs.next()){
            double sin = funcRs.getDouble(1);
            double correctSin = Math.sin(1.23d);
            Assert.assertEquals("incorrect sin!",correctSin,sin,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }
    /**
     * See Bug 266
     * 
     * @throws SQLException
     */
    @Test
    @Ignore
    public void testAcosFunction() throws SQLException{
        ResultSet funcRs = rule.executeQuery("select acos(data) from t");
        int rows = 0;
        while(funcRs.next()){
            double acos = funcRs.getDouble(1);
            double correctAcos = Math.acos(1.23d);
            Assert.assertEquals("incorrect acos!",correctAcos,acos,1/100000d);
            LOG.info(funcRs.getDouble(1));
            rows++;
        }
        Assert.assertTrue("Incorrect rows returned!",rows>0);
    }
    
    
    
}

