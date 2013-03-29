package com.splicemachine.derby.impl.sql.execute.action;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 3/8/13
 */
public class NonNullConstraintTest {
    private static final Logger LOG = Logger.getLogger(NonNullConstraintTest.class);

    private static final Map<String,String> tableMap = Maps.newHashMap();
    static{
        tableMap.put("t","name varchar(40) NOT NULL, val int");
    }

    @BeforeClass
    public static void startTests() throws Exception{
        DerbyTestRule.start();
    }

    @AfterClass
    public static void shutdownTests() throws Exception{
        DerbyTestRule.shutdown();
    }

    @Rule
    public DerbyTestRule rule = new DerbyTestRule(tableMap,LOG);

    @Test(expected = SQLException.class)
    public void testCannotAddNullEntryToNonNullTable() throws Exception{
        rule.getStatement().execute("insert into t (name, val) values (null,27)");
    }

    @Test
    public void testCanStillAddEntryToNonNullTable() throws Exception{
        rule.getStatement().execute("insert into t (name, val) values ('sfines',27)");

        ResultSet rs = rule.executeQuery("select * from t");
        Assert.assertTrue("No Columns returned!",rs.next());
        String name = rs.getString(1);
        int val = rs.getInt(2);

        Assert.assertEquals("Incorrect name returned","sfines",name);
        Assert.assertEquals("Incorrect value returned",27,val);
    }
}
