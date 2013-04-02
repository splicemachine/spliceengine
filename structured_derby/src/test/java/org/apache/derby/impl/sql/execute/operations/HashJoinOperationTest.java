package org.apache.derby.impl.sql.execute.operations;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Map;

import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import org.apache.log4j.Logger;
import org.junit.*;
import com.splicemachine.derby.test.SpliceDerbyTest;

public class HashJoinOperationTest  {
	private static Logger LOG = Logger.getLogger(HashJoinOperationTest.class);
    private static Map<String,String> tableMap  = Maps.newHashMap();
    static{
        tableMap.put("HashJoinOperationTest_c","si varchar(40), sa varchar(40)");
        tableMap.put("HashJoinOperationTest_d","si varchar(40), sa varchar(40)");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableMap,false,LOG);

	@BeforeClass 
	public static void startup() throws Exception {
        DerbyTestRule.start();

        rule.createTables();

        insertData();
	}

    private static void insertData() throws Exception{
        System.err.println("inserting data");
        Statement s = rule.getStatement();
        for (int i =0; i< 10; i++) {
            s.execute("insert into HashJoinOperationTest_c values('" + i + "','" + "i')");
            if (i!=9)
                s.execute("insert into HashJoinOperationTest_d values('" + i + "','" + "i')");
        }
    }

    @AfterClass
	public static void shutdown() throws Exception {
        rule.dropTables();
        DerbyTestRule.shutdown();
	}



	@Test
    public void testVarcharInnerJoin() throws SQLException {
        ResultSet rs  = rule.executeQuery("select c.si, d.si from HashJoinOperationTest_c c inner join HashJoinOperationTest_d d on c.si = d.si");
        int j = 0;
        while (rs.next()) {
            j++;
            LOG.info("c.si="+rs.getString(1)+",d.si="+rs.getString(2));
            Assert.assertNotNull(rs.getString(1));
            if (!rs.getString(1).equals("9")) {
                Assert.assertNotNull(rs.getString(2));
            } else {
                Assert.assertNull(rs.getString(2));
            }
        }
        Assert.assertEquals(9, j);
    }

	@Test
	public void testHashInnerJoin() throws Exception{

	}
}
