package org.apache.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.test.DerbyTestRule;
import junit.framework.Assert;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
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
 *         Created on: 3/6/13
 */
public class PrimaryKeyScanTest {
    private static Logger LOG = Logger.getLogger(PrimaryKeyScanTest.class);

    private static final Map<String,String> tableSchemas = Maps.newHashMap();
    static{
        tableSchemas.put("a","pk_1 varchar(50), pk_2 varchar(50),val int, PRIMARY KEY(pk_1,pk_2)");
//        tableSchemas.put("sort","pk_1varchar(50), pk_2 varchar(50),val int PRIMARY KEY(pk,pk_2)");
    }

    @Rule public static DerbyTestRule rule = new DerbyTestRule(tableSchemas,false,LOG);

    private static final Map<Pair<String,String>,Integer> correctData = Maps.newHashMap();
    private static final int pk1Size = 10;
    private static final int pk2Size = 10;

    @BeforeClass
    public static void startup() throws Exception {
        DerbyTestRule.start();
        rule.createTables();
        insertData();
    }

    private static void insertData() throws SQLException {
        PreparedStatement ps = rule.prepareStatement("insert into a (pk_1, pk_2,val) values (?,?,?)");
        for (int i =0; i< pk1Size; i++) {
            String pk1 = "pk_1_"+i;
            for(int j=0;j<pk2Size;j++){
                String pk2 = "pk_2_"+j;
                int val = i;
                ps.setString(1,pk1);
                ps.setString(2, pk2);
                ps.setInt(3, val);
                ps.executeUpdate();
                correctData.put(Pair.newPair(pk1,pk2),val);
            }
        }
    }

    @AfterClass
    public static void shutdown() throws Exception {
        rule.dropTables();
        DerbyTestRule.shutdown();
    }

    @Test
    public void testScanAllData() throws SQLException{
        ResultSet rs = rule.executeQuery("select * from a");
        List<String> results = Lists.newArrayListWithExpectedSize(correctData.size());
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctData.size(),results.size());
    }

    @Test
    public void testScanForNullEntries() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1 is null");

        Assert.assertTrue("Expected 0 rows returned!",!rs.next());
    }

    @Test
    public void testPkOneQualifiedScan() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1 = 'pk_1_1'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size,results.size());
    }

    @Test
    public void testPkTwoQualifiedScan() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_2 = 'pk_2_1'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk1Size,results.size());
    }

    @Test
    public void testRestrictedScan() throws Exception{
        ResultSet rs = rule.executeQuery("select pk_1,val from a where pk_1 = 'pk_1_1'");

        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            int val = rs.getInt(2);

            results.add(String.format("pk_1: %s,val:%d",pk1,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size,results.size());
    }

    @Test
    public void testScanWithGreaterThanOperator() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1 > 'pk_1_0'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }


    @Test
    public void testScanWithGreaterThanOrEqualsOperator() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1 >= 'pk_1_1'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanWithLessThanOperator() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1 < 'pk_1_"+(pk1Size-1)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanWithLessThanOrEqualsOperator() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1<= 'pk_1_"+(pk1Size-2)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-1),results.size());
    }

    @Test
    public void testScanGreaterThanLessEquals() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1> 'pk_1_0' and pk_1 <='pk_1_"+(pk1Size-2)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }

    @Test
    public void testScanGreaterEqualLessEquals() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1>= 'pk_1_1' and pk_1 <='pk_1_"+(pk1Size-2)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }

    @Test
    public void testScanGreaterThanLess() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1> 'pk_1_0' and pk_1 <'pk_1_"+(pk1Size-1)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }

    @Test
    public void testScanGreaterEqualsLess() throws Exception{
        ResultSet rs = rule.executeQuery("select * from a where pk_1>= 'pk_1_1' and pk_1 <'pk_1_"+(pk1Size-1)+"'");
        List<String> results = Lists.newArrayListWithExpectedSize(1);
        while(rs.next()){
            String pk1 = rs.getString(1);
            String pk2 = rs.getString(2);
            int val = rs.getInt(3);

            Pair pair = Pair.newPair(pk1,pk2);
            Assert.assertTrue("could not find pair!",correctData.containsKey(pair));
            Assert.assertEquals("Incorrect value for pair!",correctData.get(pair).intValue(),val);

            results.add(String.format("pk_1: %s,pk_2: %s, val:%d",pk1,pk2,val));
        }

        for(String result:results){
            LOG.info(result);
        }

        Assert.assertEquals("Incorrect number of rows returned!",pk2Size*(pk1Size-2),results.size());
    }


}
