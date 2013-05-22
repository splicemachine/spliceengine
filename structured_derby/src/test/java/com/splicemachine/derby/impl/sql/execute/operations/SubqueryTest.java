package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Set;

/**
 * @author Scott Fines
 *         Created on: 5/17/13
 */
public class SubqueryTest {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();

    protected static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SubqueryTest.class.getSimpleName());
    protected static SpliceTableWatcher t1Watcher = new SpliceTableWatcher("t1",schemaWatcher.schemaName,"(k int, l int)");
    protected static SpliceTableWatcher t2Watcher = new SpliceTableWatcher("t2",schemaWatcher.schemaName,"(k int, l int)");

    private static final int size = 10;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher)
            .around(t1Watcher)
            .around(t2Watcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+t2Watcher.toString()+" values (?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i+1);
                            ps.execute();
                        }

                        ps = spliceClassWatcher.prepareStatement("insert into "+ t1Watcher.toString()+" values(?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1,i);
                            ps.setInt(2,i+1);
                            ps.execute();
                            if(i%2==0){
                                //put in a duplicate
                                ps.setInt(1,i);
                                ps.setInt(2,i+1);
                                ps.execute();
                            }

                        }
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }finally{
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    @Test
    public void testValuesSubSelect() throws Exception {
        /*
         * Regression for Bug 285. Make sure that values ((select ..)) works as expected
         */
        ResultSet rs = methodWatcher.executeQuery("values ((select k from "+t2Watcher.toString()+" where k=1),2)");
        Set<Integer> correctResults = Sets.newHashSet(1);
        List<Integer> retResults = Lists.newArrayList();

        while(rs.next()){
            int val = rs.getInt(1);
            int sec = rs.getInt(2);
            System.out.printf("value=%d,sec=%d%n",val,sec);
            Assert.assertTrue("value "+ val+" is not contained in the correct results!",correctResults.contains(val));
            retResults.add(val);
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctResults.size(),retResults.size());

        for(int correct:retResults){
            int numFound=0;
            for(int ret:retResults){
                if(ret==correct){
                    numFound++;
                    if(numFound>1)
                        Assert.fail("Value "+ ret+" was returned more than once!");
                }
            }
        }
    }

    @Test
    public void testValuesSubselectWithTwoSelects() throws Exception {
        ResultSet rs = methodWatcher.executeQuery("values ((select k from "+t2Watcher.toString()+" where k = 1), (select l from "+t2Watcher.toString()+" where l =4))");
        Set<Integer> correctResults = Sets.newHashSet(1);
        Set<Integer> correctSecondResults = Sets.newHashSet(4);

        List<Integer> retResults = Lists.newArrayList();
        List<Integer> retSecResults = Lists.newArrayList();

        while(rs.next()){
            int val = rs.getInt(1);
            int sec = rs.getInt(2);
            System.out.printf("value=%d,sec=%d%n",val,sec);
            Assert.assertTrue("value "+ val+" is not contained in the correct results!",correctResults.contains(val));
            retResults.add(val);

            Assert.assertTrue("second value "+ sec+" is not contained in the correct results!",correctSecondResults.contains(sec));
        }
        Assert.assertEquals("Incorrect number of rows returned!",correctResults.size(),retResults.size());

        for(int correct:retResults){
            int numFound=0;
            for(int ret:retResults){
                if(ret==correct){
                    numFound++;
                    if(numFound>1)
                        Assert.fail("Value "+ ret+" was returned more than once!");
                }
            }
        }
    }

    @Test
    public void testSubqueryInJoinClause() throws Exception {
        /* Regression test for bug 273 */
        ResultSet rs = methodWatcher.executeQuery("select * from "+t1Watcher.toString()+" a join "+ t2Watcher.toString()+" b on a.k = b.k and a.k in (select k from "+ t1Watcher.toString()+" where a.k ="+t1Watcher.toString()+".k)");
        while(rs.next()){
            System.out.printf("k=%d,l=%d",rs.getInt(1),rs.getInt(2));
        }
    }

    @Test
    public void testInDoesNotReturnDuplicates() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(String.format("select k from %s a where k in (select k from %s )",t2Watcher.toString(),t1Watcher.toString()));
        Set<Integer> priorResults = Sets.newHashSet();
        while(rs.next()){
            Integer nextK = rs.getInt(1);
            System.out.printf("nextK=%d%n",nextK);
            Assert.assertTrue("duplicate result "+ nextK +" returned!",!priorResults.contains(nextK));
            priorResults.add(nextK);
        }
        Assert.assertTrue("No Rows returned!",priorResults.size()>0);
    }
}
