package com.splicemachine.derby.impl.job;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.suites.Stats;

/**
 * Tests that we can submit multiple different jobs concurrently without too much trouble.
 *
 * @author Scott Fines
 * Created on: 5/23/13
 */
public class ConcurrentJobIT {
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher();
    private static final Logger LOG = Logger.getLogger(ConcurrentJobIT.class);
    public static final String CLASS_NAME = ConcurrentJobIT.class.getSimpleName().toUpperCase();
    public static final String TABLE_NAME = "T";
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);
    protected static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME,spliceSchemaWatcher.schemaName,"(username varchar(40),i int)");
    public static Stats stats = new Stats();

    private static final int size = 100;
    private static final int numThreads = 5;
    private static final int numIterations = 100;

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher(){
                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement("insert into "+ spliceTableWatcher.toString() + " values (?,?)");
                        for(int i=0;i<size;i++){
                            ps.setInt(1, i);
                            stats.add(i);
                            ps.setString(2,Integer.toString(i+1));
                            ps.executeUpdate();
                        }
                        spliceClassWatcher.splitTable(TABLE_NAME,CLASS_NAME,size/3);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                    finally {
                        spliceClassWatcher.closeAll();
                    }
                }

            });

    @Rule public SpliceWatcher methodWatcher = new SpliceWatcher();

    private static ExecutorService executor;

    private static ExecutorService getExecutor() {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("test-thread-%d").build();
        return Executors.newFixedThreadPool(numThreads, factory);
    }

    @Before
    public void setUp() throws Exception {
        executor = getExecutor();
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test
    public void testMultipleScalarAggregates() throws Exception {
        List<Future<Void>> futures = Lists.newArrayList();
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    for (int iter = 0; iter < numIterations; iter++) {
                        LOG.trace("running test for the " + iter + "th time");
                        testCountOperation();
                    }
                    LOG.info("Tests completed");
                    return null;
                }
            }));
        }
        //wait for it to finish
        for(Future<Void> future:futures){
            future.get();
        }
    }

    private void testCountOperation() throws Exception{
        ResultSet rs = methodWatcher.executeQuery(String.format("select count(*) from %s", spliceTableWatcher.toString()));
        int count =0;
        while(rs.next()){
            Assert.assertEquals("incorrect count returned!",stats.getCount(),rs.getInt(1));
            count++;
        }
        Assert.assertEquals("incorrect number of rows returned!",1,count);
    }
}
