package com.splicemachine.olap;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Random;

/**
 * Created by dgomezferro on 3/17/16.
 */
public class OlapClientTest {
    private static final Logger LOG = Logger.getLogger(OlapClientTest.class);

    private static OlapServer olapServer;
    private static OlapClient olapClient;
    private static Clock clock;

    @BeforeClass
    public static void beforeClass() throws Exception {
        olapServer = new OlapServer(0); // any port
        olapServer.startServer();
        clock = new SystemClock();
        olapClient = new OlapClientImpl(olapServer.getBoundHost(), olapServer.getBoundPort(), 10000, clock);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        ((OlapClientImpl)olapClient).shutdown();
        olapServer.stopServer();
    }

    @Test(timeout = 3000)
    public void simpleTest() throws Exception {
        final Random rand = new Random(0);
        int sleep = rand.nextInt(200);
        DumbOlapResult result = olapClient.submitOlapJob(new DumbOlapJob(sleep, 13));
        Assert.assertNotNull(result);
        Assert.assertEquals(13, result.order);
    }

    @Test(timeout = 3000)
    public void failingJobTest() throws Exception {
        try {
            DumbOlapResult result = olapClient.submitOlapJob(new FailingOlapJob());
            Assert.fail("Didn't raise exception");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Expected exception"));
        }
    }

    @Test(timeout = 3000)
    public void concurrencyTest() throws Exception {
        int size = 16;
        Thread[] threads = new Thread[size];
        final DumbOlapResult[] results = new DumbOlapResult[size];
        final Random rand = new Random(size);
        for (int i = 0; i < size; ++i) {
            final int j = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int sleep = rand.nextInt(200);
                    try {
                        results[j] = olapClient.submitOlapJob(new DumbOlapJob(sleep, j));
                    } catch (IOException e) {
                        e.printStackTrace();
                        results[j] = null;
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < size; ++i) {
            threads[i].join();
        }
        for (int i = 0; i < size; ++i) {
            Assert.assertNotNull(results[i]);
            Assert.assertEquals(i, results[i].order);
        }
    }
    @Test(timeout = 10000)
    public void overflowTest() throws Exception {
        int size = 32;
        Thread[] threads = new Thread[size];
        final DumbOlapResult[] results = new DumbOlapResult[size];
        final Random rand = new Random(size);
        for (int i = 0; i < size; ++i) {
            final int j = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int sleep = rand.nextInt(2000);
                    try {
                        results[j] = olapClient.submitOlapJob(new DumbOlapJob(sleep, j));
                    } catch (IOException e) {
                        e.printStackTrace();
                        results[j] = null;
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < size; ++i) {
            threads[i].join();
        }
        for (int i = 0; i < size; ++i) {
            Assert.assertNotNull(results[i]);
            Assert.assertEquals(i, results[i].order);
        }
    }

    public static class DumbOlapResult extends AbstractOlapResult {
        int order;

        public DumbOlapResult(){
        }

        public DumbOlapResult(int order) {
            this.order = order;
        }
    }

    public static class DumbOlapJob extends AbstractOlapCallable<DumbOlapResult> {
        private static final Logger LOG = Logger.getLogger(DumbOlapJob.class);
        int order;
        int sleep;

        public DumbOlapJob(){
        }

        public DumbOlapJob(int sleep, int order) {
            this.sleep = sleep;
            this.order = order;
        }

        @Override
        public DumbOlapResult call() throws Exception {
            LOG.trace("started job " + this.getCallerId() + " with order " + order);
            Thread.sleep(sleep);
            LOG.trace("finished job " + this.getCallerId() + " with order " + order);
            return new DumbOlapResult(order);
        }
    }


    public static class FailingOlapJob extends AbstractOlapCallable<DumbOlapResult> {
        public FailingOlapJob(){
        }

        @Override
        public DumbOlapResult call() throws Exception {
            throw new IOException("Expected exception");
        }
    }
}
