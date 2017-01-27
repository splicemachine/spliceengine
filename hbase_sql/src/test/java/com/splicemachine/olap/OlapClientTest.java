/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.olap;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.concurrent.SystemClock;
import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.iapi.sql.olap.DistributedJob;
import com.splicemachine.derby.iapi.sql.olap.OlapStatus;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.Ignore;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * Basic tests around the OlapServer's functionality.
 *
 * Created by dgomezferro on 3/17/16.
 */
@Ignore
@SuppressWarnings("unused")
public class OlapClientTest {
    private static final Logger LOG = Logger.getLogger(OlapClientTest.class);

    private static OlapServer olapServer;
    private static OlapClient olapClient;

    @BeforeClass
    public static void beforeClass() throws Exception {
        Logger.getLogger(MappedJobRegistry.class).setLevel(Level.INFO);
        Logger.getLogger(OlapPipelineFactory.class).setLevel(Level.INFO);
        Logger.getLogger("splice.config").setLevel(Level.WARN);
        Logger.getLogger(OlapRequestHandler.class).setLevel(Level.WARN);
        setupServer();
    }


    @AfterClass
    public static void afterClass() throws Exception {
        olapClient.shutdown();
        olapServer.stopServer();
    }

    @Test(timeout = 3000)
    public void simpleTest() throws Exception {
        final Random rand = new Random(0);
        int sleep = rand.nextInt(200);
        DumbOlapResult result = olapClient.execute(new DumbDistributedJob(sleep,13));
        Assert.assertNotNull(result);
        Assert.assertEquals(13, result.order);
    }

    @Test(timeout = 16000)
    public void longRunningTest() throws Exception {
        final Random rand = new Random(0);
        int sleep = 4000;
        DumbOlapResult result = olapClient.execute(new DumbDistributedJob(sleep,13));
        Assert.assertNotNull(result);
        Assert.assertEquals(13, result.order);
    }

    @Test(timeout = 20000, expected = IllegalStateException.class)
    public void cantReuseJobsTest() throws Exception {
        final Random rand = new Random(0);
        int sleep = rand.nextInt(200);
        DumbDistributedJob ddj = new DumbDistributedJob(sleep,13);
        DumbOlapResult result = olapClient.execute(ddj);
        Assert.assertNotNull(result);
        Assert.assertEquals(13, result.order);
        DumbOlapResult result2 = olapClient.execute(ddj);
        Assert.fail("Should have raised exception");
    }

    @Test(timeout = 3000)
    @Ignore // per sf
    public void failingJobTest() throws Exception {
        try {
            DumbOlapResult result = olapClient.execute(new FailingDistributedJob("failingJob"));
            Assert.fail("Didn't raise exception");
        } catch (IOException e) {
            Assert.assertTrue(e.getMessage().contains("Expected exception"));
        }
    }

    @Test
    @Ignore // per sf
    public void repeatedFailingJob() throws Exception{
        for(int i=0;i<100;i++){
            failingJobTest();
        }
    }

    @Test(timeout = 20000)
    public void concurrencyTest() throws Exception {
        int size = 32;
        Thread[] threads = new Thread[size];
        final AtomicReferenceArray<DumbOlapResult> results = new AtomicReferenceArray<>(size);
        final Random rand = new Random(size);
        for (int i = 0; i < size; ++i) {
            final int j = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int sleep = rand.nextInt(200);
                    try {
                        results.set(j, olapClient.execute(new DumbDistributedJob(sleep,j)));
                    } catch (IOException e) {
                        results.set(j, null);
                    }catch(TimeoutException te){
                        Assert.fail("Timed out");
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < size; ++i) {
            threads[i].join();
        }
        for (int i = 0; i < size; ++i) {
            Assert.assertNotNull(results.get(i));
            Assert.assertEquals(i, results.get(i).order);
        }
    }

    @Test(timeout = 20000)
    public void concurrencySameNameTest() throws Exception {
        int size = 32;
        Thread[] threads = new Thread[size];
        final AtomicReferenceArray<DumbOlapResult> results = new AtomicReferenceArray<>(size);
        final Random rand = new Random(size);
        for (int i = 0; i < size; ++i) {
            final int j = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int sleep = rand.nextInt(200);
                    try {
                        results.set(j, olapClient.execute(new SameNameJob(sleep,j)));
                    } catch (Exception e) {
                        LOG.error("Unexpected exception", e);
                        results.set(j, null);
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < size; ++i) {
            threads[i].join();
        }
        for (int i = 0; i < size; ++i) {
            Assert.assertNotNull(results.get(i));
            Assert.assertEquals(i, results.get(i).order);
        }
    }

    @Test(timeout = 20000)
    public void overflowTest() throws Exception {
        int size = 32;
        Thread[] threads = new Thread[size];
        final AtomicReferenceArray<DumbOlapResult> results = new AtomicReferenceArray<>(size);
        final Random rand = new Random(size);
        for (int i = 0; i < size; ++i) {
            final int j = i;
            threads[i] = new Thread() {
                @Override
                public void run() {
                    int sleep = rand.nextInt(2000);
                    try {
                        results.set(j, olapClient.execute(new DumbDistributedJob(sleep,j)));
                    } catch (IOException e) {
                        results.set(j, null);
                    }catch(TimeoutException te){
                        Assert.fail("Timed out");
                    }
                }
            };
            threads[i].start();
        }
        for (int i = 0; i < size; ++i) {
            threads[i].join();
        }
        for (int i = 0; i < size; ++i) {
            Assert.assertNotNull(results.get(i));
            Assert.assertEquals(i, results.get(i).order);
        }
    }

    @Test(timeout=10000)
    public void testServerFailureAfterSubmit() throws Exception{
       /*
        * Tests what would happen if the server went down after we had successfully submitted, but while
        * we are waiting. Because this is inherently concurrent, we use multiple threads
        */
        final AtomicReferenceArray<DumbOlapResult> results = new AtomicReferenceArray<>(1);
        final AtomicReferenceArray<Throwable> errors = new AtomicReferenceArray<>(1);
        Thread t = new Thread(new Runnable(){
            @Override
            public void run(){
                try{
                    results.set(0, olapClient.execute(new DumbDistributedJob(100000,0)));
                }catch(IOException | TimeoutException e){
                    errors.set(0, e);
                    results.set(0, null);
                }
            }
        });
        t.start();

        Thread.sleep(1000);
        //shut down the server
        olapServer.stopServer();

        try{
            t.join();
            Assert.assertNull(results.get(0));
            Assert.assertNotNull(errors.get(0));
        }finally{
            //restart the server
            olapClient.shutdown();
            setupServer();
        }
    }

    private static class DumbOlapResult extends AbstractOlapResult {
        int order;

        public DumbOlapResult(){
        }

        DumbOlapResult(int order) {
            this.order = order;
        }

        @Override
        public boolean isSuccess(){
            return true;
        }
    }

    private static class DumbDistributedJob extends DistributedJob{
        private static final Logger LOG = Logger.getLogger(DumbDistributedJob.class);
        int order;
        int sleep;

        public DumbDistributedJob(){ }

        DumbDistributedJob(int sleep,int order) {
            this.sleep = sleep;
            this.order = order;
        }

        @Override
        public Callable<Void> toCallable(final OlapStatus jobStatus,Clock clock,long clientTimeoutCheckIntervalMs){
            return new Callable<Void>(){
                @Override
                public Void call() throws Exception{
                    jobStatus.markRunning();
                    LOG.trace("started job " + getUniqueName() + " with order " + order);
                    Thread.sleep(sleep);
                    LOG.trace("finished job " + getUniqueName() + " with order " + order);
                    jobStatus.markCompleted(new DumbOlapResult(order));
                    return null;
                }
            };
        }

        @Override
        public String getName(){
            return "DumbDistributedJob["+order+"]";
        }

    }

    private static class SameNameJob extends DumbDistributedJob {

        public SameNameJob() {}

        SameNameJob(int sleep,int order) {
            super(sleep, order);
        }

        @Override
        public String getName(){
            return "SameNameJob";
        }

    }

    private static class FailingDistributedJob extends DistributedJob{
        private String uniqueId;

        public FailingDistributedJob(){
        }

        FailingDistributedJob(String uniqueId){
            this.uniqueId=uniqueId;
        }

        @Override
        public Callable<Void> toCallable(final OlapStatus jobStatus,Clock clock,long clientTimeoutCheckIntervalMs){
            return new Callable<Void>(){
                @Override
                public Void call() throws Exception{
                    jobStatus.markRunning();
                    jobStatus.markCompleted(new FailedOlapResult(new IOException("Expected exception")));
                    return null;
                }
            };
        }

        @Override
        public String getName(){
            return uniqueId;
        }

    }

    private static void setupServer(){
        Clock clock=new SystemClock();
        olapServer = new OlapServer(0,clock); // any port
        olapServer.startServer(HConfiguration.getConfiguration());
        JobExecutor nl = new AsyncOlapNIOLayer(olapServer.getBoundHost(),olapServer.getBoundPort(), 10);
        olapClient = new TimedOlapClient(nl,10000);
    }
}
