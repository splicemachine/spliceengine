package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.junit.*;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 4/29/13
 */
@Ignore("This has to be ignored because the HBaseTestingUtility attempts to open a " +
        "server on the same port as the external server does, which causes all these tests" +
        "to just sit there and do nothing. They work, however, for individual testing, and should " +
        "not be discarded")
public class SITableWriterHBaseTest {
    private static final Logger LOG = Logger.getLogger(TableWriterHBaseTest.class);

    private static final String TABLE_NAME = "TEST_TABLE";
    private static HBaseTestingUtility testUtil = new HBaseTestingUtility();
    private TableWriter testWriter;

    private ExecutorService executor;

    @BeforeClass
    public static void setUpClass() throws Exception{
        testUtil.getConfiguration().addResource("hbase-site-local.xml");
        //turn off cache-updating.
        testUtil.getConfiguration().setLong("hbase.htable.regioncache.updateinterval",Long.MAX_VALUE);
        //make sure we do lots of flushes to maximize chances of error
        testUtil.getConfiguration().setInt("hbase.client.write.buffer.maxentries",50);
        testUtil.getConfiguration().reloadConfiguration();

        testUtil.startMiniCluster();
    }

    @Before
    public void setUp() throws Exception{
        HBaseAdmin admin = testUtil.getHBaseAdmin();

        HTableDescriptor tableDesc = new HTableDescriptor(TABLE_NAME);
        tableDesc.addCoprocessor("com.splicemachine.derby.hbase.SpliceIndexEndpoint");
        tableDesc.addCoprocessor("com.splicemachine.hbase.TestWriteLoader");
        tableDesc.addCoprocessor("com.splicemachine.si.coprocessors.SIObserver");
        tableDesc.addFamily(new HColumnDescriptor("attributes"));
        tableDesc.addFamily(new HColumnDescriptor("_si"));

        admin.createTable(tableDesc);

        testWriter = TableWriter.create(testUtil.getConfiguration());
        testWriter.start();
        executor = Executors.newFixedThreadPool(4);
    }

    @After
    public void tearDown() throws Exception{
        testWriter.shutdown();
        executor.shutdownNow();
        testUtil.deleteTable(TABLE_NAME.getBytes());
    }

    @AfterClass
    public static void tearDownClass() throws Exception{
        testUtil.shutdownMiniCluster();
    }

    @Test(timeout = 10000)
    public void testCanHandleHBaseSplits() throws Exception {
        LOG.debug("Allowing some writes through");
        CallBuffer<Mutation> writeBuffer =
                testWriter.synchronousWriteBuffer(TABLE_NAME.getBytes(),new TableWriter.FlushWatcher() {
                    @Override
                    public List<Mutation> preFlush(List<Mutation> mutations) throws Exception {
                        return mutations;
                    }

                    @Override
                    public Response globalError(Throwable t) throws Exception {
                        return t instanceof NotServingRegionException ? Response.RETRY: Response.THROW_ERROR;
                    }

                    @Override
                    public Response partialFailure(MutationRequest request,MutationResponse response) throws Exception {
                        Collection<MutationResult> errors = response.getFailedRows().values();
                        for(MutationResult error : errors){
                            if(!error.isRetryable())
                                return Response.THROW_ERROR;
                        }
                        return Response.RETRY;
                    }
                });

        LOG.debug("Adding 10 items to buffer before waiting for approval");
        for(int i=0;i<20;i+=2){
            LOG.trace("writing row "+i+":"+i+":"+(i*10));
            Put put = new Put(Bytes.toBytes(i));
            put.setAttribute("si-transaction-id",Bytes.toBytes(1));
            put.add("attributes".getBytes(),Bytes.toBytes(i),Bytes.toBytes(i*10));
            writeBuffer.add(put);
        }
        LOG.debug("Flushing buffer");
        writeBuffer.flushBuffer();

        LOG.debug("splitting table");
        //split the table
        HBaseAdmin admin = testUtil.getHBaseAdmin();
        admin.split(TABLE_NAME.getBytes(), Bytes.toBytes(10));
        LOG.debug("table split requested");

        //allow the next batch of writes through

        LOG.debug("Continuing writes, adding 10 more items to buffer");
        for(int i=1;i<=20;i+=2){
            LOG.trace("writing row "+i+":"+i+":"+(i*10));
            Put put = new Put(Bytes.toBytes(i));
            put.setAttribute("si-transaction-id",Bytes.toBytes(1));
            put.add("attributes".getBytes(),Bytes.toBytes(i),Bytes.toBytes(i*10));
            writeBuffer.add(put);
        }
        LOG.debug("Flushing buffer");
        writeBuffer.flushBuffer();
        LOG.debug("Closing buffer");
        writeBuffer.close();

        //make sure that there are 20 rows
        Assert.assertEquals("Incorrect row count!", 20, testUtil.countRows(new HTable(testUtil.getConfiguration(), TABLE_NAME.getBytes())));
    }

    @Test(timeout = 10000)
    public void testCanSplitRegionUnderWritingCoprocessor() throws Throwable {
        final HTable table = new HTable(testUtil.getConfiguration(),TABLE_NAME.getBytes());

        final CountDownLatch latch = new CountDownLatch(1);
        final int size = 1000;
        Future<Void> writeFuture = executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                try {
                    table.coprocessorExec(LoaderProtocol.class, new byte[0], new byte[0], new Batch.Call<LoaderProtocol, Object>() {
                        @Override
                        public Object call(LoaderProtocol instance) throws IOException {
                            try {
                                instance.loadSomeData(testUtil.getConfiguration(), TABLE_NAME,size/2,0);
                            } catch (Exception e) {
                                throw new IOException(e);
                            }
                            return null;
                        }
                    });
                    latch.countDown();
                    table.coprocessorExec(LoaderProtocol.class, new byte[0], new byte[0], new Batch.Call<LoaderProtocol, Object>() {
                        @Override
                        public Object call(LoaderProtocol instance) throws IOException {
                            try {
                                instance.loadSomeData(testUtil.getConfiguration(), TABLE_NAME,size/2,size/2);
                            } catch (Exception e) {
                                throw new IOException(e);
                            }
                            return null;
                        }
                    });
                } catch (Throwable throwable) {
                    throw new Exception(throwable);
                }
                return null;
            }
        });

        latch.await();
        //we know there's data in there now, so split the table
        testUtil.getHBaseAdmin().split(TABLE_NAME.getBytes(), Bytes.toBytes(size / 2));

        writeFuture.get();

        Assert.assertEquals("Incorrect row count!", size, testUtil.countRows(table));
    }
}
