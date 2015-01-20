package com.splicemachine.pipeline.coprocessor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.ByteString;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.hbase.table.SpliceHTable;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.RemoteWithExtrasException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.zookeeper.ClientCnxn;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 *         Date: 1/14/15
 */
public class MultiRowLoadGenerator {
    private static final Logger LOG = Logger.getLogger(MultiRowLoadGenerator.class);

    private static final int BUFFER_SIZE = 8192;
    private static final int rowByteSize = 128;
    private static final int numIterations = 10000;
    private static final int numThreads = 1;

    public static void main(String...args)throws Exception{
        configureLogging();
        String tableName = "TEST";
        Configuration configuration = new Configuration();
        configuration.set("hbase.zookeeper.quorum","127.0.0.1:2181");

        try(HBaseAdmin admin = new HBaseAdmin(configuration)) {
            cleanOldData(admin, tableName);
        }

        ExecutorService executor = Executors.newFixedThreadPool(numThreads,new ThreadFactoryBuilder()
                .setDaemon(true).setNameFormat("writer-%d").build());
        CompletionService<Void> service = new ExecutorCompletionService<>(executor);
        SpliceLogUtils.info(LOG,"Beginning execution on %d threads",numThreads);
        try {
            Snowflake snowflake = new Snowflake((short) 1);
            for (int i = 0; i < numThreads; i++) {
                service.submit(new LoadTask(configuration, snowflake, tableName));
            }
            for (int i = 0; i < numThreads; i++) {
                service.take().get();
            }
        }finally{
            SpliceLogUtils.info(LOG, "Execution complete");
            executor.shutdownNow();
        }
    }

    private static void configureLogging() {
        Logger.getLogger(Configuration.class).setLevel(Level.FATAL);
        Logger.getLogger(ClientCnxn.class).setLevel(Level.ERROR);
        Logger.getLogger(ZooKeeper.class).setLevel(Level.ERROR);
        Logger.getLogger(RecoverableZooKeeper.class).setLevel(Level.ERROR);

        Logger.getLogger(MultiRowLoadGenerator.class).setLevel(Level.INFO);
    }

    private static void cleanOldData(HBaseAdmin admin, String tableName) throws IOException {
        HTableDescriptor tableDescriptor;
        try {
            tableDescriptor = admin.getTableDescriptor(tableName.getBytes());
//            return;
            admin.disableTable(tableName);
            admin.deleteTable(tableName);
        }catch(TableNotFoundException tnfe){
            tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
            tableDescriptor.addCoprocessor("com.splicemachine.pipeline.coprocessor.MultiRowEndpoint");
            HColumnDescriptor col = new HColumnDescriptor(SpliceConstants.DEFAULT_FAMILY_BYTES);
            tableDescriptor.addFamily(col);
        }

        admin.createTable(tableDescriptor);
    }

    private static class LoadTask implements Callable<Void> {
        private final Configuration configuration;
        private final Snowflake.Generator snowflake;
        private final String tableName;
        private final AtomicLong totalBytesFlushed = new AtomicLong(0);
        private final AtomicLong totalNetworkCalls = new AtomicLong(0);

        public LoadTask(Configuration configuration, Snowflake snowflake, String tableName) {
            this.configuration = configuration;
            this.snowflake = snowflake.newGenerator(BUFFER_SIZE);
            this.tableName = tableName;
        }

        @Override
        public Void call() throws Exception {
            SpliceMessage.KV[] buffer = new SpliceMessage.KV[BUFFER_SIZE];
            int bufferPos = 0;
            int totalWritten = 0;
            int flushCount =0;
            Random random = new Random();

            byte[] rowData = new byte[rowByteSize];
            try(HTableInterface table = new SpliceHTable(tableName.getBytes(),configuration,true)){
                SpliceLogUtils.info(LOG, "Beginning load of %d records", numIterations);
                for(int i=0;i<numIterations;i++) {
                    byte[] key = snowflake.nextBytes();
                    random.nextBytes(rowData);
                    buffer[bufferPos] = SpliceMessage.KV.newBuilder().setKey(ZeroCopyLiteralByteString.wrap(key))
                            .setRow(ByteString.copyFrom(rowData)).build();
                    bufferPos = (bufferPos + 1) & (BUFFER_SIZE - 1);
                    if (bufferPos == 0) {
                        flush(table, buffer, BUFFER_SIZE);
                        totalWritten+=BUFFER_SIZE;
                        flushCount++;
                        if(flushCount % 100 ==0){
                            if(flushCount % 10000 == 0)
                                SpliceLogUtils.info(LOG,"Loaded %d records",totalWritten);
                            else if(flushCount % 1000 == 0)
                                SpliceLogUtils.debug(LOG,"Loaded %d records",totalWritten);
                            else
                                SpliceLogUtils.trace(LOG,"Loaded %d records",totalWritten);
                        }

                    }
                }
                flush(table,buffer,bufferPos);
                SpliceLogUtils.debug(LOG,"Load complete");
            }

            if(LOG.isInfoEnabled()){
                double avgBytesPerNetworkCall = ((double)totalBytesFlushed.get())/totalNetworkCalls.get();
                LOG.info("Avg Bytes/Network Call: "+avgBytesPerNetworkCall);
            }
            return null;
        }

        private void flush(HTableInterface table, final SpliceMessage.KV[] buffer,final int size) throws Exception {
            byte[] minKey= null;
            byte[] maxKey = null;
            for(int i=0;i<size;i++){
                byte[] compare = buffer[i].getKey().toByteArray();
                if(minKey==null || Bytes.compareTo(minKey,compare)>0){
                    minKey =compare;
                }
                if(maxKey==null || Bytes.compareTo(maxKey,compare)<0){
                    maxKey = compare;
                }
            }

            SpliceLogUtils.trace(LOG,"flushing %d records",size);
            try {
                table.coprocessorService(SpliceMessage.MultiRowService.class, HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW, new BoundCall<SpliceMessage.MultiRowService, SpliceMessage.MultiRowResponse>() {
                    @Override
                    public SpliceMessage.MultiRowResponse call(SpliceMessage.MultiRowService instance) throws IOException {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public SpliceMessage.MultiRowResponse call(byte[] startKey, byte[] stopKey, SpliceMessage.MultiRowService instance) throws IOException {
                        SpliceMessage.MultiRowRequest.Builder mrwb = SpliceMessage.MultiRowRequest.newBuilder();
                        mrwb = mrwb.setTimestamp(1l);
                        long totalBytes = 0;
                        for(int i=0;i<size;i++){
                            SpliceMessage.KV kv = buffer[i];
                            if(BytesUtil.isRowInRange(kv.getKey().toByteArray(),startKey,stopKey)) {
                                totalBytes += kv.getRow().size() + kv.getKey().size();
                                mrwb.addKvs(kv);
                            }
                        }
                        LoadTask.this.totalBytesFlushed.addAndGet(totalBytes);
                        LoadTask.this.totalNetworkCalls.incrementAndGet();

                        SpliceRpcController controller = new SpliceRpcController();
                        BlockingRpcCallback<SpliceMessage.MultiRowResponse> responseCallback = new BlockingRpcCallback<>();
                        return doWrite(instance, mrwb, controller, responseCallback, 0, null);
                    }
                });

                SpliceLogUtils.trace(LOG,"Flush complete");
            } catch (Throwable throwable) {
                throw new ExecutionException(throwable);
            }
        }

        private SpliceMessage.MultiRowResponse doWrite(SpliceMessage.MultiRowService instance,
                                                       SpliceMessage.MultiRowRequest.Builder mrwb,
                                                       SpliceRpcController controller,
                                                       BlockingRpcCallback<SpliceMessage.MultiRowResponse> responseCallback,
                                                       int tryCount,
                                                       Throwable lastException) throws IOException {
            if(tryCount>50) {
                SpliceLogUtils.error(LOG,"Received a RegionTooBusyException over 50 times, failing");
                throw Exceptions.getIOException(lastException);
            }
            instance.bulkWrite(controller, mrwb.build(), responseCallback);

            Throwable t = controller.getThrowable();
            if (t != null) {
                SpliceLogUtils.trace(LOG, "Got an error", t);
                if(t instanceof RegionTooBusyException){
                    SpliceLogUtils.debug(LOG,"Got a RegionTooBusyException, retrying");
                    //wait a second and try again
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) { }
                    doWrite(instance,mrwb,controller,responseCallback,tryCount+1,t);
                }else{
                    if (t instanceof RemoteWithExtrasException){
                        t = ((RemoteWithExtrasException)t).unwrapRemoteException();
                    }

                    if(t instanceof NotServingRegionException){
                        SpliceLogUtils.debug(LOG,"Got a NotServingRegionException, retrying");
                        //wait a second then try again
                        try{
                            Thread.sleep(1000);
                        }catch(InterruptedException ignored){  }
                        HBaseRegionCache.getInstance().invalidate(tableName.getBytes());
                        doWrite(instance,mrwb,controller,responseCallback,tryCount+1,t);
                    } else
                        throw Exceptions.getIOException(t);
                }
            }
            return responseCallback.get();
        }
    }
}
