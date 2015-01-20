package com.splicemachine.pipeline.coprocessor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.DerbyFactoryImpl;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.impl.BulkWritesRPCInvoker;
import com.splicemachine.pipeline.impl.WriteCoordinator;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.Snowflake;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Scott Fines
 *         Date: 1/15/15
 */
public class WritePipelineLoadGenerator {
    private static final Logger LOG = Logger.getLogger(WritePipelineLoadGenerator.class);
    private static final String tableName = "TEST";
    private static final int numIterations= 100000000;
    private static final int BUFFER_SIZE = 1024;
    private static final int rowByteSize= 128;
    private static final int numThreads = 20;
    private static final long txnId = 3l;

    private static final AtomicBoolean error = new AtomicBoolean(false);
//    private static final TrafficController rateLimiter = TrafficShaping.fixedRateTrafficShaper(100000000,100000000,TimeUnit.SECONDS);

    public static void main(String...args) throws Exception{
        configureLogging();
        BulkWritesRPCInvoker.forceRemote = true;
        DerbyFactoryDriver.derbyFactory = new DerbyFactoryImpl();
        WriteCoordinator coordinator = WriteCoordinator.create(SpliceConstants.config);
        coordinator.start();

        ExecutorService loadExecutor  = Executors.newFixedThreadPool(numThreads,new ThreadFactoryBuilder().setDaemon(true).setNameFormat("loader-%d").build());
        Snowflake snowflake = new Snowflake((short)1);
        try {
            CompletionService<Long> completionService = new ExecutorCompletionService<>(loadExecutor);
            SpliceLogUtils.info(LOG,"Beginning load");
            long startTime = System.currentTimeMillis();
            for(int i=0;i<numThreads;i++){
                completionService.submit(new LoadTask(tableName,coordinator,snowflake));
            }
            long totalRows = 0l;
            for(int i=0;i<numThreads;i++){
                totalRows+=completionService.take().get();
            }
            long stopTime = System.currentTimeMillis();
            double timeSec = (stopTime-startTime)/1000d;
            SpliceLogUtils.info(LOG,"Load of %d rows Completed in %f seconds",totalRows,timeSec);
        }finally{
            coordinator.shutdown();
            loadExecutor.shutdownNow();
        }
    }

    private static void configureLogging() {
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger(Configuration.class).setLevel(Level.FATAL);

        Logger.getLogger(WritePipelineLoadGenerator.class).setLevel(Level.DEBUG);
    }

    private static class LoadTask implements Callable<Long> {
        private final String tableName;
        private final WriteCoordinator writeCoordinator;
        private final Snowflake.Generator uuidGen;

        public LoadTask(String tableName,WriteCoordinator writeCoordinator,Snowflake snowflake) {
            this.tableName = tableName;
            this.writeCoordinator = writeCoordinator;
            this.uuidGen = snowflake.newGenerator(BUFFER_SIZE);
        }

        @Override
        public Long call() throws Exception {
            RecordingCallBuffer<KVPair> callBuffer = writeCoordinator.writeBuffer(tableName.getBytes(),new ActiveWriteTxn(txnId,txnId));
            Random random = new Random();
            long numRows = 0l;

            for (int i = 0; i < numIterations; i++) {
                if(error.get())
                    return numRows;
                //rate limit tester
//                rateLimiter.acquire(8+rowByteSize);
                byte[] key = uuidGen.nextBytes();
                byte[] row = new byte[rowByteSize];
                random.nextBytes(row);
                try {
                    callBuffer.add(new KVPair(key, row));
                }catch(Exception e){
                    error.set(true);
                    LOG.error(e);
                    return numRows;
                }
                numRows++;
                if (numRows % 10000000 == 0) {
                    SpliceLogUtils.info(LOG, "Wrote %d rows", numRows);
                } else if (numRows % 1000000 == 0) {
                    SpliceLogUtils.debug(LOG, "Wrote %d rows", numRows);
                } else if (numRows % 100000 == 0) {
                    SpliceLogUtils.trace(LOG, "Wrote %d rows", numRows);
                }
            }
            try {
                callBuffer.flushBuffer();
                callBuffer.close();
            }catch(Exception e){
                error.set(true);
                LOG.error(e);
            }
            return numRows;
        }
    }
}
