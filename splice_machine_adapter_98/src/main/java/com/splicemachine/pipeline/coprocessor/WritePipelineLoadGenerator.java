package com.splicemachine.pipeline.coprocessor;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.hbase.DerbyFactoryImpl;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
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
    private static final int numThreads = 10;
    private static final long txnId = 3l;

    public static void main(String...args) throws Exception{
        configureLogging();
        BulkWritesRPCInvoker.forceRemote = true;
        DerbyFactoryDriver.derbyFactory = new DerbyFactoryImpl();
        WriteCoordinator coordinator = WriteCoordinator.create(SpliceConstants.config);

        ExecutorService loadExecutor  = Executors.newFixedThreadPool(numThreads,new ThreadFactoryBuilder().setDaemon(true).setNameFormat("loader-%d").build());
        Snowflake snowflake = new Snowflake((short)1);
        try {
            CompletionService<Void> completionService = new ExecutorCompletionService<>(loadExecutor);
            SpliceLogUtils.info(LOG,"Beginning load");
            for(int i=0;i<numThreads;i++){
                completionService.submit(new LoadTask(tableName,coordinator,snowflake));
            }
            for(int i=0;i<numThreads;i++){
                completionService.take().get();
            }
        }finally{
            SpliceLogUtils.info(LOG,"Load Complete");
            coordinator.shutdown();
            loadExecutor.shutdownNow();
        }
    }

    private static void configureLogging() {
        Logger.getRootLogger().setLevel(Level.ERROR);
        Logger.getLogger(Configuration.class).setLevel(Level.FATAL);

        Logger.getLogger(WritePipelineLoadGenerator.class).setLevel(Level.DEBUG);
    }

    private static class LoadTask implements Callable<Void> {
        private final String tableName;
        private final WriteCoordinator writeCoordinator;
        private final Snowflake.Generator uuidGen;

        public LoadTask(String tableName,WriteCoordinator writeCoordinator,Snowflake snowflake) {
            this.tableName = tableName;
            this.writeCoordinator = writeCoordinator;
            this.uuidGen = snowflake.newGenerator(BUFFER_SIZE);
        }

        @Override
        public Void call() throws Exception {

            CallBuffer<KVPair> callBuffer = writeCoordinator.writeBuffer(tableName.getBytes(),new ActiveWriteTxn(txnId,txnId));
            Random random = new Random();
            int numRows = 0;

            for(int i=0;i<numIterations;i++){
                byte[] key = uuidGen.nextBytes();
                byte[] row = new byte[rowByteSize];
                random.nextBytes(row);
                callBuffer.add(new KVPair(key,row));
                numRows++;
                if(numRows%10000000==0){
                    SpliceLogUtils.info(LOG, "Wrote %d rows", numRows);
                } else if(numRows%1000000==0){
                    SpliceLogUtils.debug(LOG,"Wrote %d rows",numRows);
                }else if(numRows%100000==0){
                    SpliceLogUtils.trace(LOG,"Wrote %d rows",numRows);
                }
            }
            callBuffer.flushBuffer();
            callBuffer.close();
            return null;
        }
    }
}
