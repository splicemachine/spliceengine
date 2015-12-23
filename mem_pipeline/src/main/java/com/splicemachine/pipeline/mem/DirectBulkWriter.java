package com.splicemachine.pipeline.mem;

import com.google.common.base.Function;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.pipeline.PartitionWritePipeline;
import com.splicemachine.pipeline.api.BulkWriter;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.*;
import com.splicemachine.pipeline.context.NoOpPipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
import com.splicemachine.pipeline.writehandler.SharedCallBufferFactory;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.TransactionalRegionFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class DirectBulkWriter implements BulkWriter{
    private final PartitionFactory<Object> partitionFactory;
    private final SConfiguration config;
    private final PipelineExceptionFactory exceptionFactory;
    private final Map<Long,ContextFactoryLoader> factoryLoaderMap;
    private final TransactionalRegionFactory txnRegionFactory;
    private final ServerControl serverControl;
    private final WriteCoordinator writeCoordinator;

    public DirectBulkWriter(PartitionFactory<Object> partitionFactory,
                            SConfiguration config,
                            PipelineExceptionFactory exceptionFactory,
                            Map<Long,ContextFactoryLoader> factoryLoaderMap,
                            TransactionalRegionFactory txnRegionFactory,
                            ServerControl serverControl,
                            WriteCoordinator writeCoordinator){
        this.partitionFactory=partitionFactory;
        this.config=config;
        this.exceptionFactory=exceptionFactory;
        this.txnRegionFactory = txnRegionFactory;
        this.serverControl=serverControl;
        this.writeCoordinator=writeCoordinator;
        this.factoryLoaderMap = factoryLoaderMap;
    }

    @Override
    public BulkWritesResult write(BulkWrites write,boolean refreshCache) throws IOException{
        Collection<BulkWrite> bulkWrites=write.getBulkWrites();
        Collection<Pair<PartitionWritePipeline,BulkWriteResult>> interResults = new ArrayList<>(bulkWrites.size());
        TxnView txn = write.getTxn();
        SharedCallBufferFactory bFactory = new SharedCallBufferFactory(writeCoordinator);
        for(BulkWrite bw:bulkWrites){
           interResults.add(submitWrite(txn,bw,bFactory));
        }
        Collection<BulkWriteResult> finalResults = new ArrayList<>(bulkWrites.size());
        Iterator<BulkWrite> bwIter = bulkWrites.iterator();
        Iterator<Pair<PartitionWritePipeline,BulkWriteResult>> interIter = interResults.iterator();
        //noinspection WhileLoopReplaceableByForEach
        while(interIter.hasNext()){
            if(!bwIter.hasNext())
                throw new IllegalStateException("Unexpected end of iterator");
            Pair<PartitionWritePipeline,BulkWriteResult> interR = interIter.next();
            BulkWrite bw = bwIter.next();
            finalResults.add(interR.getFirst().finishWrite(interR.getSecond(),bw));
        }
        return new BulkWritesResult(finalResults);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private Pair<PartitionWritePipeline,BulkWriteResult> submitWrite(TxnView writingTxn,BulkWrite bw,SharedCallBufferFactory bFactory) throws IOException{
        Partition partition = partitionFactory.getTable(bw.getEncodedStringName());
        long conglomerateId=Long.parseLong(partition.getName());
        WriteContextFactory<TransactionalRegion> lwf = WriteContextFactoryManager.getWriteContext(conglomerateId,
                config,
                partitionFactory,
                exceptionFactory,
                new Function<Object, String>(){ @Override public String apply(Object input){ return (String)input; } },
                factoryLoaderMap.get(conglomerateId)
        );
        lwf.prepare();
        TransactionalRegion tr = txnRegionFactory.newRegion(partition);
        PartitionWritePipeline pwp = new PartitionWritePipeline(serverControl,partition,lwf,
                tr,
                NoOpPipelineMeter.INSTANCE,
                exceptionFactory
        );
        BulkWriteResult bulkWriteResult=pwp.submitBulkWrite(writingTxn,bw,bFactory,serverControl);

        return Pair.newPair(pwp,bulkWriteResult);
    }
}
