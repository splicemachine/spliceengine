package com.splicemachine.pipeline;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.pipeline.api.BulkWriterFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.PipelineMeter;
import com.splicemachine.pipeline.context.NoOpPipelineMeter;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.mem.DirectBulkWriterFactory;
import com.splicemachine.pipeline.mem.DirectPipelineExceptionFactory;
import com.splicemachine.pipeline.traffic.AtomicSpliceWriteControl;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.txn.KeepAliveScheduler;
import com.splicemachine.si.api.txn.TxnStore;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.driver.SIEnvironment;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.DataFilterFactory;
import com.splicemachine.storage.PartitionInfoCache;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class MPipelineEnv  implements PipelineEnvironment{
    private SIEnvironment siEnv;
    private BulkWriterFactory writerFactory;
    private ContextFactoryDriver ctxFactoryDriver;

    public MPipelineEnv(SIEnvironment siEnv) throws IOException{
        super();
        this.siEnv=siEnv;
        this.writerFactory = new DirectBulkWriterFactory(new MappedPipelineFactory(),
                new AtomicSpliceWriteControl(Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE,Integer.MAX_VALUE),
                pipelineExceptionFactory());
        this.ctxFactoryDriver = ContextFactoryDriverService.loadDriver();
    }

    @Override
    public PartitionFactory tableFactory(){
        return siEnv.tableFactory();
    }

    @Override
    public ExceptionFactory exceptionFactory(){
        return siEnv.exceptionFactory();
    }

    @Override
    public SConfiguration configuration(){
        return siEnv.configuration();
    }

    @Override
    public SDataLib dataLib(){
        return siEnv.dataLib();
    }

    @Override
    public TxnStore txnStore(){
        return siEnv.txnStore();
    }

    @Override
    public OperationStatusFactory statusFactory(){
        return siEnv.statusFactory();
    }

    @Override
    public TimestampSource timestampSource(){
        return siEnv.timestampSource();
    }

    @Override
    public TxnSupplier txnSupplier(){
        return siEnv.txnSupplier();
    }

    @Override
    public IgnoreTxnCacheSupplier ignoreTxnSupplier(){
        return siEnv.ignoreTxnSupplier();
    }

    @Override
    public RollForward rollForward(){
        return siEnv.rollForward();
    }

    @Override
    public TxnOperationFactory operationFactory(){
        return siEnv.operationFactory();
    }

    @Override
    public SIDriver getSIDriver(){
        return siEnv.getSIDriver();
    }

    @Override
    public PartitionInfoCache partitionInfoCache(){
        return siEnv.partitionInfoCache();
    }

    @Override
    public KeepAliveScheduler keepAliveScheduler(){
        return siEnv.keepAliveScheduler();
    }

    @Override
    public DataFilterFactory filterFactory(){
        return siEnv.filterFactory();
    }

    @Override
    public Clock systemClock(){
        return siEnv.systemClock();
    }

    @Override
    public KeyedReadResolver keyedReadResolver(){
        return siEnv.keyedReadResolver();
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return DirectPipelineExceptionFactory.INSTANCE;
    }

    @Override
    public PipelineDriver getPipelineDriver(){
        return PipelineDriver.driver();
    }

    @Override
    public ContextFactoryDriver contextFactoryDriver(){
        return ctxFactoryDriver;
    }

    @Override
    public PipelineCompressor pipelineCompressor(){
        return null;
    }

    @Override
    public BulkWriterFactory writerFactory(){
        return writerFactory;
    }

    @Override
    public PipelineMeter pipelineMeter(){
        return NoOpPipelineMeter.INSTANCE;
    }

    @Override
    public DistributedFileSystem fileSystem(){
        return siEnv.fileSystem();
    }
}
