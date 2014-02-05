package com.splicemachine.hbase.writer;

import com.splicemachine.tools.SemaphoreValve;
import com.splicemachine.tools.Valve;
import org.apache.hadoop.hbase.RegionTooBusyException;
import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Writer which regulates how many concurrent writes are allowed, gating flushes as necessary (forcing flushes
 * onto the calling thread if the maximum is exceeded.
 *
 * @author Scott Fines
 * Created on: 9/6/13
 */
public class RegulatedWriter implements Writer{
    private final Writer delegate;
    private final WriteRejectedHandler writeRejectedHandler;

    private final Valve valve;

    public RegulatedWriter(Writer delegate,
                           WriteRejectedHandler writeRejectedHandler,
                           int maxConcurrentWrites ) {
        this.delegate = delegate;
        this.writeRejectedHandler = writeRejectedHandler;
				this.valve = new SemaphoreValve(new SemaphoreValve.PassiveOpeningPolicy(maxConcurrentWrites));
//				this.valve = new OptimizingValve(maxConcurrentWrites,
//								maxConcurrentWrites,
//								new MovingThreshold(MovingThreshold.OptimizationStrategy.MINIMIZE,16),
//								new MovingThreshold(MovingThreshold.OptimizationStrategy.MAXIMIZE,16));
    }


    @Override
    public Future<WriteStats> write(byte[] tableName,
																		BulkWrite action,
																		WriteConfiguration writeConfiguration) throws ExecutionException {
        int version = valve.tryAllow();
        if(version<0){
            //The valve is full, so reject the write
            return writeRejectedHandler.writeRejected(tableName, action, writeConfiguration);
        }
        //The valve has allowed us through
        return delegate.write(tableName,action,new ClosingWriteConfiguration(writeConfiguration));
    }

    @Override
    public void stopWrites() {
        throw new UnsupportedOperationException("Stop underlying writer instance instead");
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        throw new UnsupportedOperationException("register underlying writer instance instead");
    }

    public int getCurrentMaxFlushes() {
        return valve.getAvailable();
    }

    public void setCurrentMaxFlushes(int oldMaxFlushes) {
        valve.setMaxPermits(oldMaxFlushes);
    }

    public static interface WriteRejectedHandler{

        public Future<WriteStats> writeRejected(byte[] tableName, BulkWrite action, WriteConfiguration writeConfiguration) throws ExecutionException;
    }

    /**
     * WriteRejectedHandler which delegates to an additional writer (typically a synchronous writer, but perhaps a backup pool).
     *
     * Don't ever pass the same writer to this as you pass to the RegulatedWriter, or it pretty much defeats the purpose
     * of using a regulated writer.
     */
    public static class OtherWriterHandler implements WriteRejectedHandler{

        public OtherWriterHandler(Writer otherWriter) {
            this.otherWriter = otherWriter;
        }

        private final Writer otherWriter;

        @Override
        public Future<WriteStats> writeRejected(byte[] tableName, BulkWrite action, WriteConfiguration writeConfiguration) throws ExecutionException {
            return otherWriter.write(tableName,action,writeConfiguration);
        }
    }

    private class ClosingWriteConfiguration extends ForwardingWriteConfiguration {

        public ClosingWriteConfiguration(WriteConfiguration writeConfiguration) {
						super(writeConfiguration);
        }

        @Override
        public WriteResponse globalError(Throwable t) throws ExecutionException {
            /*
             * If we receive a RegionTooBusyException, then we must properly deal with it.
             *
             * When a RegionTooBusyException happens, we rely on the underlying configuration to wait for the appropriate
             * length of time, but we ALSO adjust the valve down, suggesting that it halve its allowed writes. This will
             * (hopefully) prevent us from overloading the server again.
             */
            if(t instanceof RegionTooBusyException)
                valve.adjustValve(Valve.SizeSuggestion.HALVE);

            return super.globalError(t);
        }

				@Override
				public void writeComplete(long timeTakenMs, long numRecordsWritten) {
						valve.release();
						super.writeComplete(timeTakenMs, numRecordsWritten);
				}
    }
}
