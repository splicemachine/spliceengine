package com.splicemachine.hbase.writer;

import com.splicemachine.tools.Valve;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

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
        this.valve = new Valve(new Valve.FixedMaxOpeningPolicy(maxConcurrentWrites)); //TODO -sf- make adaptive OpeningPolicy
    }


    @Override
    public Future<Void> write(byte[] tableName,
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

    public static interface WriteRejectedHandler{

        public Future<Void> writeRejected(byte[] tableName,BulkWrite action, WriteConfiguration writeConfiguration) throws ExecutionException;
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
        public Future<Void> writeRejected(byte[] tableName, BulkWrite action, WriteConfiguration writeConfiguration) throws ExecutionException {
            return otherWriter.write(tableName,action,writeConfiguration);
        }
    }


    private class ClosingWriteConfiguration implements WriteConfiguration {
        private WriteConfiguration writeConfiguration;

        public ClosingWriteConfiguration(WriteConfiguration writeConfiguration) {
            this.writeConfiguration = writeConfiguration;
        }

        @Override public int getMaximumRetries() { return writeConfiguration.getMaximumRetries(); }
        @Override public WriteResponse globalError(Throwable t) throws ExecutionException { return writeConfiguration.globalError(t); }
        @Override public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException { return writeConfiguration.partialFailure(result, request); }
        @Override public long getPause() { return writeConfiguration.getPause(); }

        @Override
        public void writeComplete() {
            valve.release();
            writeConfiguration.writeComplete();
        }
    }
}
