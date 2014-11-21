package com.splicemachine.pipeline.writer;

import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteRejectedHandler;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;
import com.splicemachine.pipeline.writeconfiguration.ForwardingWriteConfiguration;
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
	private static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private final Writer delegate;
    private final WriteRejectedHandler writeRejectedHandler;
    private final Valve valve;

    public RegulatedWriter(Writer delegate,
                           WriteRejectedHandler writeRejectedHandler,
                           int maxConcurrentWrites ) {
        this.delegate = delegate;
        this.writeRejectedHandler = writeRejectedHandler;
		this.valve = new SemaphoreValve(new SemaphoreValve.PassiveOpeningPolicy(maxConcurrentWrites));
    }


    @Override
    public Future<WriteStats> write(byte[] tableName,
											BulkWrites action,
											WriteConfiguration writeConfiguration) throws ExecutionException {
    	/*
        int version = valve.tryAllow();
        if(version<0){
            //The valve is full, so reject the write
            return writeRejectedHandler.writeRejected(tableName, action, writeConfiguration);
        }
        */
        //The valve has allowed us through
      //  return delegate.write(tableName,action,new ClosingWriteConfiguration(writeConfiguration));
          return delegate.write(tableName,action,writeConfiguration);

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
            if(derbyFactory.isRegionTooBusyException(t))
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
