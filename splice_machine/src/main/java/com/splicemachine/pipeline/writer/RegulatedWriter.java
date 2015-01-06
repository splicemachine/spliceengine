package com.splicemachine.pipeline.writer;

import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteRejectedHandler;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.impl.BulkWrites;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Writer which regulates how many concurrent writes are allowed, gating flushes as necessary (forcing flushes
 * onto the calling thread if the maximum is exceeded.
 *
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class RegulatedWriter implements Writer {
    private final Writer delegate;

    public RegulatedWriter(Writer delegate) {
        this.delegate = delegate;
    }

    @Override
    public Future<WriteStats> write(byte[] tableName,
                                    BulkWrites action,
                                    WriteConfiguration writeConfiguration) throws ExecutionException {
        return delegate.write(tableName, action, writeConfiguration);
    }

    @Override
    public void stopWrites() {
        throw new UnsupportedOperationException("Stop underlying writer instance instead");
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        throw new UnsupportedOperationException("register underlying writer instance instead");
    }

}