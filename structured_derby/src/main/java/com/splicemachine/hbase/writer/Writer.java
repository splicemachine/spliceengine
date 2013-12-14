package com.splicemachine.hbase.writer;

import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public interface Writer {

    public Future<Void> write(byte[] tableName,BulkWrite action,WriteConfiguration writeConfiguration) throws ExecutionException;

    void stopWrites();

    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,NotCompliantMBeanException,InstanceAlreadyExistsException,MBeanRegistrationException;

    public enum WriteResponse{
        THROW_ERROR,
        RETRY,
        IGNORE
    }

    public interface WriteConfiguration {

        int getMaximumRetries();

        WriteResponse globalError(Throwable t) throws ExecutionException;

        WriteResponse partialFailure(BulkWriteResult result,BulkWrite request) throws ExecutionException;

        long getPause();

        void writeComplete();
    }

}
