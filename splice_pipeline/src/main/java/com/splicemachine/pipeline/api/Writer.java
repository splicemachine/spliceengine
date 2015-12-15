package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.impl.BulkWrites;
import javax.management.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Interface for performing physical writes
 * 
 * @author Scott Fines
 * Created on: 8/8/13
 */
public interface Writer {

    public Future<WriteStats> write(byte[] tableName, BulkWrites action, WriteConfiguration writeConfiguration) throws ExecutionException;

    void stopWrites();

    public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,NotCompliantMBeanException,InstanceAlreadyExistsException,MBeanRegistrationException;

}
