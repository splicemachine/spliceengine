package com.splicemachine.lifecycle;

import javax.management.MBeanServer;

/**
 * @author Scott Fines
 *         Date: 1/4/16
 */
public interface DatabaseLifecycleService{

    void start() throws Exception;

    void registerJMX(MBeanServer mbs) throws Exception;

    void shutdown() throws Exception;
}
