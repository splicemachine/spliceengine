package com.splicemachine.derby.lifecycle;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public interface DistributedDerbyStartup{
    /**
     * Iniitiate and execute the distributed startup. This does
     * whatever is necessary (like talk to HMaster in HBase, etc) to ensure
     * that the distributed portion of the startup sequenece is managed correctly.
     */
    void distributedStart() throws IOException;

    void markBootFinished() throws IOException;

    /**
     * @return true if we are to assume this is the first time we've ever
     *  booted (e.g. create new tables)
     */
    boolean connectAsFirstTime();
}
