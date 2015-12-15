package com.splicemachine.si.api.server;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public interface ServerControl{

    /**
     * Tell the server that we are starting an operation. This should prevent
     * the underlying storage from moving around. In HBase, for example, this will
     * prevent region splits.
     *
     * @throws IOException if we are unable to start an operation.
     */
    void startOperation() throws IOException;

    /**
     * Inform the server that an operation is completed. This will allow the underlying
     * storage to start moving around again. If you call this without first calling
     * startOperation(), nothing should happen.
     *
     * @throws IOException if we are unable to stop the operation for some bad reason
     */
    void stopOperation() throws IOException;

    /**
     * Ensure that the calling network connection is still open (i.e. that the client
     * hasn't disconnected on us already). If there is no client connection, or if the client
     * connection is known to have disconnected, throw an error.
     *
     * Don't call this from a situation where you may not be in a network operation, otherwise,you'll
     * get spurious errors.
     *
     * @throws IOException if there is no open network channel
     */
    void ensureNetworkOpen() throws IOException;
}
