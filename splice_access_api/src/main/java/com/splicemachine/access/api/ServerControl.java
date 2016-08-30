/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access.api;

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

    /**
     * Determine if we should treat the "server" as available or not. If true, then this server
     * can be considered active and actions can be taken against it. If false, then actions should
     * not be taken.
     *
     * This is particularly prevalent in systems where a "server" is differentiated by sub-units (such as with
     * HBase, where a "server" is really a region + network layer).
     *
     * @return {@code true} if the server should be considered available, {@code false} otherwise.
     */
    boolean isAvailable();
}
