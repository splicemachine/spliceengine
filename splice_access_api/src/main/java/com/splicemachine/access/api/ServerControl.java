/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
