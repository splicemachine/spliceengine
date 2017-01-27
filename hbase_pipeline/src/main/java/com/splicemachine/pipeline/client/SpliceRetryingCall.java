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

package com.splicemachine.pipeline.client;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 5/13/15
 */
public interface SpliceRetryingCall<T> extends AutoCloseable{
    /**
     * Prepare by setting up any connections to servers, etc., ahead of {@link #call(int)} invocation.
     * @param reload Set this to true if need to requery locations
     * @throws IOException e
     */
    void prepare(final boolean reload) throws IOException;

    /**
     * Called when {@link #call(int)} throws an exception and we are going to retry; take action to
     * make it so we succeed on next call (clear caches, do relookup of locations, etc.).
     * @param t
     * @param retrying True if we are in retrying mode (we are not in retrying mode when max
     * retries == 1; we ARE in retrying mode if retries > 1 even when we are the last attempt)
     */
    void throwable(final Throwable t, boolean retrying);

    /**
     * Computes a result, or throws an exception if unable to do so.
     *
     * @param callTimeout - the time available for this call. 0 for infinite.
     * @return computed result
     * @throws Exception if unable to compute a result
     */
    T call(int callTimeout) throws Exception;

    /**
     * @return Some details from the implementation that we would like to add to a terminating
     * exception; i.e. a fatal exception is being thrown ending retries and we might like to add
     * more implementation-specific detail on to the exception being thrown.
     */
    String getExceptionMessageAdditionalDetail();

    /**
     * @param pause
     * @param tries
     * @return Suggestion on how much to sleep between retries
     */
    long sleep(final long pause, final int tries);

    @Override void close();
}
