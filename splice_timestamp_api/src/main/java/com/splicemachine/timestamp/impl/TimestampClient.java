/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.timestamp.impl;

import java.util.Random;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import org.apache.log4j.Logger;

/**
 * Accepts concurrent requests for new transactional timestamps and
 * sends them over a shared connection to the remote {@link TimestampServer}.
 * For the caller, the invocation of {@link #getNextTimestamp()}
 * is synchronous.
 * <p>
 * This class should generally not be constructed directly.
 *
 * @author Walt Koetke
 */
public class TimestampClient {

    protected final TimestampConnection connection;
    protected final int timeoutMillis;

    public TimestampClient(int timeoutMillis, TimestampHostProvider timestampHostProvider) {
        this.timeoutMillis = timeoutMillis;
        this.connection = new TimestampConnection(timeoutMillis, timestampHostProvider);
    }

    public void shutdown() {
        connection.shutdown();
    }

    /**
     * Returns the port number which the client should use when connecting
     * to the timestamp server.
     */
    protected int getPort() {
        return connection.getPort();
    }

    protected void connectIfNeeded() throws TimestampIOException{
        connection.connectIfNeeded();
    }

    public void bumpTimestamp(long timestamp) throws TimestampIOException {
        TimestampMessage.TimestampRequest.Builder requestBuilder = TimestampMessage.TimestampRequest.newBuilder()
                .setTimestampRequestType(TimestampMessage.TimestampRequestType.BUMP_TIMESTAMP)
                .setBumpTimestamp(TimestampMessage.BumpTimestamp.newBuilder().setTimestamp(timestamp));
        connection.issueRequest(requestBuilder);
    }

    public long getNextTimestamp() throws TimestampIOException {
        return connection.getSingleTimestamp();
    }

    public long getCurrentTimestamp() throws TimestampIOException {
        TimestampMessage.TimestampRequest.Builder requestBuilder = TimestampMessage.TimestampRequest.newBuilder()
                .setTimestampRequestType(TimestampMessage.TimestampRequestType.GET_CURRENT_TIMESTAMP);
        return connection.issueRequest(requestBuilder).getGetCurrentTimestampResponse().getTimestamp();
    }
}
