
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
 *
 */

package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import com.splicemachine.timestamp.api.TimestampIOException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.when;

public class TimestampClientTest {

    @Test
    public void testExceptionDuringConnection() throws TimestampIOException {
        TimestampServer ts = new TimestampServer(0, new TimestampServerHandler(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 10));
        ts.startServer();

        int port = ts.getBoundPort();

        TimestampHostProvider hostProvider = Mockito.mock(TimestampHostProvider.class, Mockito.RETURNS_DEEP_STUBS);
        TimestampClient tc = new TimestampClient(1000,  hostProvider);

        when(hostProvider.getHost()).thenThrow(new RuntimeException("Failure")).thenReturn("localhost");
        when(hostProvider.getPort()).thenReturn(port);


        // We raise an exception during the first connection, but the next should succeed
        try {
            tc.connectIfNeeded();
            fail("Expected exception");
        } catch(Exception e) {
            //ignore
        }

        // This one should succeed
        tc.connectIfNeeded();

        // We make sure the connection is active
        tc.getNextTimestamp();
        
        ts.stopServer();
    }


    @Test
    public void testExceptionDoesntLeaveUsedClientIds() throws Exception {

        TimestampServerHandler tsh = new TimestampServerHandler(Mockito.mock(TimestampBlockManager.class, Mockito.RETURNS_DEEP_STUBS), 1000) {
            // Force an exception the first time a message is received on the server side
            
            boolean first = true;
            @Override
            public void messageReceived(ChannelHandlerContext ctx, MessageEvent e) throws Exception {
                if (first) {
                    first = false;
                    throw new RuntimeException("First call");
                }
                super.messageReceived(ctx, e);
            }
        };

        TimestampServer ts = new TimestampServer(0, tsh);
        ts.startServer();

        int port = ts.getBoundPort();

        TimestampHostProvider hostProvider = new TimestampHostProvider() {
            @Override
            public String getHost() {
                return "localhost";
            }
            @Override
            public int getPort() {
                return port;
            }
        };
        TimestampClient tc = new TimestampClient(1000,  hostProvider);

        tc.connectIfNeeded();

        try {
            tc.getNextTimestamp();
            fail("Expected exception");
        } catch (Exception e) {
            // expected
            e.printStackTrace();
        }

        // Make sure we use all client ids and wrap around (64K)
        for (int i = 0; i < 80000; ++i) {
            tc.getNextTimestamp();
        }

    }
}
