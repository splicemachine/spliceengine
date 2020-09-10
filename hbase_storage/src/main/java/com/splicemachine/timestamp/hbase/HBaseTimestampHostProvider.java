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

package com.splicemachine.timestamp.hbase;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.timestamp.impl.TimestampClient;
import com.splicemachine.timestamp.api.TimestampHostProvider;
import org.apache.log4j.Logger;

/**
 * HBase-based Timestamp host provider.
 *
 * Created by jleach on 12/9/15.
 */
public class HBaseTimestampHostProvider implements TimestampHostProvider {
    private static final Logger LOG = Logger.getLogger(HBaseTimestampHostProvider.class);
    private final HBaseConnectionFactory connectionFactory;
    private final int port;

    public HBaseTimestampHostProvider(HBaseConnectionFactory connectionFactory, int port){
        this.connectionFactory=connectionFactory;
        this.port=port;
    }

    public String getHost() throws TimestampIOException{
        String hostName = null;
        try {
            hostName = connectionFactory.getMasterServer().getHostname();
        } catch (Exception e) {
            TimestampClient.throwClientError(LOG, "Unable to determine host name for active hbase master", e);
        }
        return hostName;
    }

    public int getPort() {
        return port;
    }

}
