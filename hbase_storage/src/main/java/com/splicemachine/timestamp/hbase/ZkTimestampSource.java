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

package com.splicemachine.timestamp.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.impl.TimestampClient;
import com.splicemachine.timestamp.impl.TimestampServer;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Timestamp source implementation that utilizes a special purpose service
 * {@link TimestampServer} to provide transaction timestamps. To access
 * this service, a singleton client instance of {@link TimestampClient}
 * is used to manage the concurrent requests that come through this
 * timestamp source.
 *
 * @author Walt Koetke
 */
public class ZkTimestampSource implements TimestampSource {

    private static final Logger LOG = Logger.getLogger(ZkTimestampSource.class);

    private RecoverableZooKeeper _rzk;
    private volatile TimestampClient _tc = null;
    private String rootZkPath;

    public ZkTimestampSource(SConfiguration config,RecoverableZooKeeper rzk) {
        _rzk = rzk;
        initialize(config);
    }
    
    private void initialize(SConfiguration config) {
    	// We synchronize because we only want one instance of TimestampClient
    	// per region server, each of which handles multiple concurrent requests.
    	// Should be fine since synchronization occurs on the server.
    	synchronized(this) {
    		if (_tc == null) {
                rootZkPath = config.getSpliceRootPath();
                int timeout = config.getTimestampClientWaitTime();
                int timestampPort = config.getTimestampServerBindPort();
		    	LOG.info("Creating the TimestampClient...");
                HBaseConnectionFactory hbcf = HBaseConnectionFactory.getInstance(config);
                _tc = new TimestampClient(timeout,
                        new HBaseTimestampHostProvider(hbcf,timestampPort));
    		}
    	}
    }
    
    protected TimestampClient getTimestampClient() {
        if (_tc == null) {
            LOG.error("The timestamp source has been closed.");
            throw new RuntimeException("The timestamp source has been closed.");
        }
    	return _tc;
    }
    
    @Override
    public long nextTimestamp() {
		TimestampClient client = getTimestampClient();
				
		long nextTimestamp;
		try {
			nextTimestamp = client.getNextTimestamp();
		} catch (Exception e) {
			LOG.error("Unable to fetch new timestamp", e);
			throw new RuntimeException("Unable to fetch new timestamp", e);
		}

		SpliceLogUtils.debug(LOG, "Next timestamp: %s", nextTimestamp);
		
		return nextTimestamp;
	}

	// The following two are same as ZooKeeperStatTimestampSource,
	// and can probably stay this way.
	
    @Override
    public void rememberTimestamp(long timestamp) {
        byte[] data = Bytes.toBytes(timestamp);
        try {
            _rzk.setData(rootZkPath+HConfiguration.MINIMUM_ACTIVE_PATH, data, -1);
        } catch (Exception e) {
            LOG.error("Couldn't remember timestamp", e);
            throw new RuntimeException("Couldn't remember timestamp",e);
        }
    }

    @Override
    public long retrieveTimestamp() {
        byte[] data;
        try {
            data = _rzk.getData(rootZkPath+HConfiguration.MINIMUM_ACTIVE_PATH, false, null);
        } catch (Exception e) {
            LOG.error("Couldn't retrieve minimum timestamp", e);
            return 0;
        }
        return Bytes.toLong(data);
    }

    @Override
    public synchronized void shutdown() {
        if(_tc != null) {
            _tc.shutdown();
            _tc = null;
        }
    }
}
