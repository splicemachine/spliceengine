package com.splicemachine.si.txn;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TimestampSource;
import com.splicemachine.si.impl.timestamp.TimestampClient;
import com.splicemachine.si.impl.timestamp.TimestampClientFactory;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Timestamp source implementation that utilizes a special purpose service
 * {@link TimestampServer} to provide transaction timestamps. To access
 * this service, a singleton client instance of {@link TimestampClient}
 * is used to manage the concurrent requests that come through this
 * timestamp source.
 * <p>
 * This class obsoletes {@link ZooKeeperStatTimestampSource},
 * which should no longer be used.
 * 
 * @author Walt Koetke
 */
public class SpliceTimestampSource implements TimestampSource {

    private static final Logger LOG = Logger.getLogger(SpliceTimestampSource.class);

    private RecoverableZooKeeper _rzk;
    private TimestampClient _tc = null;

    public SpliceTimestampSource(RecoverableZooKeeper rzk) {
        _rzk = rzk;
        initialize();
    }
    
    private void initialize() {
    	// We synchronize because we only want one instance of TimestampClient
    	// per region server, each of which handles multiple concurrent requests.
    	// Should be fine since synchronization occurs on the server.
    	synchronized(this) {
    		if (_tc == null) {
		    	LOG.info("Creating the TimestampClient...");
		    	_tc = TimestampClientFactory.createNewInstance();
    		}
    	}
    }
    
    protected TimestampClient getTimestampClient() {
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
            _rzk.setData(SpliceConstants.zkSpliceMinimumActivePath, data, -1);
        } catch (Exception e) {
            LOG.error("Couldn't remember timestamp", e);
            throw new RuntimeException("Couldn't remember timestamp",e);
        }
    }

    @Override
    public long retrieveTimestamp() {
        byte[] data;
        try {
            data = _rzk.getData(SpliceConstants.zkSpliceMinimumActivePath, false, null);
        } catch (Exception e) {
            LOG.error("Couldn't retrieve minimum timestamp", e);
            return 0;
        }
        return Bytes.toLong(data);
    }
}
