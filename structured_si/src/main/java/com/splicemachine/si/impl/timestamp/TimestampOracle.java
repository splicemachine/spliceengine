package com.splicemachine.si.impl.timestamp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.splicemachine.constants.SpliceConstants;

import java.util.concurrent.atomic.AtomicLong;

public class TimestampOracle {

    private static final Logger LOG = Logger.getLogger(TimestampOracle.class);

	private final RecoverableZooKeeper _zooKeeper;

	// Pointer to the specific znode instance that is specifically configured for timestamp block storage
	private final String _blockNode;
	
	private final int _blockSize = SpliceConstants.timestampBlockSize;

	// Contains the next timestamp value to be returned to caller use
	private final AtomicLong _timestampCounter = new AtomicLong(0l);
	
	// Maximum timestamp that we can feed before reserving another block
	private volatile long _maxReservedTimestamp = -1l;

	// Singleton instance, used by TimestampServerHandler
	private static TimestampOracle _instance;
	
	public static final TimestampOracle getInstance(RecoverableZooKeeper rzk, String blockNode)
	    throws TimestampIOException {
		synchronized(TimestampOracle.class) {
			if (_instance == null) {
				TimestampUtil.doServerInfo(LOG, "Initializing TimestampOracle...");
				_instance = new TimestampOracle(rzk, blockNode);
			}
			return _instance;
		}
	}
	
	private TimestampOracle(RecoverableZooKeeper rzk, String blockNode) throws TimestampIOException {
		_zooKeeper = rzk;
		_blockNode = blockNode;
		initialize();
	}

	private void initialize() throws TimestampIOException {

		// read the current state of the block
		try {
			synchronized(this) {
				byte[] data = _zooKeeper.getData(_blockNode, false, new Stat());
				long maxReservedTs = Bytes.toLong(data);
				TimestampUtil.doServerInfo(LOG, "Initializing: existing max reserved timestamp = %s", maxReservedTs);
				
				// If no previous maximum reserved timestamp found, then assume this is first time
				// new implementation (TimestampOracle) is being used. Fetch previous maximum
				// using deprecated zookeeper based mechanism (ZooKeeperStatTimestampSource).
				// This allows us to seamlessly pick up where we left off without some sort
				// 'migration' step.
				
				if (maxReservedTs == 0) {
					Stat statHigh = new Stat();
					byte[] dataOrig = _zooKeeper.getData(SpliceConstants.zkSpliceTransactionPath, false, statHigh);
		            String counterTransactionPath = Bytes.toString(dataOrig);
		            long highBits = (long)(statHigh.getVersion() - 1) << 32;

		            Stat statCounter = new Stat();
		            // We don't care about dataCounter. We're really after statCounter.
		            /* byte[] dataCounter = */ _zooKeeper.getData(counterTransactionPath, false, statCounter);
		            int version = statCounter.getVersion();
		            maxReservedTs = version | highBits;
					TimestampUtil.doServerInfo(LOG, "Initializing: detected last timestamp from prior TimestampSource implementation: %s", maxReservedTs);
				}

				_maxReservedTimestamp = maxReservedTs;
				_timestampCounter.set(_maxReservedTimestamp + 1);
			}
		} catch (KeeperException e) {
			throw new TimestampIOException(e);
		} catch (InterruptedException e) {
			throw new TimestampIOException(e);
		}
	}

	public long getNextTimestamp() throws TimestampIOException {
		long nextTS = _timestampCounter.getAndIncrement();
		long maxTS = _maxReservedTimestamp; // avoid the double volatile read
		if (nextTS > maxTS) {
			reserveNextBlock(maxTS);
		}
		return nextTS;
	}

	private void reserveNextBlock(long priorMaxReservedTimestamp) throws TimestampIOException {
		synchronized(this)  {
			if (_maxReservedTimestamp > priorMaxReservedTimestamp) return; // some other thread got there first
			long nextMax = _maxReservedTimestamp + _blockSize;
			byte[] data = Bytes.toBytes(nextMax);
			try {
				_zooKeeper.setData(_blockNode, data, -1 /* version */); // durably reserve the next block
				_maxReservedTimestamp = nextMax;
				TimestampUtil.doServerDebug(LOG, "Next timestamp block reserved wich max = %s", _maxReservedTimestamp);
			} catch (KeeperException e) {
				throw new TimestampIOException(e);
			} catch (InterruptedException e) {
				throw new TimestampIOException(e);
			}
		}
	}
}
