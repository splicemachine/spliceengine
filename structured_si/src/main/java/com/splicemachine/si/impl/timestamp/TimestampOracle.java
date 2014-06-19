package com.splicemachine.si.impl.timestamp;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import com.splicemachine.constants.SpliceConstants;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

public class TimestampOracle {

    private static final Logger LOG = Logger.getLogger(TimestampOracle.class);

	private final RecoverableZooKeeper _zooKeeper;

	// Pointer to the specific znode instance that is specifically configured for timestamp block storage
	private final String _blockNode;
	
	private final int _blockSize = 10000; // TODO: configrable?

	// Contains the next timestamp value to be returned to caller use
	private final AtomicLong _timestampCounter = new AtomicLong(0l);
	
	// Maximum timestamp that we can feed before reserving another block
	private volatile long _maxReservedTimestamp = -1l;

	private static TimestampOracle _instance;
	
	public static final TimestampOracle getInstance(RecoverableZooKeeper rzk, String blockNode) {
		synchronized(TimestampOracle.class) {
			if (_instance == null) {
				TimestampUtil.doServerDebug(LOG, "initializing TimestampOracle...");
				_instance = new TimestampOracle(rzk, blockNode);
			}
			return _instance;
		}
	}
	
	private TimestampOracle(RecoverableZooKeeper rzk, String blockNode) {
		_zooKeeper = rzk;
		_blockNode = blockNode;
		initialize();
	}

	private void initialize() {

		// read the current state of the block
		try {
			synchronized(this) {
				byte[] data = _zooKeeper.getData(_blockNode, false, new Stat());
				long maxReservedTs = Bytes.toLong(data);
				doDebug("initialize: existing max reserved timestamp = " + maxReservedTs);
				
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
		            byte[] dataCounter = _zooKeeper.getData(counterTransactionPath, false, statCounter);
		            int version = statCounter.getVersion();
		            maxReservedTs = version | highBits;
					doDebug("Detected last timestamp from previous implementation: " + maxReservedTs);
				}

				_maxReservedTimestamp = maxReservedTs;
				_timestampCounter.set(_maxReservedTimestamp + 1);
			}
		} catch (KeeperException e) {
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	public long getNextTimestamp() throws IOException {
		long nextTS = _timestampCounter.getAndIncrement();
		long maxTS = _maxReservedTimestamp; // avoid the double volatile read
		if (nextTS > maxTS) {
			reserveNextBlock(maxTS);
		}
		// TODO: assert that this ts has never been used before (for debugging purposes)?
		return nextTS;
	}

	private void reserveNextBlock(long priorMaxReservedTimestamp) throws IOException {
		synchronized(this)  {
			if (_maxReservedTimestamp > priorMaxReservedTimestamp) return; // some other thread got there first

			long nextMax = _maxReservedTimestamp + _blockSize;
			byte[] data = Bytes.toBytes(nextMax);
			try {
				_zooKeeper.setData(_blockNode, data, -1 /* version */); // durably reserve the next block
				_maxReservedTimestamp = nextMax;
				doDebug("reserveNextBlock: next block reserved with max = " + _maxReservedTimestamp);
			} catch (KeeperException e) {
				throw new IOException(e);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	protected void doDebug(String s) {
    	TimestampUtil.doServerDebug(LOG, s);
    }
	
}
