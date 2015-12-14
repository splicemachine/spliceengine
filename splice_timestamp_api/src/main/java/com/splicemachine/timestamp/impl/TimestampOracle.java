package com.splicemachine.timestamp.impl;

import com.splicemachine.timestamp.api.TimestampDataSource;
import com.splicemachine.timestamp.api.TimestampMasterManagement;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import javax.management.ObjectName;

public class TimestampOracle implements TimestampMasterManagement {

    private static final Logger LOG = Logger.getLogger(TimestampOracle.class);

	// Contains the next timestamp value to be returned to caller use
	private final AtomicLong _timestampCounter = new AtomicLong(0l);
	
	// Maximum timestamp that we can feed before reserving another block
	private volatile long _maxReservedTimestamp = -1l;

	// Singleton instance, used by TimestampServerHandler
	private static TimestampOracle _instance;
	
	// Metrics to expose via JMX. See TimestampMasterManagement
	// for solid definitions of each metric.
    private AtomicLong _numBlocksReserved = new AtomicLong(0);
    private AtomicLong _numTimestampsCreated = new AtomicLong(0);

    private TimestampDataSource timestampDataSource;
    private int blockSize;

    public static final TimestampOracle getInstance(TimestampDataSource timestampDataSource, int blockSize)
	    throws TimestampIOException {
		synchronized(TimestampOracle.class) {
			if (_instance == null) {
                SpliceLogUtils.info(LOG, "Initializing TimestampOracle...");
				_instance = new TimestampOracle(timestampDataSource,blockSize);
			}
			return _instance;
		}
	}
	
	private TimestampOracle(TimestampDataSource timestampDataSource, int blockSize) throws TimestampIOException {
        this.timestampDataSource = timestampDataSource;
        this.blockSize = blockSize;
		initialize();
	}

    /**
     * Read the current state of the block from the timestampDataSource
     *
     * @throws TimestampIOException
     */
	private void initialize() throws TimestampIOException {
			synchronized(this) {
                _maxReservedTimestamp = timestampDataSource.initialize();
				_timestampCounter.set(_maxReservedTimestamp + 1);
			}
			try {
				registerJMX();
			} catch (Exception e) {
                SpliceLogUtils.error(LOG, "Unable to register Timestamp Generator with JMX. Service will function but metrics will not be available.");
			}
    }

	public long getNextTimestamp() throws TimestampIOException {
		long nextTS = _timestampCounter.getAndIncrement();
		long maxTS = _maxReservedTimestamp; // avoid the double volatile read
		if (nextTS > maxTS) {
			reserveNextBlock(maxTS);
		}
		_numTimestampsCreated.incrementAndGet(); // JMX metric
		return nextTS;
	}

    private void reserveNextBlock(long priorMaxReservedTimestamp) throws TimestampIOException {
        synchronized(this)  {
            if (_maxReservedTimestamp > priorMaxReservedTimestamp) return; // some other thread got there first
            long nextMax = _maxReservedTimestamp + blockSize;
            timestampDataSource.reserveNextBlock(nextMax);
            _maxReservedTimestamp = nextMax;
            _numBlocksReserved.incrementAndGet(); // JMX metric
            SpliceLogUtils.debug(LOG, "Next timestamp block reserved with max = %s", _maxReservedTimestamp);
        }
	}

	private void registerJMX() throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
        registerJMX(mbs);
        SpliceLogUtils.info(LOG, "Timestamp Generator on master successfully registered with JMX");
	}

    private void registerJMX(MBeanServer mbs) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
        ObjectName name = new ObjectName("com.splicemachine.si.impl.timestamp.generator:type=TimestampMasterManagement");  // Same string is in JMXUtils
        mbs.registerMBean(this, name);
    }

	@Override
	public long getNumberTimestampsCreated() {
		return _numTimestampsCreated.get();
	}

	@Override
	public long getNumberBlocksReserved() {
		return _numBlocksReserved.get();
	}
}
