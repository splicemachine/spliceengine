/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.api.TimestampIOException;
import com.splicemachine.timestamp.api.TimestampOracleStatistics;
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

public class TimestampOracle implements TimestampOracleStatistics{
	private static final long TIMESTAMP_INCREMENT = 0x100l;

    private static final Logger LOG = Logger.getLogger(TimestampOracle.class);

	// Contains the next timestamp value to be returned to caller use
	private final AtomicLong _timestampCounter = new AtomicLong(0l);
	
	// Maximum timestamp that we can feed before reserving another block
	private volatile long _maxReservedTimestamp = -1l;

	// Singleton instance, used by TimestampServerHandler
	private static volatile TimestampOracle _instance;
	
	// Metrics to expose via JMX. See TimestampOracleStatistics
	// for solid definitions of each metric.
    private AtomicLong _numBlocksReserved = new AtomicLong(0);
    private AtomicLong _numTimestampsCreated = new AtomicLong(0);

    private TimestampBlockManager timestampBlockManager;
    private long blockSize;

    public static TimestampOracle getInstance(TimestampBlockManager timestampBlockManager, int blockSize) throws TimestampIOException{
		TimestampOracle to = _instance;
		if(to==null){
			synchronized(TimestampOracle.class){
				to = _instance;
				if(to==null){
					SpliceLogUtils.info(LOG,"Initializing TimestampOracle...");
					to=_instance=new TimestampOracle(timestampBlockManager,blockSize);
				}
			}
		}
		return to;
	}
	
	private TimestampOracle(TimestampBlockManager timestampBlockManager, int blockSize) throws TimestampIOException {
        this.timestampBlockManager=timestampBlockManager;
        this.blockSize = blockSize * TIMESTAMP_INCREMENT;
		initialize();
	}

    /**
     * Read the current state of the block from the timestampBlockManager
     *
     * @throws TimestampIOException
     */
	private void initialize() throws TimestampIOException {
			synchronized(this) {
                _maxReservedTimestamp = timestampBlockManager.initialize();
				_timestampCounter.set(_maxReservedTimestamp + TIMESTAMP_INCREMENT);
			}
			try {
				registerJMX();
			} catch (Exception e) {
                SpliceLogUtils.error(LOG, "Unable to register Timestamp Generator with JMX. Service will function but metrics will not be available.");
			}
    }

	public long getNextTimestamp(boolean refresh) throws TimestampIOException {
		if (refresh) {
            _maxReservedTimestamp = timestampBlockManager.initialize();
			_timestampCounter.set(_maxReservedTimestamp);
		}
		long nextTS = _timestampCounter.addAndGet(TIMESTAMP_INCREMENT);
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
            timestampBlockManager.reserveNextBlock(nextMax);
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
        ObjectName name = new ObjectName("com.splicemachine.si.client.timestamp.generator:type=TimestampOracleStatistics");  // Same string is in JMXUtils
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
