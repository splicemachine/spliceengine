/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
    private int blockSize;

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
        this.blockSize = blockSize;
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
				_timestampCounter.set(_maxReservedTimestamp + 0x100);
			}
			try {
				registerJMX();
			} catch (Exception e) {
                SpliceLogUtils.error(LOG, "Unable to register Timestamp Generator with JMX. Service will function but metrics will not be available.");
			}
    }

	public long getNextTimestamp() throws TimestampIOException {
		long nextTS = _timestampCounter.addAndGet(0x100);
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
