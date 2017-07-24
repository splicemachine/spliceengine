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

package com.splicemachine.hbase.jmx;

import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.spark_project.guava.collect.Lists;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.derby.utils.DatabasePropertyManagement;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.threadpool.ThreadPoolStatus;
import com.splicemachine.timestamp.api.TimestampClientStatistics;
import com.splicemachine.timestamp.api.TimestampOracleStatistics;
import com.splicemachine.utils.Pair;
import com.splicemachine.utils.logging.Logging;

public class JMXUtils {


	public static final String LOGGING_MANAGEMENT = "com.splicemachine.utils.logging:type=LogManager";
    public static final String MONITORED_THREAD_POOL = "com.splicemachine.writer.async:type=ThreadPoolStatus";
	public static final String STATEMENT_MANAGEMENT_BASE = "com.splicemachine.statement:type=StatementManagement";
    public static final String ACTIVE_WRITE_HANDLERS = "com.splicemachine.derby.hbase:type=ActiveWriteHandlers";
    public static final String SPLICEMACHINE_VERSION = "com.splicemachine.version:type=DatabaseVersion";
    public static final String TIMESTAMP_MASTER_MANAGEMENT = "com.splicemachine.si.client.timestamp.generator:type=TimestampMasterManagement";
    public static final String TIMESTAMP_REGION_MANAGEMENT = "com.splicemachine.si.client.timestamp.request:type=TimestampRegionManagement";
	public static final String DATABASE_PROPERTY_MANAGEMENT = "com.splicemachine.derby.utils:type=DatabasePropertyManagement";

    public static List<Pair<String,JMXConnector>> getMBeanServerConnections(Collection<Pair<String,String>> serverConnections) throws IOException {
        List<Pair<String,JMXConnector>> mbscArray =new ArrayList<>(serverConnections.size());
        int regionServerJMXPort = EngineDriver.driver().getConfiguration().getPartitionserverJmxPort();
        for (Pair<String,String> serverConn: serverConnections) {
            JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%1$s:%2$d/jndi/rmi://%1$s:%2$d/jmxrmi",serverConn.getFirst(),regionServerJMXPort));
            JMXConnector jmxc = JMXConnectorFactory.connect(url, null);
            mbscArray.add(Pair.newPair(serverConn.getSecond(),jmxc));
        }
        return mbscArray;
    }

    public static List<Logging> getLoggingManagement(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<Logging> activeWrites =new ArrayList<>();
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            activeWrites.add(getNewMXBeanProxy(mbsc.getSecond(),LOGGING_MANAGEMENT,Logging.class));
        }
        return activeWrites;
    }

    //TODO -sf- this won't work in MapR
    protected static final String REGION_SERVER_STATISTICS = "Hadoop:service=HBase,name=RegionServer,sub=Server";
	public static ObjectName getRegionServerStatistics() throws MalformedObjectNameException {
        return JMXUtils.getDynamicMBean(REGION_SERVER_STATISTICS);
	}
	
	public static List<ThreadPoolStatus> getMonitoredThreadPools(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
		List<ThreadPoolStatus> monitoredThreadPools =new ArrayList<>();
		for (Pair<String,JMXConnector> mbsc: mbscArray) {
			monitoredThreadPools.add(getNewMBeanProxy(mbsc.getSecond(),MONITORED_THREAD_POOL,ThreadPoolStatus.class));
		}
		return monitoredThreadPools;
	}

    public static List<PipelineDriver.ActiveWriteHandlersIface> getActiveWriteHandlers(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<PipelineDriver.ActiveWriteHandlersIface> activeWrites =new ArrayList<>();
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            activeWrites.add(getNewMBeanProxy(mbsc.getSecond(),ACTIVE_WRITE_HANDLERS,PipelineDriver.ActiveWriteHandlersIface.class));
        }
        return activeWrites;
    }

    public static List<DatabaseVersion> getSpliceMachineVersion(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<DatabaseVersion> versions =new ArrayList<>();
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            versions.add(getNewMBeanProxy(mbsc.getSecond(), SPLICEMACHINE_VERSION, DatabaseVersion.class));
        }
        return versions;
    }

	public static List<Pair<String,StatementManagement>> getStatementManagers(List<Pair<String, JMXConnector>> connections) throws IOException, MalformedObjectNameException {
		List<Pair<String,StatementManagement>> managers = Lists.newArrayListWithCapacity(connections.size());
		for(Pair<String,JMXConnector> connectorPair:connections){
				managers.add(Pair.newPair(connectorPair.getFirst(),getNewMXBeanProxy(connectorPair.getSecond(),STATEMENT_MANAGEMENT_BASE,StatementManagement.class)));
		}
		return managers;
	}

    public static List<TimestampOracleStatistics> getTimestampOracleStatistics(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException {
        List<TimestampOracleStatistics> managers = new ArrayList<>();
        for (Pair<String,JMXConnector> connection : connections) {
            managers.add(getNewMXBeanProxy(connection.getSecond(), TIMESTAMP_MASTER_MANAGEMENT, TimestampOracleStatistics.class));
        }
        return managers;
    }

    public static List<Pair<String,TimestampClientStatistics>> getTimestampClientStatistics(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException {
        List<Pair<String, TimestampClientStatistics>> managers = Lists.newArrayListWithCapacity(connections.size());
        for (Pair<String,JMXConnector> connectorPair : connections) {
            managers.add(Pair.newPair(connectorPair.getFirst(), getNewMXBeanProxy(connectorPair.getSecond(), TIMESTAMP_REGION_MANAGEMENT, TimestampClientStatistics.class)));
        }
        return managers;
    }

    public static List<DatabasePropertyManagement> getDatabasePropertyManagement(List<Pair<String, JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<DatabasePropertyManagement> dbProps =new ArrayList<>();
        for (Pair<String, JMXConnector> mbsc : mbscArray) {
        	dbProps.add(getNewMXBeanProxy(mbsc.getSecond(), DATABASE_PROPERTY_MANAGEMENT, DatabasePropertyManagement.class));
        }
        return dbProps;
    }

	public static <T> T getNewMBeanProxy(JMXConnector mbsc, String mbeanName, Class<T> type) throws MalformedObjectNameException, IOException {
		ObjectName objectName = new ObjectName(mbeanName);
		return JMX.newMBeanProxy(mbsc.getMBeanServerConnection(), objectName,type, true);
	}

	public static <T> T getNewMXBeanProxy(JMXConnector mbsc, String mbeanName, Class<T> type) throws MalformedObjectNameException, IOException {
		ObjectName objectName = new ObjectName(mbeanName);
		return JMX.newMXBeanProxy(mbsc.getMBeanServerConnection(), objectName,type, true);
	}

	public static ObjectName getDynamicMBean(String mbeanName) throws MalformedObjectNameException {
	    return new ObjectName(mbeanName);
	}

}
