package com.splicemachine.hbase.jmx;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.DynamicMBean;
import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import org.apache.hadoop.hbase.regionserver.metrics.RegionServerStatistics;

import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlers;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.job.TaskSchedulerManagement;

public class JMXUtils {	
	public static final String MONITORED_THREAD_POOL = "com.splicemachine.writer.async:type=ThreadPoolStatus";
	public static final String TASK_SCHEDULER_MANAGEMENT = "com.splicemachine.job:type=TaskSchedulerManagement";
	public static final String REGION_SERVER_STATISTICS = "hadoop:service=RegionServer,name=RegionServerStatistics";
	public static final String ACTIVE_WRITE_HANDLERS = "com.splicemachine.dery.hbase:type=ActiveWriteHandlers";
	
	public static List<MBeanServerConnection> getMBeanServerConnections(Collection<String> serverConnections) throws IOException {
		List<MBeanServerConnection> mbscArray = new ArrayList<MBeanServerConnection>(serverConnections.size());
		for (String serverName: serverConnections) {
			JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%s/jndi/rmi://%s:10102/jmxrmi",serverName,serverName));
		    JMXConnector jmxc = JMXConnectorFactory.connect(url, null); 		
		    mbscArray.add(jmxc.getMBeanServerConnection()); 
		}
		return mbscArray;
	}

	public static ObjectName getRegionServerStatistics() throws MalformedObjectNameException, NullPointerException {
		return getDynamicMBean(REGION_SERVER_STATISTICS);
	}
	
	public static List<ThreadPoolStatus> getMonitoredThreadPools(List<MBeanServerConnection> mbscArray) throws MalformedObjectNameException, NullPointerException {
		List<ThreadPoolStatus> monitoredThreadPools = new ArrayList<ThreadPoolStatus>();
		for (MBeanServerConnection mbsc: mbscArray) {
			monitoredThreadPools.add(getNewMBeanProxy(mbsc,MONITORED_THREAD_POOL,ThreadPoolStatus.class));
		}
		return monitoredThreadPools;
	}

	public static List<ActiveWriteHandlersIface> getActiveWriteHandlers(List<MBeanServerConnection> mbscArray) throws MalformedObjectNameException, NullPointerException {
		List<ActiveWriteHandlersIface> activeWrites = new ArrayList<ActiveWriteHandlersIface>();
		for (MBeanServerConnection mbsc: mbscArray) {
			activeWrites.add(getNewMBeanProxy(mbsc,ACTIVE_WRITE_HANDLERS,ActiveWriteHandlersIface.class));
		}
		return activeWrites;
	}

	public static List<TaskSchedulerManagement> getTaskSchedulerManagement(List<MBeanServerConnection> mbscArray) throws MalformedObjectNameException, NullPointerException {
		List<TaskSchedulerManagement> taskSchedules = new ArrayList<TaskSchedulerManagement>();
		for (MBeanServerConnection mbsc: mbscArray) {
			taskSchedules.add(getNewMBeanProxy(mbsc,TASK_SCHEDULER_MANAGEMENT,TaskSchedulerManagement.class));
		}
		return taskSchedules;
	}
	
	public static <T> T getNewMBeanProxy(MBeanServerConnection mbsc, String mbeanName, Class<T> type) throws MalformedObjectNameException, NullPointerException {
	    ObjectName objectName = new ObjectName(mbeanName);
	    return JMX.newMBeanProxy(mbsc, objectName,type, true);
	}

	public static ObjectName getDynamicMBean(String mbeanName) throws MalformedObjectNameException, NullPointerException {
	    return new ObjectName(mbeanName);
	}

}
