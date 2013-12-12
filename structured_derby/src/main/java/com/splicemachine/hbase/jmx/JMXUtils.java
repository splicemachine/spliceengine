package com.splicemachine.hbase.jmx;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskScheduler;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskSchedulerManagement;
import com.splicemachine.derby.impl.job.scheduler.TieredSchedulerManagement;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.job.TaskSchedulerManagement;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import javax.management.JMX;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

public class JMXUtils {
		public static final String MONITORED_THREAD_POOL = "com.splicemachine.writer.async:type=ThreadPoolStatus";
		public static final String GLOBAL_TASK_SCHEDULER_MANAGEMENT = "com.splicemachine.job:type=TieredSchedulerManagement";
		public static final String TIER_TASK_SCHEDULER_MANAGEMENT_BASE = "com.splicemachine.job.tasks.tier-";
		public static final String REGION_SERVER_STATISTICS = "hadoop:service=RegionServer,name=RegionServerStatistics";
	public static final String ACTIVE_WRITE_HANDLERS = "com.splicemachine.dery.hbase:type=ActiveWriteHandlers";
	
	public static List<JMXConnector> getMBeanServerConnections(Collection<String> serverConnections) throws IOException {
		List<JMXConnector> mbscArray = new ArrayList<JMXConnector>(serverConnections.size());
		for (String serverName: serverConnections) {
			JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%s/jndi/rmi://%s:10102/jmxrmi",serverName,serverName));
		    JMXConnector jmxc = JMXConnectorFactory.connect(url, null); 		
		    mbscArray.add(jmxc);
		}
		return mbscArray;
	}

	public static ObjectName getRegionServerStatistics() throws MalformedObjectNameException {
		return getDynamicMBean(REGION_SERVER_STATISTICS);
	}
	
	public static List<ThreadPoolStatus> getMonitoredThreadPools(List<JMXConnector> mbscArray) throws MalformedObjectNameException, IOException {
		List<ThreadPoolStatus> monitoredThreadPools = new ArrayList<ThreadPoolStatus>();
		for (JMXConnector mbsc: mbscArray) {
			monitoredThreadPools.add(getNewMBeanProxy(mbsc,MONITORED_THREAD_POOL,ThreadPoolStatus.class));
		}
		return monitoredThreadPools;
	}

	public static List<ActiveWriteHandlersIface> getActiveWriteHandlers(List<JMXConnector> mbscArray) throws MalformedObjectNameException, IOException {
		List<ActiveWriteHandlersIface> activeWrites = new ArrayList<ActiveWriteHandlersIface>();
		for (JMXConnector mbsc: mbscArray) {
			activeWrites.add(getNewMBeanProxy(mbsc,ACTIVE_WRITE_HANDLERS,ActiveWriteHandlersIface.class));
		}
		return activeWrites;
	}

	public static List<TieredSchedulerManagement> getTaskSchedulerManagement(List<JMXConnector> mbscArray) throws MalformedObjectNameException, IOException {
		List<TieredSchedulerManagement> taskSchedules = Lists.newArrayListWithCapacity(mbscArray.size());
		for (JMXConnector mbsc: mbscArray) {
			taskSchedules.add(getNewMBeanProxy(mbsc, GLOBAL_TASK_SCHEDULER_MANAGEMENT,TieredSchedulerManagement.class));
		}
		return taskSchedules;
	}

		public static List<StealableTaskSchedulerManagement> getTieredSchedulerManagement(int tier,List<JMXConnector> mbscArray) throws MalformedObjectNameException, IOException {
				List<StealableTaskSchedulerManagement> taskSchedules = Lists.newArrayListWithCapacity(mbscArray.size());
				String mbeanName = TIER_TASK_SCHEDULER_MANAGEMENT_BASE + tier + ":type=StealableTaskSchedulerManagement";
				for (JMXConnector mbsc: mbscArray) {
						taskSchedules.add(getNewMBeanProxy(mbsc, mbeanName,StealableTaskSchedulerManagement.class));
				}
				return taskSchedules;
		}

	public static <T> T getNewMBeanProxy(JMXConnector mbsc, String mbeanName, Class<T> type) throws MalformedObjectNameException, IOException {
	    ObjectName objectName = new ObjectName(mbeanName);
	    return JMX.newMBeanProxy(mbsc.getMBeanServerConnection(), objectName,type, true);
	}

	public static ObjectName getDynamicMBean(String mbeanName) throws MalformedObjectNameException {
	    return new ObjectName(mbeanName);
	}

}
