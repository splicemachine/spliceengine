package com.splicemachine.hbase.jmx;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskSchedulerManagement;
import com.splicemachine.derby.impl.job.scheduler.TieredSchedulerManagement;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.utils.logging.Logging;
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
import org.apache.hadoop.hbase.util.Pair;

public class JMXUtils {
    public static final String LOGGING_MANAGEMENT = "com.splicemachine.utils.logging:type=LogManager";
    public static final String MONITORED_THREAD_POOL = "com.splicemachine.writer.async:type=ThreadPoolStatus";
    public static final String GLOBAL_TASK_SCHEDULER_MANAGEMENT = "com.splicemachine.job:type=TieredSchedulerManagement";
    public static final String TIER_TASK_SCHEDULER_MANAGEMENT_BASE = "com.splicemachine.job.tasks.tier-";
	public static final String STATEMENT_MANAGEMENT_BASE = "com.splicemachine.statement:type=StatementManagement";
    public static final String REGION_SERVER_STATISTICS = "hadoop:service=RegionServer,name=RegionServerStatistics";
    public static final String ACTIVE_WRITE_HANDLERS = "com.splicemachine.dery.hbase:type=ActiveWriteHandlers";
    public static final String JOB_SCHEDULER_MANAGEMENT = "com.splicemachine.job:type=JobSchedulerManagement";

	public static List<Pair<String,JMXConnector>> getMBeanServerConnections(Collection<String> serverConnections) throws IOException {
		List<Pair<String,JMXConnector>> mbscArray = new ArrayList<Pair<String,JMXConnector>>(serverConnections.size());
		for (String serverName: serverConnections) {
			JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%s/jndi/rmi://%s:10102/jmxrmi",serverName,serverName));
		    JMXConnector jmxc = JMXConnectorFactory.connect(url, null); 		
		    mbscArray.add(Pair.newPair(serverName,jmxc));
		}
		return mbscArray;
	}

    public static List<Logging> getLoggingManagement(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<Logging> activeWrites = new ArrayList<Logging>();
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            activeWrites.add(getNewMXBeanProxy(mbsc.getSecond(),LOGGING_MANAGEMENT,Logging.class));
        }
        return activeWrites;
    }

	public static ObjectName getRegionServerStatistics() throws MalformedObjectNameException {
		return getDynamicMBean(REGION_SERVER_STATISTICS);
	}
	
	public static List<ThreadPoolStatus> getMonitoredThreadPools(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
		List<ThreadPoolStatus> monitoredThreadPools = new ArrayList<ThreadPoolStatus>();
		for (Pair<String,JMXConnector> mbsc: mbscArray) {
			monitoredThreadPools.add(getNewMBeanProxy(mbsc.getSecond(),MONITORED_THREAD_POOL,ThreadPoolStatus.class));
		}
		return monitoredThreadPools;
	}

	public static List<ActiveWriteHandlersIface> getActiveWriteHandlers(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
		List<ActiveWriteHandlersIface> activeWrites = new ArrayList<ActiveWriteHandlersIface>();
		for (Pair<String,JMXConnector> mbsc: mbscArray) {
			activeWrites.add(getNewMBeanProxy(mbsc.getSecond(),ACTIVE_WRITE_HANDLERS,ActiveWriteHandlersIface.class));
		}
		return activeWrites;
	}

    public static List<Pair<String,JobSchedulerManagement>> getJobSchedulerManagement(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<Pair<String,JobSchedulerManagement>> jobMonitors = Lists.newArrayListWithCapacity(mbscArray.size());
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            jobMonitors.add(new Pair<String, JobSchedulerManagement>(mbsc.getFirst(),getNewMBeanProxy(mbsc.getSecond(), JOB_SCHEDULER_MANAGEMENT, JobSchedulerManagement.class)));
        }
        return jobMonitors;
    }

    public static List<TieredSchedulerManagement> getTaskSchedulerManagement(List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<TieredSchedulerManagement> taskSchedules = Lists.newArrayListWithCapacity(mbscArray.size());
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            taskSchedules.add(getNewMBeanProxy(mbsc.getSecond(), GLOBAL_TASK_SCHEDULER_MANAGEMENT,TieredSchedulerManagement.class));
        }
        return taskSchedules;
    }

    public static List<StealableTaskSchedulerManagement> getTieredSchedulerManagement(int tier,List<Pair<String,JMXConnector>> mbscArray) throws MalformedObjectNameException, IOException {
        List<StealableTaskSchedulerManagement> taskSchedules = Lists.newArrayListWithCapacity(mbscArray.size());
        String mbeanName = TIER_TASK_SCHEDULER_MANAGEMENT_BASE + tier + ":type=StealableTaskSchedulerManagement";
        for (Pair<String,JMXConnector> mbsc: mbscArray) {
            taskSchedules.add(getNewMBeanProxy(mbsc.getSecond(), mbeanName,StealableTaskSchedulerManagement.class));
        }
        return taskSchedules;
    }

		public static List<Pair<String,StatementManagement>> getStatementManagers(List<Pair<String, JMXConnector>> connections) throws IOException, MalformedObjectNameException {
				List<Pair<String,StatementManagement>> managers = Lists.newArrayListWithCapacity(connections.size());
				for(Pair<String,JMXConnector> connectorPair:connections){
						managers.add(Pair.newPair(connectorPair.getFirst(),getNewMXBeanProxy(connectorPair.getSecond(),STATEMENT_MANAGEMENT_BASE,StatementManagement.class)));
				}
				return managers;
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
