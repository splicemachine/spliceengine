package com.splicemachine.hbase.jmx;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.management.JMX;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;

import com.splicemachine.hbase.MonitoredThreadPool;
import com.splicemachine.hbase.ThreadPoolStatus;

public class JMXUtils {	
	public static final String MONITORED_THREAD_POOL = "com.splicemachine.writer.async:type=ThreadPoolStatus";

	
	public static List<MBeanServerConnection> getMBeanServerConnections(Collection<String> serverConnections) throws IOException {
		List<MBeanServerConnection> mbscArray = new ArrayList<MBeanServerConnection>(serverConnections.size());
		for (String serverName: serverConnections) {
			JMXServiceURL url = new JMXServiceURL(String.format("service:jmx:rmi://%s/jndi/rmi://%s:10102/jmxrmi",serverName,serverName));
		    JMXConnector jmxc = JMXConnectorFactory.connect(url, null); 		
		    mbscArray.add(jmxc.getMBeanServerConnection()); 
		}
		return mbscArray;
	}
	
	public static List<ThreadPoolStatus> getMonitoredThreadPools(List<MBeanServerConnection> mbscArray) throws MalformedObjectNameException, NullPointerException {
		List<ThreadPoolStatus> monitoredThreadPools = new ArrayList<ThreadPoolStatus>();
		for (MBeanServerConnection mbsc: mbscArray) {
			monitoredThreadPools.add(getNewMBeanProxy(mbsc,MONITORED_THREAD_POOL,ThreadPoolStatus.class));
		}
		return monitoredThreadPools;
	}
	
	public static ThreadPoolStatus getMonitoredThreadPool(MBeanServerConnection mbsc) throws MalformedObjectNameException, NullPointerException {
		return getNewMBeanProxy(mbsc,MONITORED_THREAD_POOL,ThreadPoolStatus.class);
	}
	
	public static <T> T getNewMBeanProxy(MBeanServerConnection mbsc, String mbeanName, Class<T> type) throws MalformedObjectNameException, NullPointerException {
	    ObjectName objectName = new ObjectName(mbeanName);
	    return JMX.newMBeanProxy(mbsc, objectName,type, true);
	}
		
}
