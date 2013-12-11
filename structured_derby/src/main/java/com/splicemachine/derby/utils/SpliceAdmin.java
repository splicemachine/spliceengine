package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlers;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.job.TaskSchedulerManagement;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import javax.management.AttributeNotFoundException;
import javax.management.DynamicMBean;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;

import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.regionserver.metrics.RegionServerStatistics;

/**
 * @author Jeff Cunningham
 *         Date: 12/9/13
 */
public class SpliceAdmin {	
    /**
     *
     * @return
     * @throws IOException
     * @throws SQLException 
     */
    public static void SYSCS_GET_ACTIVE_SERVERS(ResultSet[] resultSet) throws IOException, SQLException {
    	Connection connection = null;
    	try {
	    	connection = getDefaultConn();
	    	StringBuffer sb = new StringBuffer("select * from (values ");
	    	int i = 0;    	
	    	for (ServerName serverName : getServers()) {
	    		if (i!=0)
	    			sb.append(", ");
	        	sb.append(String.format("('%s','%s',%d,%d)",serverName.getHostname(),serverName.getServerName(),serverName.getPort(), serverName.getStartcode()));
	    		i++;
	    	}
	    	sb.append(") foo (hostname, servername, port, startcode)"); 
	    	PreparedStatement ps = connection.prepareStatement(sb.toString());
	    	resultSet[0] = ps.executeQuery();
    	} finally {
    		if (connection != null)	
    			connection.close();
    	}
    }

    public static List<String> getServerNames(Collection<ServerName> serverInfo) {
    	List<String> names = new ArrayList<String>(serverInfo.size());
    	for (ServerName sname : serverInfo) {
    		names.add(sname.getHostname());
    	}
    	return names;
    }
    
    public static void SYSCS_GET_WRITE_INTAKE_INFO(ResultSet[] resultSet) throws IOException, MalformedObjectNameException, NullPointerException, SQLException {
    	Connection connection = null;
    	try {    	
	    	List<ServerName> servers = getServers();
	    	List<MBeanServerConnection> connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
	    	List<ActiveWriteHandlersIface> activeWriteHandler = JMXUtils.getActiveWriteHandlers(connections);
	    	connection = getDefaultConn();
	    	StringBuffer sb = new StringBuffer("select * from (values ");
	    	int i = 0;    	
	    	for (ActiveWriteHandlersIface activeWrite : activeWriteHandler) {
	    		if (i!=0)
	    			sb.append(", ");
	        	sb.append(String.format("('%s',%d,%d,%d,%d)",servers.get(i).getHostname(),
	        			activeWrite.getActiveWriteThreads(),
	        			activeWrite.getCompactionQueueSizeLimit(),
	        			activeWrite.getFlushQueueSizeLimit(),
	        			activeWrite.getIpcReservedPool()));
	    		i++;
	    	}
	    	sb.append(") foo (hostname, activeWriteThreads, compactionQueueSizeLimit, flushQueueSizeLimit, "
	    			+ "ipcReserverdPool)"); 
	    	PreparedStatement ps = connection.prepareStatement(sb.toString());
	    	resultSet[0] = ps.executeQuery();
    	} finally {
    		if (connection != null)	
    			connection.close();
    	}
    }
    
    public static void SYSCS_GET_WRITE_PIPELINE_INFO(ResultSet[] resultSet) throws IOException, MalformedObjectNameException, NullPointerException, SQLException {
    	Connection connection = null;
    	try {    	
	    	List<ServerName> servers = getServers();
	    	List<MBeanServerConnection> connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
	    	List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
	    	connection = getDefaultConn();
	    	StringBuffer sb = new StringBuffer("select * from (values ");
	    	int i = 0;    	
	    	for (ThreadPoolStatus threadPool : threadPools) {
	    		if (i!=0)
	    			sb.append(", ");
	        	sb.append(String.format("('%s',%d,%d,%d,%d,%d,%d)",servers.get(i).getHostname(),threadPool.getActiveThreadCount(),threadPool.getMaxThreadCount(), threadPool.getPendingTaskCount(), threadPool.getTotalSuccessfulTasks(), threadPool.getTotalFailedTasks(), threadPool.getTotalRejectedTasks()));
	    		i++;
	    	}
	    	sb.append(") foo (hostname, activeThreadCount, maxThreadCount, pendingTaskCount, "
	    			+ "totalSuccessfulTasks, totalFailedTasks, totalRejectedTasks)"); 
	    	PreparedStatement ps = connection.prepareStatement(sb.toString());
	    	resultSet[0] = ps.executeQuery();
    	} finally {
    		if (connection != null)	
    			connection.close();
    	}
    }
    
    public static void SYSCS_GET_REGION_SERVER_TASK_INFO(ResultSet[] resultSet) throws IOException, SQLException, MalformedObjectNameException, NullPointerException {
    	Connection connection = null;
    	try {
	    	List<ServerName> servers = getServers();
	    	List<MBeanServerConnection> connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
	    	List<TaskSchedulerManagement> taskSchedulers = JMXUtils.getTaskSchedulerManagement(connections);
	    	connection = getDefaultConn();
	    	StringBuffer sb = new StringBuffer("select * from (values ");
	    	int i = 0;    	
	    	for (TaskSchedulerManagement taskSchedule : taskSchedulers) {
	    		if (i!=0)
	    			sb.append(", ");
	        	sb.append(String.format("('%s',%d,%d,%d,%d,%d,%d,%d,%d,%d)",servers.get(i).getHostname(),
	        			taskSchedule.getCurrentWorkers(), taskSchedule.getMaxWorkers(),
	        			taskSchedule.getNumPendingTasks(), taskSchedule.getNumRunningTasks(),
	        			taskSchedule.getTotalCancelledTasks(), taskSchedule.getTotalCompletedTasks(), 
	        			taskSchedule.getTotalFailedTasks(), taskSchedule.getTotalInvalidatedTasks(), 
	        			taskSchedule.getTotalSubmittedTasks()));
	    		i++;
	    	}
	    	sb.append(") foo (hostname, currentWorkers, maxWorkers, pending, running, totalCancelled, "
	    			+ "totalCompleted, totalFailed, totalInvalidated, totalSubmitted)"); 
	    	PreparedStatement ps = connection.prepareStatement(sb.toString());
	    	resultSet[0] = ps.executeQuery();
    	} finally {
    		if (connection != null)
    			connection.close();
    	}
    }
    
    public static void SYSCS_GET_REGION_SERVER_STATS_INFO(ResultSet[] resultSet) throws IOException, SQLException, MalformedObjectNameException, NullPointerException, AttributeNotFoundException, MBeanException, ReflectionException, InstanceNotFoundException {
    	Connection connection = null;
    	try {
	    	List<ServerName> servers = getServers();
	    	List<MBeanServerConnection> connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
	    	ObjectName regionServerStats = JMXUtils.getRegionServerStatistics();
	    	connection = getDefaultConn();
	    	StringBuffer sb = new StringBuffer("select * from (values ");
	    	int i = 0;    	
	    	for (MBeanServerConnection mbsc : connections) {
	    		if (i!=0)
	    			sb.append(", ");
	        		sb.append(String.format("('%s','%s','%s','%s','%s','%s','%s','%s','%s')",servers.get(i).getHostname(),
	        			mbsc.getAttribute(regionServerStats,"regions"),
	        			mbsc.getAttribute(regionServerStats,"fsReadLatencyAvgTime"),
	        			mbsc.getAttribute(regionServerStats,"fsWriteLatencyAvgTime"),
	        			mbsc.getAttribute(regionServerStats,"writeRequestsCount"),
	        			mbsc.getAttribute(regionServerStats,"readRequestsCount"),
	        			mbsc.getAttribute(regionServerStats,"requests"),
	        			mbsc.getAttribute(regionServerStats,"compactionQueueSize"),
	        			mbsc.getAttribute(regionServerStats,"flushQueueSize")));	        			
	        			i++;
	    	}
	    	sb.append(") foo (hostname, regions, fsReadLatencyAvgTime, fsWriteLatencyAvgTime, "
	    			+ "writeRequestsCount, readRequestsCount, "
	    			+ "requests, compactionQueueSize, flushQueueSize)"); 
	    	PreparedStatement ps = connection.prepareStatement(sb.toString());
	    	resultSet[0] = ps.executeQuery();
    	} finally {
    		if (connection != null)
    			connection.close();
    	}
    }


    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(String schemaName) throws IOException, InterruptedException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // todo
            // do sys query for conglomerate for schema?
            // find all tables in schema
            // perform major compaction on each
            Collection<byte[]> tables = null;
            for (byte[] table : tables) {
                admin.majorCompact(table);
            }
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(String schemaName, String tableName)
            throws IOException, InterruptedException, SQLException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // sys query for conglomerate for table/schema
            long conglomID = getConglomid(getDefaultConn(), schemaName, tableName);
            byte[] table = null;
            admin.majorCompact(table);
        } finally {
            if (admin != null) {
                admin.close();
            }
        }
    }

    public static void getActiveTasks() throws MasterNotRunningException, ZooKeeperConnectionException {
        HBaseAdmin admin = SpliceUtils.getAdmin();
        // todo
        // get JMX connection
        // exec JMX query
        // close connection
        // return ?
    }

    public static void getActiveSQLStatementsByNode() {
        // todo
    }

    public static long getConglomid(Connection conn, String schemaName, String tableName) throws SQLException {
        if (schemaName == null)
            schemaName = "APP";
        ResultSet rs = null;
        PreparedStatement s = null;
        try{
            s = conn.prepareStatement(
                    "select " +
                            "conglomeratenumber " +
                            "from " +
                            "sys.sysconglomerates c, " +
                            "sys.systables t, " +
                            "sys.sysschemas s " +
                            "where " +
                            "t.tableid = c.tableid and " +
                            "t.schemaid = s.schemaid and " +
                            "s.schemaname = ? and " +
                            "t.tablename = ?");
            s.setString(1,schemaName.toUpperCase());
            s.setString(2,tableName.toUpperCase());
            rs = s.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }else{
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            }
        }finally{
            if(rs!=null) rs.close();
            if(s!=null) s.close();
        }
    }

    public static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if(id!=null){
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }



    public static void sendSMTP(
            String toAddress,
            String fromAddress,
            String subject,
            String content,
            String transportProtocol,
            String smtpHost,
            int smtpPort)
            throws Exception {
        Properties profile = new Properties();
        profile.put("mail.transport.protocol", "smtp");
        profile.put("mail.smtp.host", "smtp@acme_widgets.com");
        profile.put("mail.smtp.port", "25");
        // TODO: java mail
//        InternetAddress from = new InternetAddress(fromAddress);
//        InternetAddress recipient = new InternetAddress(toAddress);
//        Session session = Session.getInstance(profile);
//
//        MimeMessage myMessage = new MimeMessage(session);
//        myMessage.setFrom(from);
//        myMessage.setSubject(subject);
//        myMessage.setText(content);
//        myMessage.addRecipient(Message.RecipientType.TO, recipient);
//        // Send the message
//        javax.mail.Transport.send(myMessage);
    }


    private static Map<ServerName,HServerLoad> getLoad() throws IOException {
        Map<ServerName,HServerLoad> serverLoadMap = new HashMap<ServerName, HServerLoad>();
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            for (ServerName serverName : getServers()) {
                serverLoadMap.put(serverName, admin.getClusterStatus().getLoad(serverName));
            }
        } finally {
            if (admin !=null)
                admin.close();
        }

        return serverLoadMap;
    }

    private static List<ServerName> getServers() throws IOException {
        HBaseAdmin admin = null;
        List<ServerName> servers = null;
        try {
            admin = SpliceUtils.getAdmin();
            servers = new ArrayList<ServerName>(admin.getClusterStatus().getServers());
        } finally {
            if (admin !=null)
                admin.close();
        }
        return servers;
    }

}
