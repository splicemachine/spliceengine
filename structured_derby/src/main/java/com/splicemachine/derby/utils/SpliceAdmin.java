package com.splicemachine.derby.utils;

import com.splicemachine.derby.hbase.SpliceIndexEndpoint.ActiveWriteHandlersIface;
import com.splicemachine.derby.impl.job.scheduler.StealableTaskSchedulerManagement;
import com.splicemachine.derby.impl.job.scheduler.TieredSchedulerManagement;
import com.splicemachine.hbase.ThreadPoolStatus;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.job.JobSchedulerManagement;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanException;
import javax.management.MBeanServerConnection;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.ReflectionException;
import javax.management.remote.JMXConnector;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Pair;

/**
 * @author Jeff Cunningham
 *         Date: 12/9/13
 */
public class SpliceAdmin {
    /**
     *
     * @return
     * @throws SQLException
     */
    public static void SYSCS_GET_ACTIVE_SERVERS(ResultSet[] resultSet) throws SQLException {
        StringBuilder sb = new StringBuilder("select * from (values ");
        int i = 0;
        for (ServerName serverName : getServers()) {
            if (i!=0) {
                sb.append(", ");
            }
            sb.append(String.format("('%s',%d,%d)",
                serverName.getHostname(),
                serverName.getPort(),
                serverName.getStartcode()));
            i++;
        }
        sb.append(") foo (hostname, port, startcode)");
        resultSet[0] = executeStatement(sb);
    }

    public static void SYSCS_GET_WRITE_INTAKE_INFO(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<ActiveWriteHandlersIface> activeWriteHandler = null;
            try {
                activeWriteHandler = JMXUtils.getActiveWriteHandlers(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }  catch (IOException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (ActiveWriteHandlersIface activeWrite : activeWriteHandler) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d,%d,%d,%d)",servers.get(i).getHostname(),
                    activeWrite.getActiveWriteThreads(),
                    activeWrite.getCompactionQueueSizeLimit(),
                    activeWrite.getFlushQueueSizeLimit(),
                    activeWrite.getIpcReservedPool()));
                i++;
            }
            sb.append(") foo (hostname, activeWriteThreads, compactionQueueSizeLimit, flushQueueSizeLimit, ipcReserverdPool)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void SYSCS_SET_MAX_TASKS(int workerTier,int maxWorkers) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<StealableTaskSchedulerManagement> taskSchedulers = null;
            try {
                taskSchedulers = JMXUtils.getTieredSchedulerManagement(workerTier, connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            for (StealableTaskSchedulerManagement taskScheduler : taskSchedulers) {
                taskScheduler.setCurrentWorkers(maxWorkers);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void SYSCS_GET_JOB_IDS(ResultSet[] resultSet) throws SQLException{
        List<ServerName> servers = getServers();
        // <jobID-><statement,jobHost>>
        Map<String,List<Pair<String,String>>> jobMap = new HashMap<String, List<Pair<String,String>>>();
        // <jobID-><taskID,taskHost,status>>
        Map<String,List<Trip<String,String,String>>> taskMap = new HashMap<String, List<Trip<String,String,String>>>();

        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));

            List<Pair<String,JobSchedulerManagement>> jobMonitors;
            try {
                jobMonitors = JMXUtils.getJobSchedulerManagement(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }

            for (Pair<String,JobSchedulerManagement> taskMonitor : jobMonitors) {
                for (String job : taskMonitor.getSecond().getRunningJobs()) {
                    // [jobID,statement]
                    String[] jobComponents = job.split("\\"+JobSchedulerManagement.SEP_CHAR);
                    String jobID = jobComponents[0];
                    List<Pair<String,String>> jobVals = jobMap.get(jobID);
                    if (jobVals == null) {
                        jobVals = new ArrayList<Pair<String,String>>();
                    }
                    jobVals.add(new Pair<String, String>(jobComponents[1],taskMonitor.getFirst()));
                    jobMap.put(jobID,jobVals);
                }
                for (String task : taskMonitor.getSecond().getRunningTasks()) {
                    // [jobID,taskID,taskStatus]
                    String[] taskComponents = task.split("\\"+JobSchedulerManagement.SEP_CHAR);
                    String jobID = taskComponents[0];
                    List<Trip<String,String,String>> taskVals = taskMap.get(jobID);
                    if (taskVals == null) {
                        taskVals = new ArrayList<Trip<String,String,String>>();
                    }
                    taskVals.add(new Trip<String,String,String>(taskComponents[1],taskMonitor.getFirst(),taskComponents[2]));
                    taskMap.put(jobID,taskVals);
                }
            }

            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (Map.Entry<String, List<Pair<String, String>>> jobEntry : jobMap.entrySet()) {
                String jobID = jobEntry.getKey();
                for (Pair<String, String> statement : jobEntry.getValue()) {
                    String sql = statement.getFirst();
                    String jobHost = statement.getSecond();
                    String taskID = "unknownID";
                    String taskHost = "unknownHost";
                    String taskStatus = "unknownStatus";
                    List<Trip<String,String,String>> taskEntries = taskMap.get(jobID);
                    if (i != 0) {
                        sb.append(", ");
                    }
                    if (taskEntries != null && ! taskEntries.isEmpty()) {
                        // todo - does nothing if no task info
                        for (Trip<String,String,String> taskEntry : taskEntries) {
                            taskID = taskEntry.getFirst();
                            taskHost = taskEntry.getSecond();
                            taskStatus = taskEntry.getThird();
                            sb.append(String.format("('%s','%s','%s','%s','%s','%s')",
                                    sql,
                                    jobID,
                                    jobHost,
                                    taskID,
                                    taskHost,
                                    taskStatus));
                        }
                    } else {
                        sb.append(String.format("('%s','%s','%s','%s','%s','%s')",
                                sql,
                                jobID,
                                jobHost,
                                taskID,
                                taskHost,
                                taskStatus));
                    }
                    i++;
                }
            }
            sb.append(") foo (statement, jobid, jobhost, taskid, taskhost, status)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void SYSCS_GET_MAX_TASKS(int workerTier,ResultSet[] resultSet) throws SQLException{
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));

            List<StealableTaskSchedulerManagement> taskSchedulers;
            try {
                taskSchedulers = JMXUtils.getTieredSchedulerManagement(workerTier, connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (StealableTaskSchedulerManagement taskScheduler : taskSchedulers) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d)",
                    servers.get(i).getHostname(),
                    taskScheduler.getCurrentWorkers()));
                i++;
            }
            sb.append(") foo (hostname, maxTaskWorkers)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }

    }

    public static void SYSCS_GET_GLOBAL_MAX_TASKS(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));

            List<TieredSchedulerManagement> taskSchedulers = null;
            try {
                taskSchedulers = JMXUtils.getTaskSchedulerManagement(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (TieredSchedulerManagement taskScheduler : taskSchedulers) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d)",
                    servers.get(i).getHostname(),
                    taskScheduler.getTotalWorkerCount()));
                i++;
            }
            sb.append(") foo (hostname, maxTaskWorkers)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    
    public static void SYSCS_SET_WRITE_POOL(int writePool) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<ThreadPoolStatus> threadPools = null;
            try {
                threadPools = JMXUtils.getMonitoredThreadPools(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            for (ThreadPoolStatus threadPool : threadPools) {
                threadPool.setMaxThreadCount(writePool);
            }
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void SYSCS_GET_WRITE_POOL(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<ThreadPoolStatus> threadPools = null;
            try {
                threadPools = JMXUtils.getMonitoredThreadPools(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (ThreadPoolStatus threadPool : threadPools) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d)",
                    servers.get(i).getHostname(),
                    threadPool.getMaxThreadCount()));
                i++;
            }
            sb.append(") foo (hostname, maxTaskWorkers)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }


    public static void SYSCS_GET_WRITE_PIPELINE_INFO(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<ThreadPoolStatus> threadPools = null;
            try {
                threadPools = JMXUtils.getMonitoredThreadPools(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (ThreadPoolStatus threadPool : threadPools) {
                if (i != 0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d,%d,%d,%d,%d,%d)",
                    servers.get(i).getHostname(),
                    threadPool.getActiveThreadCount(),
                    threadPool.getMaxThreadCount(),
                    threadPool.getPendingTaskCount(),
                    threadPool.getTotalSuccessfulTasks(),
                    threadPool.getTotalFailedTasks(),
                    threadPool.getTotalRejectedTasks()));
                i++;
            }
            sb.append(") foo (hostname, activeThreadCount, maxThreadCount, pendingTaskCount, totalSuccessfulTasks, totalFailedTasks, totalRejectedTasks)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }
    
    public static void SYSCS_GET_REGION_SERVER_TASK_INFO(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            List<TieredSchedulerManagement> taskSchedulers = null;
            try {
                taskSchedulers = JMXUtils.getTaskSchedulerManagement(connections);
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (TieredSchedulerManagement taskSchedule : taskSchedulers) {
                if (i !=0) {
                    sb.append(", ");
                }
                sb.append(String.format("('%s',%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d,%d)",
                        servers.get(i).getHostname(),
                        taskSchedule.getTotalWorkerCount(),
                        taskSchedule.getPending(),
                        taskSchedule.getExecuting(),
                        taskSchedule.getTotalCancelledTasks(),
                        taskSchedule.getTotalCompletedTasks(),
                        taskSchedule.getTotalFailedTasks(),
                        taskSchedule.getTotalInvalidatedTasks(),
                        taskSchedule.getTotalSubmittedTasks(),
                        taskSchedule.getTotalShruggedTasks(),
                        taskSchedule.getTotalStolenTasks(),
                        taskSchedule.getMostLoadedTier(),
                        taskSchedule.getLeastLoadedTier()));
                i++;
            }
            sb.append(") foo (hostname, totalWorkers, pending, running, totalCancelled, "
                    + "totalCompleted, totalFailed, totalInvalidated, totalSubmitted,totalShrugged,totalStolen,mostLoadedTier,leastLoadedTier)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }
    
    public static void SYSCS_GET_REGION_SERVER_STATS_INFO(ResultSet[] resultSet) throws SQLException {
        List<ServerName> servers = getServers();
        List<Pair<String,JMXConnector>> connections = null;
        try {
            connections = JMXUtils.getMBeanServerConnections(getServerNames(servers));
            ObjectName regionServerStats = null;
            try {
                regionServerStats = JMXUtils.getRegionServerStatistics();
            } catch (MalformedObjectNameException e) {
                throw new SQLException(e);
            }
            StringBuilder sb = new StringBuilder("select * from (values ");
            int i = 0;
            for (Pair<String,JMXConnector> mxc : connections) {
                MBeanServerConnection mbsc = mxc.getSecond().getMBeanServerConnection();
                if (i !=0) {
                    sb.append(", ");
                }
                try {
                    sb.append(String.format("('%s','%d','%d','%d','%d','%d','%f','%d','%d')",
                        servers.get(i).getHostname(),
                        mbsc.getAttribute(regionServerStats,"regions"),
                        mbsc.getAttribute(regionServerStats,"fsReadLatencyAvgTime"),
                        mbsc.getAttribute(regionServerStats,"fsWriteLatencyAvgTime"),
                        mbsc.getAttribute(regionServerStats,"writeRequestsCount"),
                        mbsc.getAttribute(regionServerStats,"readRequestsCount"),
                        mbsc.getAttribute(regionServerStats,"requests"),
                        mbsc.getAttribute(regionServerStats,"compactionQueueSize"),
                        mbsc.getAttribute(regionServerStats,"flushQueueSize")));
                } catch (MBeanException e) {
                    throw new SQLException(e);
                } catch (AttributeNotFoundException e) {
                    throw new SQLException(e);
                } catch (InstanceNotFoundException e) {
                    throw new SQLException(e);
                } catch (ReflectionException e) {
                    throw new SQLException(e);
                } catch (IOException e) {
                    throw new SQLException(e);
                }
                i++;
            }
            sb.append(") foo (hostname, regions, fsReadLatencyAvgTime, fsWriteLatencyAvgTime, ");
            sb.append("writeRequestsCount, readRequestsCount, ");
            sb.append("requests, compactionQueueSize, flushQueueSize)");
            resultSet[0] = executeStatement(sb);
        } catch (IOException e) {
            throw new SQLException(e);
        } finally {
            if (connections != null) {
                for (Pair<String,JMXConnector> connector : connections) {
                    try {
                        connector.getSecond().close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
    }

    public static void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException {
        StringBuilder sb = new StringBuilder("select * from (values ");
        int i =0;
        for (Map.Entry<ServerName,HServerLoad> serverLoad : getLoad().entrySet()) {
            if ( i != 0) {
                sb.append(", ");
            }
            ServerName sn = serverLoad.getKey();
            sb.append(String.format("('%s',%d,%d)",
                sn.getHostname(),
                    sn.getPort(),
                serverLoad.getValue().getTotalNumberOfRequests()));
            i++;
        }
        sb.append(") foo (hostname, port, totalRequests)");
        resultSet[0] = executeStatement(sb);
    }

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(String schemaName) throws SQLException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // todo implement
            // do sys query for conglomerate for schema?
            // find all tables in schema
            // perform major compaction on each
            Collection<byte[]> tables = Arrays.asList(new byte[0]);
            for (byte[] table : tables) {
                try {
                    admin.majorCompact(table);
                } catch (IOException e) {
                    throw new SQLException(e);
                } catch (InterruptedException e) {
                    throw new SQLException(e);
                }
            }
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
            }
        }
    }

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(String schemaName, String tableName)
            throws SQLException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // sys query for conglomerate for table/schema
            long conglomID = getConglomid(getDefaultConn(), schemaName, tableName);
            // TODO impl
            byte[] table = new byte[0];
            if (table != null && table.length >0) {
                try {
                    admin.majorCompact(table);
                } catch (IOException e) {
                    throw new SQLException(e);
                } catch (InterruptedException e) {
                    throw new SQLException(e);
                }
            }
        } finally {
            if (admin != null) {
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
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
                    "select conglomeratenumber from sys.sysconglomerates c, sys.systables t, sys.sysschemas s " +
                            "where t.tableid = c.tableid and t.schemaid = s.schemaid and s.schemaname = ? and t.tablename = ?");
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
        if(id!=null) {
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    private static ResultSet executeStatement(StringBuilder sb) throws SQLException {
        ResultSet result = null;
        Connection connection = getDefaultConn();
        try {
            PreparedStatement ps = connection.prepareStatement(sb.toString());
            result = ps.executeQuery();
            connection.commit();
        } catch (SQLException e){
            connection.rollback();
            throw new SQLException(sb.toString(), e);
        } finally {
            connection.close();
        }
        return result;
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


    private static List<String> getServerNames(Collection<ServerName> serverInfo) {
        List<String> names = new ArrayList<String>(serverInfo.size());
        for (ServerName sname : serverInfo) {
            names.add(sname.getHostname());
        }
        return names;
    }

    private static Map<ServerName,HServerLoad> getLoad() throws SQLException {
        Map<ServerName,HServerLoad> serverLoadMap = new HashMap<ServerName, HServerLoad>();
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            for (ServerName serverName : getServers()) {
                try {
                    serverLoadMap.put(serverName, admin.getClusterStatus().getLoad(serverName));
                } catch (IOException e) {
                    throw new SQLException(e);
                }
            }
        } finally {
            if (admin !=null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
        }

        return serverLoadMap;
    }

    private static List<ServerName> getServers() throws SQLException {
        HBaseAdmin admin = null;
        List<ServerName> servers = null;
        try {
            admin = SpliceUtils.getAdmin();
            try {
                servers = new ArrayList<ServerName>(admin.getClusterStatus().getServers());
            } catch (IOException e) {
                throw new SQLException(e);
            }
        } finally {
            if (admin !=null)
                try {
                    admin.close();
                } catch (IOException e) {
                    // ignore
                }
        }
        return servers;
    }

    private static class Trip<T, U, V> {
        private final T first;
        private final U second;
        private final V third;

        public Trip(T first, U second, V third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }

        public T getFirst() {
            return first;
        }

        public U getSecond() {
            return second;
        }

        public V getThird() {
            return third;
        }
    }
}
