package com.splicemachine.derby.utils;

import com.splicemachine.derby.iapi.sql.execute.MBeanResultSet;
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
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.hadoop.hbase.HServerLoad;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;

/**
 * @author Jeff Cunningham
 *         Date: 12/9/13
 */
public class SpliceAdmin {

    /**
     *
     * @return
     * @throws IOException
     */
    public static ResultSet SYSCS_GET_ACTIVE_SERVERS() throws IOException {
        List<String> colNames = Arrays.asList("hostname","servername", "port", "startcode");
        List<List<String>> rows = new ArrayList<List<String>>();
        for (ServerName serverName : getServers()) {
            List<String> row = new ArrayList<String>(colNames.size());
            row.add(serverName.getHostname());
            row.add(serverName.getServerName());
            row.add(Integer.toString(serverName.getPort()));
            row.add(Long.toString(serverName.getStartcode()));
            rows.add(row);
        }
       return new MBeanResultSet(rows, colNames);
    }

    public static ResultSet SYSCS_GET_REQUESTS() throws IOException {
        List<String> colNames = Arrays.asList("hostname","servername", "total requests");
        List<List<String>> rows = new ArrayList<List<String>>();
        for (Map.Entry<ServerName,HServerLoad> serverLoad : getLoad().entrySet()) {
            List<String> row = new ArrayList<String>(colNames.size());
            row.add(serverLoad.getKey().getHostname());
            row.add(serverLoad.getKey().getServerName());
            row.add(Integer.toString(serverLoad.getValue().getTotalNumberOfRequests()));
            rows.add(row);
        }
        return new MBeanResultSet(rows, colNames);
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
		/*
		 * gets the conglomerate id for the specified human-readable table name
		 *
		 * TODO -sf- make this a stored procedure?
		 */
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

    private static Collection<ServerName> getServers() throws IOException {
        HBaseAdmin admin = null;
        Collection<ServerName> servers = new ArrayList<ServerName>();
        try {
            admin = SpliceUtils.getAdmin();
            servers = admin.getClusterStatus().getServers();
        } finally {
            if (admin !=null)
                admin.close();
        }
        return servers;
    }

}
