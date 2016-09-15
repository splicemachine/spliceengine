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
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.client.am.*;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.jdbc.ClientBaseDataSource;
import com.splicemachine.db.jdbc.ClientDataSource;
import com.splicemachine.db.shared.common.reference.Attribute;
import com.splicemachine.db.shared.common.reference.MessageId;

import java.sql.*;
import java.sql.Connection;
import java.util.*;
import java.util.logging.Logger;

/**
 * @author Scott Fines
 *         Date: 8/23/16
 */
public class ClusteredDriver implements Driver{
    private static final String FAILURE_TIMEOUT="failureTimeout";
    private static final long DEFAULT_FAILURE_TIMEOUT=10000;
    private static SQLException driverLoadExceptions=null;
    private static ClusteredDriver registeredDriver = null;

    static{
        registerDriver(new ClusteredDriver());
    }

    private static void registerDriver(ClusteredDriver driver){
        if(Configuration.exceptionsOnLoadResources !=null) {
            driverLoadExceptions =Utils.accumulateSQLException(
                    Configuration.exceptionsOnLoadResources.getSQLException(),
                    driverLoadExceptions);
        }
        try{
            DriverManager.registerDriver(driver);
            registeredDriver = driver;
        }catch(SQLException se){
            driverLoadExceptions = new SqlException(null,new ClientMessageId(SQLState.JDBC_DRIVER_REGISTER)).getSQLException();
            driverLoadExceptions.setNextException(se);
        }
    }

    @Override
    public Connection connect(String url,Properties info) throws SQLException{
        if(driverLoadExceptions!=null)
            throw driverLoadExceptions;

        if(info==null)
            info = new Properties();

        StringTokenizer urlTokenizer = new StringTokenizer(url,"/:=, \t\n\r\f",true);

        try{
            boolean protocol=tokenizeProtocol(url,urlTokenizer);
            if(!protocol) return null; //unrecognized database URL prefix

            try{
                urlTokenizer.nextToken(":/");
            }catch(NoSuchElementException e){
                throw new SqlException(null,new ClientMessageId(SQLState.MALFORMED_URL),url,e);
            }

            String[] serverList = tokenizeServers(urlTokenizer,url); // "/server1[:port],server2[:port]...

            String database = tokenizeDatabase(urlTokenizer,url);
            Properties augmentedProperties = tokenizeURLProperties(url,info);
            database = appendDatabaseAttributes(database,augmentedProperties);

            //TODO -sf- configure java.util.logging

            //TODO -sf- re-use shared DataSources
            PoolSizingStrategy pss = configurePoolSizing(augmentedProperties);
            ConnectionSelectionStrategy css = configureSelectionStrategy(augmentedProperties);
            final long heartbeat = getHeartbeat(url,augmentedProperties);
            long serverCheckPeriod = getServerCheckPeriod(url,augmentedProperties);

            String user =ClientBaseDataSource.getUser(augmentedProperties);
            String password = ClientBaseDataSource.getPassword(augmentedProperties);
            FailureDetectorFactory fdf = new DeadlineFailureDetectorFactory(getFailureWindow(url,heartbeat,augmentedProperties));
            ServerPoolFactory poolFactory = new ConfiguredServerPoolFactory(database,user,password,fdf,pss);

            ClusteredDataSource cds = ClusteredDataSource.newBuilder()
                    .servers(serverList)
                    .connectionSelection(css)
                    .serverPoolFactory(poolFactory)
                    .heartbeatPeriod(heartbeat)
                    .discoveryWindow(serverCheckPeriod)
                    .build();

            cds.detectServers(); //run a detection to fill out all the servers
            return new ClusteredConnection(cds,true,augmentedProperties);
        }catch(SqlException se){
            throw se.getSQLException();
        }
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException{
        try{
            return tokenizeProtocol(url,new StringTokenizer(url,"/:=; \t\n\r\f",true));
        }catch(SqlException e){
            throw e.getSQLException();
        }
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url,Properties info) throws SQLException{
        java.sql.DriverPropertyInfo driverPropertyInfo[] = new java.sql.DriverPropertyInfo[2];

        // If there are no properties set already,
        // then create a dummy properties just to make the calls go thru.
        if (info == null) {
            info = new java.util.Properties();
        }

        driverPropertyInfo[0] =
                new java.sql.DriverPropertyInfo(Attribute.USERNAME_ATTR,
                        info.getProperty(Attribute.USERNAME_ATTR, ClientDataSource.propertyDefault_user));

        driverPropertyInfo[1] =
                new java.sql.DriverPropertyInfo(Attribute.PASSWORD_ATTR,
                        info.getProperty(Attribute.PASSWORD_ATTR));

        driverPropertyInfo[0].description =
                SqlException.getMessageUtil().getTextMessage(
                        MessageId.CONN_USERNAME_DESCRIPTION);
        driverPropertyInfo[1].description =
                SqlException.getMessageUtil().getTextMessage(
                        MessageId.CONN_PASSWORD_DESCRIPTION);

        driverPropertyInfo[0].required = true;
        driverPropertyInfo[1].required = false; // depending on the security mechanism

        return driverPropertyInfo;
    }

    @Override
    public int getMajorVersion(){
        return Version.getMajorVersion();
    }

    @Override
    public int getMinorVersion(){
        return Version.getMinorVersion();
    }

    @Override
    public boolean jdbcCompliant(){
        return Configuration.jdbcCompliant;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException{
        throw new SQLFeatureNotSupportedException();
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private static boolean tokenizeProtocol(String url, java.util.StringTokenizer urlTokenizer) throws SqlException {
        // Is this condition necessary, StringTokenizer constructor may do this for us
        if (url == null) {
            return false;
        }

        if (urlTokenizer == null) {
            return false;
        }

        try{
            String jdbc=urlTokenizer.nextToken(":");
            if(!jdbc.equals("jdbc")){
                return false;
            }
            if(!urlTokenizer.nextToken(":").equals(":")){
                return false; // Skip over the first colon in jdbc:splice:
            }
            String dbname=urlTokenizer.nextToken(":");
            // For Derby AS need to check for // since jdbc:splice: is also the
            // embedded prefix
            if("splice".equals(dbname)||"spliceClustered".equals(dbname)){
                // Skip over the second colon in jdbc:splice:
                return urlTokenizer.nextToken(":").equals(":");
            }else return false;
        } catch (java.util.NoSuchElementException e) {
            return false;
        }
    }

    private String[] tokenizeServers(StringTokenizer urlTokenizer,String url) throws SqlException{
        try{
            if(!urlTokenizer.nextToken("/").equals("/")){
                throw new SqlException(null,
                        new ClientMessageId(SQLState.MALFORMED_URL),url);
            }
            String serverStr = urlTokenizer.nextToken("/"); //skip to the next String
            urlTokenizer.nextToken("/");
            StringTokenizer serverPortTokenizer = new StringTokenizer(serverStr,",");
            List<String> serverPortPairs = new LinkedList<>();
            while(serverPortTokenizer.hasMoreTokens()){
                serverPortPairs.add(serverPortTokenizer.nextToken(","));
            }

            return serverPortPairs.toArray(new String[serverPortPairs.size()]);

        }catch(NoSuchElementException e){
            throw new SqlException(null,
                    new ClientMessageId(SQLState.MALFORMED_URL),url);
        }
    }

    private static String tokenizeDatabase(java.util.StringTokenizer urlTokenizer,
                                           String url) throws SqlException {
        try {
            // DERBY-618 - database name can contain spaces in the path
            return urlTokenizer.nextToken("\t\n\r\f;");
        } catch (java.util.NoSuchElementException e) {
            // A null log writer is passed, because jdbc 1 sqlexceptions are automatically traced
            throw new SqlException(null, new ClientMessageId(SQLState.MALFORMED_URL), url, e);
        }
    }

    private static java.util.Properties tokenizeURLProperties(String url,
                                                              java.util.Properties properties)
            throws SqlException {
        String attributeString = null;
        int attributeIndex;

        if ((url != null) &&
                ((attributeIndex = url.indexOf(";")) != -1)) {
            attributeString = url.substring(attributeIndex);
        }
        return ClientDataSource.tokenizeAttributes(attributeString, properties);
    }

    /**
     * Append attributes to the database name except for user/password
     * which are sent as part of the protocol, and SSL which is used
     * locally in the client.
     * Other attributes will  be sent to the server with the database name
     * Assumes augmentedProperties is not null
     *
     * @param database            - Short database name
     * @param augmentedProperties - Set of properties to append as attributes
     * @return databaseName + attributes (e.g. mydb;create=true)
     */
    private String appendDatabaseAttributes(String database, Properties augmentedProperties) {

        StringBuilder longDatabase = new StringBuilder(database);
        for (Enumeration keys = augmentedProperties.propertyNames();
             keys.hasMoreElements(); ) {
            String key = (String) keys.nextElement();
            if (key.equals(Attribute.USERNAME_ATTR) ||
                    key.equals(Attribute.PASSWORD_ATTR) ||
                    key.equals(Attribute.SSL_ATTR) ||
                    key.equals(HEARTBEAT) ||
                    key.equals(SERVER_CHECK_PERIOD)||
                    key.equals(FAILURE_TIMEOUT))
                continue;
            longDatabase.append(";").append(key).append("=").append(augmentedProperties.getProperty(key));
        }
        return longDatabase.toString();
    }

    private PoolSizingStrategy configurePoolSizing(Properties augmentedProperties){
        //TODO -sf- make this bounded by default
        return InfinitePoolSize.INSTANCE;
    }

    private ConnectionSelectionStrategy configureSelectionStrategy(Properties augmentedProperties){
        //TODO -sf- deal with properties options
        return ConnectionStrategy.ROUND_ROBIN;
    }

    private long getFailureWindow(String url,long heartbeatPeriod,Properties augmentedProperties) throws SqlException{
        long failureTimeMillis = DEFAULT_HEARTBEAT_COUNT*heartbeatPeriod; //must respond within 10 second by default
        String failureTimeout = augmentedProperties.getProperty(FAILURE_TIMEOUT);
        if(failureTimeout!=null){
            try{
                failureTimeMillis = Long.parseLong(failureTimeout);
            }catch(NumberFormatException nfe){
                throw new SqlException(null,new ClientMessageId(SQLState.MALFORMED_URL),url,nfe);
            }
        }
        return failureTimeMillis;
    }

    private static final String HEARTBEAT = "heartbeat";
    private static final String SERVER_CHECK_PERIOD= "discoveryInterval";
    private static final long DEFAULT_SERVER_CHECK = 1000L;
    private static final long DEFAULT_HEARTBEAT = 1000L;
    private static final int DEFAULT_HEARTBEAT_COUNT = 10;

    private long getHeartbeat(String url,Properties augmentedProperties) throws SqlException{
        String heartbeatStr = augmentedProperties.getProperty(HEARTBEAT);
        if(heartbeatStr==null) return DEFAULT_HEARTBEAT;
        try{
            return Long.parseLong(heartbeatStr);
        }catch(NumberFormatException nfe){
            throw new SqlException(null,new ClientMessageId(SQLState.MALFORMED_URL),url);
        }
    }

    private long getServerCheckPeriod(String url,Properties augmentedProperties) throws SqlException{
        String serverCheck = augmentedProperties.getProperty(SERVER_CHECK_PERIOD);
        if(serverCheck==null) return DEFAULT_SERVER_CHECK;
        try{
            return Long.parseLong(serverCheck);
        }catch(NumberFormatException nfe){
            throw new SqlException(null,new ClientMessageId(SQLState.MALFORMED_URL),url);
        }
    }

    public static void main(String...args) throws Exception{
        String url = "jdbc:splice://server1:1527,server2,server3:4000/splicedb;user=splice;password=splice";

        new ClusteredDriver().connect(url,null);
    }

}
