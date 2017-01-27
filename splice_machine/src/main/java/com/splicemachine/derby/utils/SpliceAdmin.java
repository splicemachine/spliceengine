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

package com.splicemachine.derby.utils;

import com.splicemachine.db.catalog.SystemProcedures;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.ddl.DDLUtils;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.protobuf.ProtoUtil;
import org.spark_project.guava.collect.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.DatabaseVersion;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.impl.sql.catalog.DataDictionaryImpl;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.jdbc.ResultSetBuilder;
import com.splicemachine.db.impl.jdbc.ResultSetBuilder.RowBuilder;
import com.splicemachine.db.impl.sql.GenericActivationHolder;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.GenericPreparedStatement;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.PartitionServer;
import com.splicemachine.storage.PartitionServerLoad;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.Map.Entry;

/**
 * @author Jeff Cunningham
 *         <p/>
 *         Date: 12/9/13
 */
@SuppressWarnings("unused")
public class SpliceAdmin extends BaseAdminProcedures{
    private static Logger LOG=Logger.getLogger(SpliceAdmin.class);

    public static void SYSCS_SET_LOGGER_LEVEL(final String loggerName,final String logLevel) throws SQLException{
        EngineDriver.driver().dbAdministrator().setLoggerLevel(loggerName,logLevel);
    }

    public static void SYSCS_GET_LOGGER_LEVEL(final String loggerName,final ResultSet[] resultSet) throws SQLException{
        List<String> levels = EngineDriver.driver().dbAdministrator().getLoggerLevel(loggerName);

        StringBuilder sb=new StringBuilder("select * from (values ");
        int i = 0;
        for(String level  : levels){
            sb.append(String.format("('%s')",level));
            if (i++ != levels.size()-1) {
                sb.append(", ");
            }
        }
        sb.append(") foo (logLevel)");
        resultSet[0]=executeStatement(sb);
    }

    public static void SYSCS_GET_LOGGERS(final ResultSet[] resultSet) throws SQLException{
        Set<String> loggers = EngineDriver.driver().dbAdministrator().getLoggers();
        StringBuilder sb=new StringBuilder("select * from (values ");
        List<String> loggerNames=new ArrayList<>(loggers);
        Collections.sort(loggerNames);
        for(String logger : loggerNames){
            sb.append(String.format("('%s')",logger));
            sb.append(", ");
        }
        if(sb.charAt(sb.length()-2)==','){
            sb.setLength(sb.length()-2);
        }
        sb.append(") foo (spliceLogger)");
        resultSet[0]=executeStatement(sb);
    }

    public static void SYSCS_GET_ACTIVE_SERVERS(ResultSet[] resultSet) throws SQLException{
        StringBuilder sb=new StringBuilder("select * from (values ");
        int i=0;
        for(PartitionServer serverName : getLoad()){
            if(i!=0){
                sb.append(", ");
            }
            sb.append(String.format("('%s',%d,%d)",
                    serverName.getHostname(),
                    serverName.getPort(),
                    serverName.getStartupTimestamp()));
            i++;
        }
        sb.append(") foo (hostname, port, startcode)");
        resultSet[0]=executeStatement(sb);
    }

    public static void SYSCS_GET_VERSION_INFO(final ResultSet[] resultSet) throws SQLException{
        Map<String,Map<String,String>> versionInfo=EngineDriver.driver().dbAdministrator().getDatabaseVersionInfo();
        StringBuilder sb=new StringBuilder("select * from (values ");
        int i=0;
        for(Map.Entry<String,Map<String,String>> entry: versionInfo.entrySet()){
            if(i!=0){
                sb.append(", ");
            }
            Map attrs = entry.getValue();
            sb.append(String.format("('%s','%s','%s','%s','%s')",
                    entry.getKey(),
                    attrs.get("release"),
                    attrs.get("implementationVersion"),
                    attrs.get("buildTime"),
                    attrs.get("url")));
            i++;
        }
        sb.append(") foo (hostname, release, implementationVersion, buildTime, url)");
        resultSet[0]=executeStatement(sb);
    }

    public static void SYSCS_GET_WRITE_INTAKE_INFO(ResultSet[] resultSets) throws SQLException{
        PipelineAdmin.SYSCS_GET_WRITE_INTAKE_INFO(resultSets);
    }

    public static void SYSCS_KILL_TRANSACTION(final long transactionId) throws SQLException{
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killTransaction(transactionId);
    }

    public static void SYSCS_KILL_STALE_TRANSACTIONS(final long maximumTransactionId) throws SQLException{
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killAllActiveTransactions(maximumTransactionId);
    }

    public static void SYSCS_SET_WRITE_POOL(final int writePool) throws SQLException{
        EngineDriver.driver().dbAdministrator().setWritePoolMaxThreadCount(writePool);

    }

    public static void SYSCS_GET_WRITE_POOL(final ResultSet[] resultSet) throws SQLException{
        Map<String,Integer> threadCounts = EngineDriver.driver().dbAdministrator().getWritePoolMaxThreadCount();

        StringBuilder sb=new StringBuilder("select * from (values ");
        int i=0;
        for(Map.Entry<String,Integer> tC:threadCounts.entrySet()){
            if(i!=0){
                sb.append(", ");
            }
            sb.append(String.format("('%s',%d)",
                    tC.getKey(),
                    tC.getValue()));
            i++;
        }
        sb.append(") foo (hostname, maxTaskWorkers)");
        resultSet[0]=executeStatement(sb);
    }

    public static void SYSCS_GET_REGION_SERVER_TASK_INFO(final ResultSet[] resultSet) throws StandardException, SQLException {
        // Be explicit about throwing runtime exception if this deleted procedure is called,
        // since it was so commonly used prior to k2. This could only happen if someone
        // upgraded from Lassen to K2 but didn't run the required upgrade script or run
        // SYSCS_UPDATE_ALL_SYSTEM_PROCEDURES.
        throw new UnsupportedOperationException(
                "SYSCS_GET_REGION_SERVER_TASK_INFO has been permanently desupported in Splice Machine."
        );
    }

    public static void SYSCS_GET_REGION_SERVER_CONFIG_INFO(final String configRoot,final int showDisagreementsOnly,final ResultSet[] resultSet) throws StandardException, SQLException{
        Map<String,DatabaseVersion> dbVersions = EngineDriver.driver().dbAdministrator().getClusterDatabaseVersions();
        boolean matchName=(configRoot!=null && !configRoot.equals(""));
        int hostIdx=0;
        String hostName;
        ResultSetBuilder rsBuilder;
        RowBuilder rowBuilder;

        try{
            rsBuilder=new ResultSetBuilder();
            rsBuilder.getColumnBuilder()
                    .addColumn("HOST_NAME",Types.VARCHAR,32)
                    .addColumn("CONFIG_NAME",Types.VARCHAR,128)
                    .addColumn("CONFIG_VALUE",Types.VARCHAR,128);

            rowBuilder=rsBuilder.getRowBuilder();
            // We arbitrarily pick DatabaseVersion MBean even though
            // we do not fetch anything from it. We just use it as our
            // mechanism for our region server context.
            SortedMap<String, String> configMap=new TreeMap<>();

            SConfiguration config=EngineDriver.driver().getConfiguration();
            Map<String,Object> configRootMap = config.getConfigMap();

            for(Map.Entry<String,DatabaseVersion> databaseVersion : dbVersions.entrySet()){
                hostName=databaseVersion.getKey();
                configMap.clear();
                for(Map.Entry<String,Object> conf : configRootMap.entrySet()){
                    configMap.put(conf.getKey(), conf.getValue().toString());
                }

                // Iterate through sorted configs and add to result set
                Set<Entry<String, String>> configSet=configMap.entrySet();
                for(Entry<String, String> configEntry : configSet){
                    rowBuilder.getDvd(0).setValue(hostName);
                    rowBuilder.getDvd(1).setValue(configEntry.getKey());
                    rowBuilder.getDvd(2).setValue(configEntry.getValue());
                    rowBuilder.addRow();
                }
                hostIdx++;
            }

            resultSet[0]=rsBuilder.buildResultSet((EmbedConnection)getDefaultConn());

            configMap.clear();

        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    public static void SYSCS_GET_REGION_SERVER_STATS_INFO(final ResultSet[] resultSet) throws SQLException{
        Collection<PartitionServer> load=getLoad();

        ExecRow template=new ValueRow(6);
        template.setRowArray(new DataValueDescriptor[]{
                new SQLVarchar(),new SQLInteger(),new SQLLongint(),new SQLLongint(),
                new SQLLongint(),new SQLLongint(),new SQLReal()
        });
        int i=0;
        List<ExecRow> rows=new ArrayList<>(load.size());
        for(PartitionServer ps:load){
            template.resetRowArray();
            DataValueDescriptor[] dvds=template.getRowArray();
            try{
                PartitionServerLoad psLoad = ps.getLoad();
                int idx=0;
                dvds[idx++].setValue(ps.getHostname());
                Set<PartitionLoad> partitionLoads=psLoad.getPartitionLoads();
                long storeFileCount = 0L;
                for(PartitionLoad pLoad:partitionLoads){
                    storeFileCount+=pLoad.getStorefileSizeMB();
                }
                dvds[idx++].setValue(partitionLoads.size());
                dvds[idx++].setValue(storeFileCount);
                dvds[idx++].setValue(psLoad.totalWriteRequests());
                dvds[idx++].setValue(psLoad.totalReadRequests());
                dvds[idx++].setValue(psLoad.totalRequests());

            }catch(StandardException se){
                throw PublicAPI.wrapStandardException(se);
            }catch(Exception e){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
            rows.add(template.getClone());
            i++;
        }
        ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[6];
        int idx=0;
        columnInfo[idx++]=new GenericColumnDescriptor("host",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR));
        columnInfo[idx++]=new GenericColumnDescriptor("regionCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        columnInfo[idx++]=new GenericColumnDescriptor("storeFileCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        columnInfo[idx++]=new GenericColumnDescriptor("writeRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        columnInfo[idx++]=new GenericColumnDescriptor("readRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        columnInfo[idx++]=new GenericColumnDescriptor("totalRequestCount",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT));
        EmbedConnection defaultConn=(EmbedConnection)BaseAdminProcedures.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
        try{
            resultsToWrap.openCore();
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
        EmbedResultSet ers=new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);

        resultSet[0]=ers;
    }

    public static void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException{
        StringBuilder sb=new StringBuilder("select * from (values ");
        int i=0;
        Collection<PartitionServer> servers=getLoad();
        boolean isFirst=true;
        for(PartitionServer server : servers){
            if(!isFirst) sb=sb.append(",");
            else isFirst=false;
            try{
                sb=sb.append("('").append(server.getHostname()).append("'")
                        .append(",").append(server.getPort())
                        .append(",").append(server.getLoad().totalRequests())
                        .append(")");
            }catch(IOException e){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }

        }
        sb.append(") foo (hostname, port, totalRequests)");
        resultSet[0]=SpliceAdmin.executeStatement(sb);
    }

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(String schemaName) throws SQLException{
        SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(schemaName,null);
    }

    /**
     * Perform a major compaction
     *
     * @param schemaName the name of the database schema to discriminate the tablename.  If null,
     *                   defaults to the 'APP' schema.
     * @param tableName  the table name on which to run compaction. If null, compaction will be run
     *                   on all tables in the schema.  Note that a given tablename can produce more
     *                   than one table, if the table has an index, for instance.
     * @throws SQLException
     */
    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(String schemaName,String tableName) throws SQLException{
        // sys query for table conglomerate for in schema
        PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
        for(long conglomID : getConglomNumbers(getDefaultConn(),schemaName,tableName)){
            try(Partition partition=tableFactory.getTable(Long.toString(conglomID))){
                partition.compact();
            }catch(IOException e){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
        }
    }

    /**
     * Perform a flush on a table
     *
     * @param schemaName the name of the database schema to discriminate the tablename.  If null,
     *                   defaults to the 'APP' schema.
     * @param tableName  the table name on which to run flush.
     * @throws SQLException
     */
    public static void SYSCS_FLUSH_TABLE(String schemaName,String tableName) throws SQLException{
        // sys query for table conglomerate for in schema
        PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
        for(long conglomID : getConglomNumbers(getDefaultConn(),schemaName,tableName)){
            try(Partition partition=tableFactory.getTable(Long.toString(conglomID))){
                partition.flush();
            }catch(IOException e){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
        }
    }

    public static void VACUUM() throws SQLException{
        Vacuum vacuum=new Vacuum(getDefaultConn());
        try{
            vacuum.vacuumDatabase();
        }finally{
            vacuum.shutdown();
        }
    }

    private static final ResultColumnDescriptor[] SCHEMA_INFO_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("SCHEMANAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("TABLENAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("REGIONNAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("IS_INDEX",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
            new GenericColumnDescriptor("HBASEREGIONS_STORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("MEMSTORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("STOREINDEXSIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };

    public static void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException{
        List<ExecRow> results=new ArrayList<>();
        try(PreparedStatement preparedStatement=SpliceAdmin.getDefaultConn().prepareStatement("SELECT S.SCHEMANAME, T.TABLENAME, " +
                "C.ISINDEX, "+
                "C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S "+
                "WHERE C.TABLEID = T.TABLEID AND T.SCHEMAID = S.SCHEMAID AND T.TABLETYPE not in ('S','E') "+
                "ORDER BY S.SCHEMANAME")){
            try(ResultSet allTablesInSchema=preparedStatement.executeQuery()){

                ExecRow template;
                try{
                    DataValueDescriptor[] columns=new DataValueDescriptor[SpliceAdmin.SCHEMA_INFO_COLUMNS.length];
                    for(int i=0;i<SpliceAdmin.SCHEMA_INFO_COLUMNS.length;i++){
                        columns[i]=SpliceAdmin.SCHEMA_INFO_COLUMNS[i].getType().getNull();
                    }
                    template=new ValueRow(columns.length);
                    template.setRowArray(columns);
                }catch(StandardException e){
                    throw PublicAPI.wrapStandardException(e);
                }

                try(PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin()){
                    while(allTablesInSchema.next()){
                        String conglom=allTablesInSchema.getObject("CONGLOMERATENUMBER").toString();
                        for(Partition ri : admin.allPartitions(conglom)){
                            String regionName=ri.getName();//Bytes.toString(ri.getRegionName());
                            int storefileSizeMB=0;
                            int memStoreSizeMB=0;
                            int storefileIndexSizeMB=0;
                            if(regionName!=null && !regionName.isEmpty()){
                                PartitionLoad regionLoad=ri.getLoad();//regionLoadMap.get(regionName);
                                if(regionLoad!=null){
                                    storefileSizeMB=regionLoad.getStorefileSizeMB();
                                    memStoreSizeMB=regionLoad.getMemStoreSizeMB();
                                    storefileIndexSizeMB=regionLoad.getStorefileIndexSizeMB();
                                }
                            }
                            DataValueDescriptor[] cols=template.getRowArray();
                            try{
                                cols[0].setValue(allTablesInSchema.getString("SCHEMANAME"));
                                cols[1].setValue(allTablesInSchema.getString("TABLENAME"));
                                cols[2].setValue(regionName);
                                cols[3].setValue(allTablesInSchema.getBoolean("ISINDEX"));
                                cols[4].setValue(storefileSizeMB);
                                cols[5].setValue(memStoreSizeMB);
                                cols[6].setValue(storefileIndexSizeMB);
                            }catch(StandardException se){
                                throw PublicAPI.wrapStandardException(se);
                            }
                            results.add(template.getClone());
                        }
                    }
                }catch(IOException ioe){
                    throw PublicAPI.wrapStandardException(Exceptions.parseException(ioe));
                }
            }
        }
        EmbedConnection defaultConn=(EmbedConnection)SpliceAdmin.getDefaultConn();
        Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(results,SpliceAdmin.SCHEMA_INFO_COLUMNS,lastActivation);
        try{
            resultsToWrap.openCore();
            EmbedResultSet ers=new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);
            resultSet[0]=ers;
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Prints all the information related to the execution plans of the stored prepared statements (metadata queries).
     */

    public static void SYSCS_GET_STORED_STATEMENT_PLAN_INFO(ResultSet[] rs) throws SQLException{
        try{
            // Wow...  who knew it was so much work to create a ResultSet?  Ouch!  The following code is annoying.

            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            DataDictionary dd=lcc.getDataDictionary();
            List list=dd.getAllSPSDescriptors();
            ArrayList<ExecRow> rows=new ArrayList<>(list.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   STMTNAME				VARCHAR
            //   TYPE					CHAR
            //   VALID					BOOLEAN
            //   LASTCOMPILED			TIMESTAMP
            //   INITIALLY_COMPILABLE	BOOLEAN
            //   CONSTANTSTATE			BLOB --> VARCHAR showing existence of plan
            DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                    new SQLVarchar(),
                    new SQLChar(),
                    new SQLBoolean(),
                    new SQLTimestamp(),
                    new SQLBoolean(),
                    new SQLVarchar()
            };
            int numCols=dvds.length;
            ExecRow dataTemplate=new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the descriptors into the rows.
            for(Object aList : list){
                SPSDescriptor spsd=(SPSDescriptor)aList;
                ExecPreparedStatement ps=spsd.getPreparedStatement(false);
                dvds[0].setValue(spsd.getName());
                dvds[1].setValue(spsd.getTypeAsString());
                dvds[2].setValue(spsd.isValid());
                dvds[3].setValue(spsd.getCompileTime());
                dvds[4].setValue(spsd.initiallyCompilable());
                dvds[5].setValue(spsd.getPreparedStatement(false)==null?null:"[object]");
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[numCols];
            columnInfo[0]=new GenericColumnDescriptor("STMTNAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,60));
            columnInfo[1]=new GenericColumnDescriptor("TYPE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR,4));
            columnInfo[2]=new GenericColumnDescriptor("VALID",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[3]=new GenericColumnDescriptor("LASTCOMPILED",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP));
            columnInfo[4]=new GenericColumnDescriptor("INITIALLY_COMPILABLE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[5]=new GenericColumnDescriptor("CONSTANTSTATE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,13));
            EmbedConnection defaultConn=(EmbedConnection)getDefaultConn();
            Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers=new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);
            rs[0]=ers;
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Get the values of all properties for the current connection.
     *
     * @param rs array of result set objects that contains all of the defined properties
     *           for the JVM, service, database, and app.
     * @throws SQLException Standard exception policy.
     **/
    public static void SYSCS_GET_ALL_PROPERTIES(ResultSet[] rs) throws SQLException{
        try{
            LanguageConnectionContext lcc=ConnectionUtil.getCurrentLCC();
            TransactionController tc=lcc.getTransactionExecute();

            // Fetch all the properties.
            Properties jvmProps=addTypeToProperties(System.getProperties(),"JVM",true);
            Properties dbProps=addTypeToProperties(tc.getProperties());  // Includes both database and service properties.
            ModuleFactory monitor=Monitor.getMonitorLite();
            Properties appProps=addTypeToProperties(monitor.getApplicationProperties(),"APP",false);

            // Merge the properties using the correct search order.
            // SEARCH ORDER: JVM, Service, Database, App
            appProps.putAll(dbProps);  // dbProps already has been overwritten with service properties.
            appProps.putAll(jvmProps);
            ArrayList<ExecRow> rows=new ArrayList<>(appProps.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   KEY			VARCHAR
            //   VALUE			VARCHAR
            //   TYPE			VARCHAR (JVM, SERVICE, DATABASE, APP)
            DataValueDescriptor[] dvds=new DataValueDescriptor[]{
                    new SQLVarchar(),
                    new SQLVarchar(),
                    new SQLVarchar()
            };
            int numCols=dvds.length;
            ExecRow dataTemplate=new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the properties into rows.  Sort the properties by key first.
            ArrayList<String> keyList=new ArrayList<>();
            for(Object o:appProps.keySet()){
                if(o instanceof String)
                    keyList.add((String)o);
            }
            Collections.sort(keyList);
            for(String key : keyList){
                String[] typedValue=(String[])appProps.get(key);
                dvds[0].setValue(key);
                dvds[1].setValue(typedValue[0]);
                dvds[2].setValue(typedValue[1]);
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[] columnInfo=new ResultColumnDescriptor[numCols];
            columnInfo[0]=new GenericColumnDescriptor("KEY",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,50));
            columnInfo[1]=new GenericColumnDescriptor("VALUE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,40));
            columnInfo[2]=new GenericColumnDescriptor("TYPE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR,10));
            EmbedConnection defaultConn=(EmbedConnection)getDefaultConn();
            Activation lastActivation=defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap=new IteratorNoPutResultSet(rows,columnInfo,lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers=new EmbedResultSet40(defaultConn,resultsToWrap,false,null,true);
            rs[0]=ers;
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Tag each property value with a 'type'.  The structure of the map changes from:
     * {key --> value}
     * to:
     * {key --> [value, type]}
     *
     * @param props           Map of properties to tag with type
     * @param type            Type of property (JVM, SERVICE, DATABASE, APP, etc.)
     * @param derbySpliceOnly If true, only include properties with keys that start with "derby" or "splice".
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props,String type,boolean derbySpliceOnly){
        Properties typedProps=new Properties();
        if(props!=null){
            for(Entry<Object, Object> objectObjectEntry : props.entrySet()){
                Entry prop=(Entry)objectObjectEntry;
                String key=(String)prop.getKey();
                if(key==null) continue;
                if(derbySpliceOnly){
                    String lowerKey=key.toLowerCase();
                    if(!lowerKey.startsWith("derby") && !lowerKey.startsWith("splice")){
                        continue;
                    }
                }
                String[] typedValue=new String[2];
                typedValue[0]=(String)prop.getValue();
                typedValue[1]=type;
                typedProps.put(key,typedValue);
            }
        }
        return typedProps;
    }

    /**
     * Tag each property value with a 'type' of SERVICE or DATABASE.  The structure of the map changes from:
     * {key --> value}
     * to:
     * {key --> [value, type]}
     *
     * @param props Map of properties to tag with type
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props){
        Properties typedProps=new Properties();
        if(props!=null){
            for(Entry<Object, Object> objectObjectEntry : props.entrySet()){
                Entry prop=(Entry)objectObjectEntry;
                String key=(String)prop.getKey();
                String[] typedValue=new String[2];
                typedValue[0]=(String)prop.getValue();
                typedValue[1]=PropertyUtil.isServiceProperty(key)?"SERVICE":"DATABASE";
                typedProps.put(key,typedValue);
            }
        }
        return typedProps;
    }

    private static final String sqlConglomsInSchema=
            "SELECT C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S "+
                    "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID AND S.SCHEMANAME = ?";

    private static final String sqlConglomsInTable=
            sqlConglomsInSchema+" AND T.TABLENAME = ?";

    public static String getSqlConglomsInSchema(){
        return sqlConglomsInSchema;
    }

    public static String getSqlConglomsInTable(){
        return sqlConglomsInTable;
    }

    /**
     * Be Careful when using this, as it will return conglomerate ids for all the indices of a table
     * as well as the table itself. While the first conglomerate SHOULD be the main table, there
     * really isn't a guarantee, and it shouldn't be relied upon for correctness in all cases.
     */
    public static long[] getConglomNumbers(Connection conn,String schemaName,String tableName) throws SQLException{
        List<Long> conglomIDs=new ArrayList<>();
        if(schemaName==null)
            // default schema
            schemaName= SQLConfiguration.SPLICE_USER;

        String query;
        boolean isTableNameEmpty;

        if(tableName==null){
            query=getSqlConglomsInSchema();
            isTableNameEmpty=true;
        }else{
            query=getSqlConglomsInTable();
            isTableNameEmpty=false;
        }

        ResultSet rs=null;
        PreparedStatement s=null;
        try{
            s=conn.prepareStatement(query);
            s.setString(1,schemaName.toUpperCase());
            if(!isTableNameEmpty){
                s.setString(2,tableName.toUpperCase());
            }
            rs=s.executeQuery();
            while(rs.next()){
                conglomIDs.add(rs.getLong(1));
            }

            if(conglomIDs.isEmpty()){
                if(isTableNameEmpty){
                    throw PublicAPI.wrapStandardException(ErrorState.LANG_SCHEMA_DOES_NOT_EXIST.newException(schemaName));
                }
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            }
        }finally{
            if(rs!=null) rs.close();
            if(s!=null) s.close();
        }
        if(conglomIDs.isEmpty()){
            return new long[0];
        }
        long[] congloms=new long[conglomIDs.size()];
        for(int i=0;i<conglomIDs.size();i++){
            congloms[i]=conglomIDs.get(i);
        }
                /*
                 * An index conglomerate id can be returned by the query before the main table one is,
				 * but it should ALWAYS have a higher conglomerate id, so if we sort the congloms,
				 * we should return the main table before any of its indices.
				 */
        Arrays.sort(congloms);
        return congloms;
    }


    private static class Trip<T,U,V>{
        private final T first;
        private final U second;
        private final V third;

        public Trip(T first,U second,V third){
            this.first=first;
            this.second=second;
            this.third=third;
        }

        public T getFirst(){
            return first;
        }

        public U getSecond(){
            return second;
        }

        public V getThird(){
            return third;
        }

    }


    public static void SYSCS_GET_GLOBAL_DATABASE_PROPERTY(final String key,final ResultSet[] resultSet) throws SQLException{
        Map<String,String> valueMap = EngineDriver.driver().dbAdministrator().getGlobalDatabaseProperty(key);

        ResultSetBuilder rsBuilder=new ResultSetBuilder();
        try{
            rsBuilder.getColumnBuilder()
                    .addColumn("HOST_NAME",Types.VARCHAR,32)
                    .addColumn("PROPERTY_VALUE",Types.VARCHAR,128);
            RowBuilder rowBuilder=rsBuilder.getRowBuilder();
            int hostIdx=0;
            for(Map.Entry<String,String> entry : valueMap.entrySet()){
                rowBuilder.getDvd(0).setValue(entry.getKey());
                rowBuilder.getDvd(1).setValue(entry.getValue());
                rowBuilder.addRow();
                hostIdx++;
            }
            resultSet[0]=rsBuilder.buildResultSet((EmbedConnection)getDefaultConn());
        }catch(StandardException se){
            throw PublicAPI.wrapStandardException(se);
        }
    }

    public static void SYSCS_SET_GLOBAL_DATABASE_PROPERTY(final String key,final String value) throws SQLException{
        EngineDriver.driver().dbAdministrator().setGlobalDatabaseProperty(key,value);
    }

    public static void SYSCS_ENABLE_ENTERPRISE(final String value) throws SQLException{
        try {
            EngineDriver.driver().manager().enableEnterprise(value.toCharArray());
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            lcc.getDataDictionary().startWriting(lcc);
            TransactionController tc = lcc.getTransactionExecute();
            DDLMessage.DDLChange ddlChange = ProtoUtil.createRefreshEnterpriseFeatures(((SpliceTransactionManager) tc).getActiveStateTxn().getTxnId());
            // Run Remotely
            tc.prepareDataDictionaryChange(DDLUtils.notifyMetadataChange(ddlChange));
        } catch (Exception e) {
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE() throws SQLException{
        EngineDriver.driver().dbAdministrator().emptyGlobalStatementCache();
    }

    private static Collection<PartitionServer> getLoad() throws SQLException{
        try(PartitionAdmin admin=SIDriver.driver().getTableFactory().getAdmin()){
            return admin.allServers();
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }
    }

    public static void GET_ACTIVATION(final String statement, final ResultSet[] resultSet) throws SQLException, StandardException {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        GenericPreparedStatement gps = (GenericPreparedStatement)lcc.prepareInternalStatement(statement);
        GenericActivationHolder activationHolder = (GenericActivationHolder)gps.getActivation(lcc, false);
        Activation activation = activationHolder.ac;
        ActivationHolder ah = new ActivationHolder(activation, null);
        byte[] activationHolderBytes = SerializationUtils.serialize(ah);
        DataValueDescriptor[] dvds = new DataValueDescriptor[] {
            new SQLBlob()
        };
        int numCols = dvds.length;
        ExecRow dataTemplate = new ValueRow(numCols);
        dataTemplate.setRowArray(dvds);

        List<ExecRow> rows = Lists.newArrayList();
        dvds[0].setValue(activationHolderBytes);
        rows.add(dataTemplate);

        // Describe the format of the output rows (ResultSet).
        ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[numCols];
        columnInfo[0] = new GenericColumnDescriptor("ACTIVATION", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BLOB));
        EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
        Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
        IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo, lastActivation);
        resultsToWrap.openCore();
        EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);

        resultSet[0] = ers;
    }

    /**
     * Stored procedure that updates the owner (authorization id) for an existing schema.
     * Only the database owner is allowed to invoke this action.
     */
    public static void SYSCS_UPDATE_SCHEMA_OWNER(String schemaName, String ownerName) throws SQLException {
        if (schemaName == null || schemaName.isEmpty()) throw new SQLException("Invalid null or empty value for 'schemaName'");
        if (ownerName == null || ownerName.isEmpty()) throw new SQLException("Invalid null or empty value for 'ownerName'");
        schemaName = schemaName.toUpperCase();
        ownerName = ownerName.toUpperCase();
        try {
            checkCurrentUserIsDatabaseOwnerAccess();
            EmbedConnection defaultConn = (EmbedConnection)getDefaultConn();
            LanguageConnectionContext lcc = defaultConn.getLanguageConnection();
            TransactionController tc = lcc.getTransactionExecute();
            DataDictionary dd = lcc.getDataDictionary();
            dd.startWriting(lcc);
            dd.getSchemaDescriptor(schemaName, tc, /* raiseError= */true);
            if (dd.getUser(ownerName) == null) {
                throw StandardException.newException(String.format("User '%s' does not exist.", ownerName));
            }
            ((DataDictionaryImpl)dd).updateSchemaAuth(schemaName, ownerName, tc);
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        } catch (Exception e) {
            throw new SQLException(e);
        }
    }

    public static void SYSCS_RESTORE_DATABASE_OWNER() throws SQLException{
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        TransactionController tc  = lcc.getTransactionExecute();

        SystemProcedures.updateSystemSchemaAuthorization("SPLICE", tc);
    }

    private static void checkCurrentUserIsDatabaseOwnerAccess() throws Exception
    {
        EmbedConnection defaultConn = (EmbedConnection)getDefaultConn();
        LanguageConnectionContext lcc = defaultConn.getLanguageConnection();
        DataDictionary dd = lcc.getDataDictionary();
        if (dd.usesSqlAuthorization()) {
            String databaseOwner = dd.getAuthorizationDatabaseOwner();
            String currentUser = lcc.getStatementContext().getSQLSessionContext().getCurrentUser();
            if (!databaseOwner.equals(currentUser)) {
                throw StandardException.newException(SQLState.DBO_ONLY);
            }
        }
    }
}
