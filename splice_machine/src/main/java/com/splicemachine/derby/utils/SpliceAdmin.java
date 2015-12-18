package com.splicemachine.derby.utils;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import com.splicemachine.db.impl.sql.GenericActivationHolder;
import com.splicemachine.db.impl.sql.GenericPreparedStatement;
import com.splicemachine.db.impl.sql.GenericStorablePreparedStatement;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionStack;
import com.splicemachine.derby.hbase.*;
import com.splicemachine.derby.hbase.SpliceObserverInstructions.ActivationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.tools.version.SpliceMachineVersion;
import com.splicemachine.derby.management.StatementManagement;
import com.splicemachine.hbase.jmx.JMXUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang.SerializationUtils;
import com.splicemachine.utils.logging.Logging;
import javax.management.MalformedObjectNameException;
import javax.management.remote.JMXConnector;
import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.monitor.ModuleFactory;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.ConnectionUtil;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SPSDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecPreparedStatement;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.jdbc.ResultSetBuilder;
import com.splicemachine.db.impl.jdbc.ResultSetBuilder.RowBuilder;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.threadpool.ThreadPoolStatus;

/**
 * @author Jeff Cunningham
 *
 *         Date: 12/9/13
 */
public class SpliceAdmin extends BaseAdminProcedures {
	protected static final DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private static Logger LOG = Logger.getLogger(SpliceAdmin.class);

    public static void SYSCS_SET_LOGGER_LEVEL(final String loggerName, final String logLevel) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    logger.setLoggerLevel(loggerName, logLevel);
                }
            }
        });
    }

    public static void SYSCS_GET_LOGGER_LEVEL(final String loggerName, final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                StringBuilder sb = new StringBuilder("select * from (values ");
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    logger.getLoggerLevel(loggerName);
                    sb.append(String.format("('%s')",
                            logger.getLoggerLevel(loggerName)));
                }
                sb.append(") foo (logLevel)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_LOGGERS(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                Set<String> uniqueLoggerNames = new HashSet<String>();
                for (Logging logger : JMXUtils.getLoggingManagement(connections)) {
                    uniqueLoggerNames.addAll(logger.getLoggerNames());
                }
                StringBuilder sb = new StringBuilder("select * from (values ");
                List<String> loggerNames = new ArrayList<String>(uniqueLoggerNames);
                Collections.sort(loggerNames);
                for (String logger : loggerNames) {
                    sb.append(String.format("('%s')", logger));
                    sb.append(", ");
                }
                if (sb.charAt(sb.length()-2) == ',') {
                    sb.setLength(sb.length()-2);
                }
                sb.append(") foo (spliceLogger)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_ACTIVE_SERVERS(ResultSet[] resultSet) throws SQLException {
        StringBuilder sb = new StringBuilder("select * from (values ");
        int i = 0;
        for (ServerName serverName : SpliceUtils.getServers()) {
            if (i != 0) {
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

    public static void SYSCS_GET_VERSION_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<SpliceMachineVersion> spliceMachineVersions = JMXUtils.getSpliceMachineVersion(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (SpliceMachineVersion spliceMachineVersion : spliceMachineVersions) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s','%s','%s','%s','%s')", connections.get(i).getFirst(),
                            spliceMachineVersion.getRelease(),
                            spliceMachineVersion.getImplementationVersion(),
                            spliceMachineVersion.getBuildTime(),
                            spliceMachineVersion.getURL()));
                    i++;
                }
                sb.append(") foo (hostname, release, implementationVersion, buildTime, url)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    public static void SYSCS_GET_WRITE_PIPELINE_INFO(ResultSet[] resultSets) throws SQLException{
        PipelineAdmin.SYSCS_GET_WRITE_PIPELINE_INFO(resultSets);
    }

    public static void SYSCS_GET_WRITE_INTAKE_INFO(ResultSet[] resultSets) throws SQLException{
        PipelineAdmin.SYSCS_GET_WRITE_INTAKE_INFO(resultSets);
    }

    public static void SYSCS_KILL_TRANSACTION(final long transactionId) throws SQLException {
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killTransaction(transactionId);
    }

    public static void SYSCS_KILL_STALE_TRANSACTIONS(final long maximumTransactionId) throws SQLException {
        /*
         * We have to leave this method in place, because Derby will actually STORE a string
         * reference to this method in database tables--removing it will therefore break
         * backwards compatibility. However, the logic has been moved to TransactionAdmin
         */
        TransactionAdmin.killAllActiveTransactions(maximumTransactionId);
    }

    public static void SYSCS_SET_WRITE_POOL(final int writePool) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
                for (ThreadPoolStatus threadPool : threadPools) {
                    threadPool.setMaxThreadCount(writePool);
                }
            }
        });

    }

    public static void SYSCS_GET_WRITE_POOL(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<ThreadPoolStatus> threadPools = JMXUtils.getMonitoredThreadPools(connections);
                StringBuilder sb = new StringBuilder("select * from (values ");
                int i = 0;
                for (ThreadPoolStatus threadPool : threadPools) {
                    if (i != 0) {
                        sb.append(", ");
                    }
                    sb.append(String.format("('%s',%d)",
                            connections.get(i).getFirst(),
                            threadPool.getMaxThreadCount()));
                    i++;
                }
                sb.append(") foo (hostname, maxTaskWorkers)");
                resultSet[0] = executeStatement(sb);
            }
        });
    }

    private String getConfigProp(String propName) {
    	Configuration config = SpliceUtils.getConfig();
    	String value = config.get(propName);
    	return value;
    }

    private static Configuration getConfig() {
    	return SpliceUtils.getConfig();
    }

    public static void SYSCS_GET_REGION_SERVER_CONFIG_INFO(final String configRoot, final int showDisagreementsOnly, final ResultSet[] resultSet) throws StandardException, SQLException {
        operate(new JMXServerOperation() {
            @SuppressWarnings("unused")
			@Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                boolean matchName = (configRoot != null && !configRoot.equals(""));
            	List<SpliceMachineVersion> spliceMachineVersions = JMXUtils.getSpliceMachineVersion(connections);
                int hostIdx = 0;
                String hostName;
                Configuration config;
        		ResultSetBuilder rsBuilder;
    			RowBuilder rowBuilder;

                try {

	                rsBuilder = new ResultSetBuilder();
	    			rsBuilder.getColumnBuilder()
	    				.addColumn("HOST_NAME",    Types.VARCHAR, 32)
	    				.addColumn("CONFIG_NAME",  Types.VARCHAR, 128)
	    				.addColumn("CONFIG_VALUE", Types.VARCHAR, 128);
	
	    			rowBuilder = rsBuilder.getRowBuilder();
	                // We arbitrarily pick SpliceMachineVersion MBean even though
	                // we do not fetch anything from it. We just use it as our
	                // mechanism for our region server context.
	                SortedMap<String, String> configMap = new TreeMap<String, String>();
	                
	                for (SpliceMachineVersion spliceMachineVersion : spliceMachineVersions) {
	            		hostName = connections.get(hostIdx).getFirst();
	                	configMap.clear();
	                    config = getConfig();
	                    for (Entry<String, String> rawEntry : config) {
	                        // use config.getValByRegex() instead of iterating and comparing manually?
	                    	if (matchName && !(rawEntry.getKey().startsWith(configRoot))) continue;
	                    	configMap.put(rawEntry.getKey(), rawEntry.getValue());
	                    }
	
	                    // Iterate through sorted configs and add to result set
	                    Set<Entry<String, String>> configSet = configMap.entrySet();
	                    for (Entry<String, String> configEntry : configSet) {
	    					rowBuilder.getDvd(0).setValue(hostName);
	    					rowBuilder.getDvd(1).setValue(configEntry.getKey());
	    					rowBuilder.getDvd(2).setValue(configEntry.getValue());
	    					rowBuilder.addRow();
	                    }
	                    hostIdx++;
	                }
	
	                resultSet[0] = rsBuilder.buildResultSet((EmbedConnection)getDefaultConn());
	
	                configMap.clear();

	    		} catch (StandardException se) {
	    			throw PublicAPI.wrapStandardException(se);
	    		}
            }
        });
    }

    public static void SYSCS_GET_REGION_SERVER_STATS_INFO(final ResultSet[] resultSet) throws SQLException {
        operate(new JMXServerOperation() {
            @Override
            public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
            	derbyFactory.SYSCS_GET_REGION_SERVER_STATS_INFO(resultSet, connections);
            }
        });
    }

    public static void SYSCS_GET_REQUESTS(ResultSet[] resultSet) throws SQLException {
    	derbyFactory.SYSCS_GET_REQUESTS(resultSet);
	}

    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA(String schemaName) throws SQLException {
        SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(schemaName, null);
    }

    /**
     * Perform a major compaction
     * @param schemaName the name of the database schema to discriminate the tablename.  If null,
     *                   defaults to the 'APP' schema.
     * @param tableName the table name on which to run compaction. If null, compaction will be run
     *                  on all tables in the schema.  Note that a given tablename can produce more
     *                  than one table, if the table has an index, for instance.
     * @throws SQLException
     */
    public static void SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE(String schemaName, String tableName)
            throws SQLException {
        HBaseAdmin admin = null;
        try {
            admin = SpliceUtils.getAdmin();
            // sys query for table conglomerate for in schema
            for (long conglomID : getConglomNumbers(getDefaultConn(), schemaName, tableName)) {
                try {
                    admin.majorCompact(HBaseTableInfoFactory.getInstance().getTableInfo(Long.toString(conglomID)));
                } catch (Exception e) {
                    SpliceLogUtils.warn(LOG, "SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE failed on %s with this message %s, waiting two seconds and will try again", Long.toString(conglomID),e.getMessage());
                    try {
                        Thread.sleep(2000);
                        admin.majorCompact(HBaseTableInfoFactory.getInstance().getTableInfo(Long.toString(conglomID)));
                    } catch (Exception secondE) {
                        SpliceLogUtils.warn(LOG, "SYSCS_PERFORM_MAJOR_COMPACTION_ON_TABLE failed on %s with this message %s after waiting 2 seconds, compaction attempt aborted", Long.toString(conglomID),e.getMessage());
                    }
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

    public static void VACUUM() throws SQLException{
        Vacuum vacuum = new Vacuum(getDefaultConn());
        try{
            vacuum.vacuumDatabase();
        }finally{
            vacuum.shutdown();
        }
    }
    public static ResultColumnDescriptor[] SCHEMA_INFO_COLUMNS = new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("SCHEMANAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("TABLENAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("REGIONNAME",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR)),
            new GenericColumnDescriptor("IS_INDEX",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN)),
            new GenericColumnDescriptor("HBASEREGIONS_STORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("MEMSTORESIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("STOREINDEXSIZE",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
    };
    public static void SYSCS_GET_SCHEMA_INFO(final ResultSet[] resultSet) throws SQLException {
    	derbyFactory.SYSCS_GET_SCHEMA_INFO(resultSet);
            }

    /**
     * Prints all the information related to the execution plans of the stored prepared statements (metadata queries).
     */
    public static void SYSCS_GET_STORED_STATEMENT_PLAN_INFO(ResultSet[] rs) throws SQLException
    {
        try {
            // Wow...  who knew it was so much work to create a ResultSet?  Ouch!  The following code is annoying.

            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            DataDictionary dd = lcc.getDataDictionary();
            List list = dd.getAllSPSDescriptors();
            ArrayList<ExecRow> rows = new ArrayList<ExecRow>(list.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   STMTNAME				VARCHAR
            //   TYPE					CHAR
            //   VALID					BOOLEAN
            //   LASTCOMPILED			TIMESTAMP
            //   INITIALLY_COMPILABLE	BOOLEAN
            //   CONSTANTSTATE			BLOB --> VARCHAR showing existence of plan
            DataValueDescriptor[] dvds = new DataValueDescriptor[] {
                    new SQLVarchar(),
                    new SQLChar(),
                    new SQLBoolean(),
                    new SQLTimestamp(),
                    new SQLBoolean(),
                    new SQLVarchar()
            };
            int numCols = dvds.length;
            ExecRow dataTemplate = new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the descriptors into the rows.
            for (Iterator li = list.iterator(); li.hasNext(); )
            {
                SPSDescriptor spsd = (SPSDescriptor) li.next();
                ExecPreparedStatement ps = spsd.getPreparedStatement(false);
                dvds[0].setValue(spsd.getName());
                dvds[1].setValue(spsd.getTypeAsString());
                dvds[2].setValue(spsd.isValid());
                dvds[3].setValue(spsd.getCompileTime());
                dvds[4].setValue(spsd.initiallyCompilable());
                dvds[5].setValue(spsd.getPreparedStatement(false) == null ? null : "[object]");
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[numCols];
            columnInfo[0] = new GenericColumnDescriptor("STMTNAME", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 60));
            columnInfo[1] = new GenericColumnDescriptor("TYPE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.CHAR, 4));
            columnInfo[2] = new GenericColumnDescriptor("VALID", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[3] = new GenericColumnDescriptor("LASTCOMPILED", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.TIMESTAMP));
            columnInfo[4] = new GenericColumnDescriptor("INITIALLY_COMPILABLE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BOOLEAN));
            columnInfo[5] = new GenericColumnDescriptor("CONSTANTSTATE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 13));
            EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo, lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);
            rs[0] = ers;
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Get the values of all properties for the current connection.
     *
     *  @param rs array of result set objects that contains all of the defined properties
     *            for the JVM, service, database, and app.
     *
     * @exception  StandardException  Standard exception policy.
     **/
    public static void SYSCS_GET_ALL_PROPERTIES(ResultSet[] rs) throws SQLException
    {
        try {
            LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
            TransactionController tc = lcc.getTransactionExecute();

            // Fetch all the properties.
            Properties jvmProps = addTypeToProperties(System.getProperties(), "JVM", true);
            Properties dbProps = addTypeToProperties(tc.getProperties());  // Includes both database and service properties.
            ModuleFactory monitor = Monitor.getMonitorLite();
            Properties appProps = addTypeToProperties(monitor.getApplicationProperties(), "APP", false);

            // Merge the properties using the correct search order.
            // SEARCH ORDER: JVM, Service, Database, App
            appProps.putAll(dbProps);  // dbProps already has been overwritten with service properties.
            appProps.putAll(jvmProps);
            ArrayList<ExecRow> rows = new ArrayList<ExecRow>(appProps.size());

            // Describe the format of the input rows (ExecRow).
            //
            // Columns of "virtual" row:
            //   KEY			VARCHAR
            //   VALUE			VARCHAR
            //   TYPE			VARCHAR (JVM, SERVICE, DATABASE, APP)
            DataValueDescriptor[] dvds = new DataValueDescriptor[] {
                    new SQLVarchar(),
                    new SQLVarchar(),
                    new SQLVarchar()
            };
            int numCols = dvds.length;
            ExecRow dataTemplate = new ValueRow(numCols);
            dataTemplate.setRowArray(dvds);

            // Transform the properties into rows.  Sort the properties by key first.
            ArrayList<String> keyList = new ArrayList(appProps.keySet());
            Collections.sort(keyList);
            for (Iterator<String> iter = keyList.iterator(); iter.hasNext(); )
            {
                String key = (String) iter.next();
                String[] typedValue = (String[]) appProps.get(key);
                dvds[0].setValue(key);
                dvds[1].setValue(typedValue[0]);
                dvds[2].setValue(typedValue[1]);
                rows.add(dataTemplate.getClone());
            }

            // Describe the format of the output rows (ResultSet).
            ResultColumnDescriptor[]columnInfo = new ResultColumnDescriptor[numCols];
            columnInfo[0] = new GenericColumnDescriptor("KEY", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 50));
            columnInfo[1] = new GenericColumnDescriptor("VALUE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 40));
            columnInfo[2] = new GenericColumnDescriptor("TYPE", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR, 10));
            EmbedConnection defaultConn = (EmbedConnection) getDefaultConn();
            Activation lastActivation = defaultConn.getLanguageConnection().getLastActivation();
            IteratorNoPutResultSet resultsToWrap = new IteratorNoPutResultSet(rows, columnInfo, lastActivation);
            resultsToWrap.openCore();
            EmbedResultSet ers = new EmbedResultSet40(defaultConn, resultsToWrap, false, null, true);
            rs[0] = ers;
        } catch (StandardException se) {
            throw PublicAPI.wrapStandardException(se);
        }
    }

    /**
     * Tag each property value with a 'type'.  The structure of the map changes from:
     *     {key --> value}
     * to:
     *     {key --> [value, type]}
     *
     * @param props				Map of properties to tag with type
     * @param type				Type of property (JVM, SERVICE, DATABASE, APP, etc.)
     * @param derbySpliceOnly	If true, only include properties with keys that start with "derby" or "splice".
     *
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props, String type, boolean derbySpliceOnly) {
        Properties typedProps = new Properties();
        if (props != null) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); )
            {
                Map.Entry prop = (Map.Entry) iter.next();
                String key = (String) prop.getKey();
                if (derbySpliceOnly && key != null) {
                    String lowerKey = key.toLowerCase();
                    if (!lowerKey.startsWith("derby") && !lowerKey.startsWith("splice")) {
                        continue;
                    }
                }
                String[] typedValue = new String[2];
                typedValue[0] = (String) prop.getValue();
                typedValue[1] = type;
                typedProps.put(key, typedValue);
            }
        }
        return typedProps;
    }

    /**
     * Tag each property value with a 'type' of SERVICE or DATABASE.  The structure of the map changes from:
     *     {key --> value}
     * to:
     *     {key --> [value, type]}
     *
     * @param props  Map of properties to tag with type
     *
     * @return the new map of typed properties
     */
    private static Properties addTypeToProperties(Properties props) {
        Properties typedProps = new Properties();
        if (props != null) {
            for (Iterator iter = props.entrySet().iterator(); iter.hasNext(); )
            {
                Map.Entry prop = (Map.Entry) iter.next();
                String key = (String) prop.getKey();
                String[] typedValue = new String[2];
                typedValue[0] = (String) prop.getValue();
                typedValue[1] = PropertyUtil.isServiceProperty(key) ? "SERVICE" : "DATABASE";
                typedProps.put(key, typedValue);
            }
        }
        return typedProps;
    }

    private static final String sqlConglomsInSchema =
		"SELECT C.CONGLOMERATENUMBER FROM SYS.SYSCONGLOMERATES C, SYS.SYSTABLES T, SYS.SYSSCHEMAS S " +
        "WHERE T.TABLEID = C.TABLEID AND T.SCHEMAID = S.SCHEMAID AND S.SCHEMANAME = ?";

    private static final String sqlConglomsInTable =
    	sqlConglomsInSchema + " AND T.TABLENAME = ?";
    
    public static String getSqlConglomsInSchema() {
    	return sqlConglomsInSchema;
    }
    
    public static String getSqlConglomsInTable() {
    	return sqlConglomsInTable;
    }
    
    /**
     * Be Careful when using this, as it will return conglomerate ids for all the indices of a table
     * as well as the table itself. While the first conglomerate SHOULD be the main table, there
     * really isn't a guarantee, and it shouldn't be relied upon for correctness in all cases.
     */
    public static long[] getConglomNumbers(Connection conn, String schemaName, String tableName) throws SQLException {
        List<Long> conglomIDs = new ArrayList<Long>();
        if (schemaName == null)
            // default schema
            schemaName = SpliceConstants.SPLICE_USER;

        String query;
        boolean isTableNameEmpty;

        if (tableName == null) {
            query = getSqlConglomsInSchema();
            isTableNameEmpty = true;
        } else {
            query = getSqlConglomsInTable();
            isTableNameEmpty = false;
        }

        ResultSet rs = null;
        PreparedStatement s = null;
        try {
            s = conn.prepareStatement(query);
            s.setString(1, schemaName.toUpperCase());
            if (!isTableNameEmpty) {
                s.setString(2, tableName.toUpperCase());
            }
            rs = s.executeQuery();
            while (rs.next()) {
                conglomIDs.add(rs.getLong(1));
            }

            if (conglomIDs.isEmpty()) {
                if (isTableNameEmpty) {
                    throw PublicAPI.wrapStandardException(ErrorState.LANG_SCHEMA_DOES_NOT_EXIST.newException(schemaName));
                }
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            }
        } finally {
            if (rs != null) rs.close();
            if (s != null) s.close();
        }
        if (conglomIDs.isEmpty()) {
            return new long[0];
        }
        long[] congloms = new long[conglomIDs.size()];
        for (int i =0; i<conglomIDs.size(); i++) {
            congloms[i] = conglomIDs.get(i);
        }
				/*
				 * An index conglomerate id can be returned by the query before the main table one is,
				 * but it should ALWAYS have a higher conglomerate id, so if we sort the congloms,
				 * we should return the main table before any of its indices.
				 */
        Arrays.sort(congloms);
        return congloms;
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


    public static void SYSCS_GET_GLOBAL_DATABASE_PROPERTY(final String key, final ResultSet[] resultSet) throws SQLException {
    	operate(new JMXServerOperation() {
    		@Override
    		public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
    			try {
    				ResultSetBuilder rsBuilder = new ResultSetBuilder();
    				rsBuilder.getColumnBuilder()
	    				.addColumn("HOST_NAME",      Types.VARCHAR, 32)
	    				.addColumn("PROPERTY_VALUE", Types.VARCHAR, 128);
    				RowBuilder rowBuilder = rsBuilder.getRowBuilder();
    				int hostIdx = 0;
    				for (DatabasePropertyManagement databasePropertyMgmt : JMXUtils.getDatabasePropertyManagement(connections)) {
    					rowBuilder.getDvd(0).setValue(connections.get(hostIdx).getFirst());
    					rowBuilder.getDvd(1).setValue(databasePropertyMgmt.getDatabaseProperty(key));
    					rowBuilder.addRow();
    					hostIdx++;
    				}
    				resultSet[0] = rsBuilder.buildResultSet((EmbedConnection)getDefaultConn());
    			} catch (StandardException se) {
    				throw PublicAPI.wrapStandardException(se);
    			}
    		}
    	});
    }

    public static void SYSCS_SET_GLOBAL_DATABASE_PROPERTY(final String key, final String value) throws SQLException {
    	operate(new JMXServerOperation() {
    		@Override
    		public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
    			for (DatabasePropertyManagement databasePropertyMgmt : JMXUtils.getDatabasePropertyManagement(connections)) {
    				databasePropertyMgmt.setDatabaseProperty(key, value);
    			}
    		}
    	});
    }

    public static void SYSCS_EMPTY_GLOBAL_STATEMENT_CACHE() throws SQLException {
    	// This procedure is essentially a wrapper around the Derby stored proc SYSCS_EMPTY_STATEMENT_CACHE
    	// such that it is done on every node in the cluster.
    	operate(new JMXServerOperation() {
    		@Override
    		public void operate(List<Pair<String, JMXConnector>> connections) throws MalformedObjectNameException, IOException, SQLException {
                List<Pair<String, StatementManagement>> statementManagers = JMXUtils.getStatementManagers(connections);
                for (Pair<String, StatementManagement> managementPair : statementManagers) {
                    managementPair.getSecond().emptyStatementCache();
    			}
    		}
    	});
    }

    public static void GET_ACTIVATION(final String statement, final ResultSet[] resultSet) throws SQLException, StandardException {
        LanguageConnectionContext lcc = ConnectionUtil.getCurrentLCC();
        GenericPreparedStatement gps = (GenericPreparedStatement)lcc.prepareInternalStatement(statement);
        GenericActivationHolder activationHolder = (GenericActivationHolder)gps.getActivation(lcc, false);
        Activation activation = activationHolder.ac;
        SpliceOperation operation = (SpliceOperation)activation.execute();
        ActivationContext activationContext = ActivationContext.create(activation, operation);
        TriggerExecutionStack triggerExecutionStack = null;
        if (lcc.hasTriggers()) {
            triggerExecutionStack = lcc.getTriggerStack();
        }

        SpliceObserverInstructions soi = new SpliceObserverInstructions(
                (GenericStorablePreparedStatement) activation.getPreparedStatement(),
                operation,
                activationContext,
                lcc.getSessionUserId(),
                lcc.getDefaultSchema(),
                triggerExecutionStack);

        byte[] soiBytes = SerializationUtils.serialize(soi);
        DataValueDescriptor[] dvds = new DataValueDescriptor[] {
                new SQLBlob()
        };
        int numCols = dvds.length;
        ExecRow dataTemplate = new ValueRow(numCols);
        dataTemplate.setRowArray(dvds);

        List<ExecRow> rows = Lists.newArrayList();
        dvds[0].setValue(soiBytes);
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
}
