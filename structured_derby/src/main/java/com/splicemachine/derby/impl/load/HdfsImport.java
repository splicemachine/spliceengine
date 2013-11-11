package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ParallelVTI;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Imports a delimiter-separated file located in HDFS in a parallel way.
 * 
 * When importing data which is contained in HDFS, there is an inherent disconnect
 * between the data locality of any normal file in HDFS, and the data locality of the
 * individual region servers. 
 * 
 *  <p>Under normal HBase circumstances, one would use HBase's provided bulk-import 
 * capabilities, which uses MapReduce to align HFiles with HBase's location and then loads
 * them in one single go. This won't work in Splice's case, however, because each insertion
 * needs to update Secondary indices, validate constraints, and so on and so forth which
 * are not executed when bulk-loading HFiles. 
 * 
 * <p>Thus, we must parallelize insertions as much as possible, while still maintaining
 * as much data locality as possible. However, it is not an inherent given that any
 * block location has a corresponding region, nor is it given that any given RegionServer
 * has blocks contained on it. To make matters worse, when a RegionServer <em>does</em>
 * have blocks contained on it, there is no guarantee that the data in those blocks 
 * is owned by that specific RegionServer.
 * 
 * <p>There isn't a perfect solution to this problem, unfortunately. This implementation
 * favors situations in which a BlockLocation is co-located with a Region; as a consequence,
 * pre-splitting a Table into regions and spreading those regions out across the cluster is likely
 * to improve the performance of this import process.
 * 
 * @author Scott Fines
 *
 */
public class HdfsImport extends ParallelVTI {
	private static final Logger LOG = Logger.getLogger(HdfsImport.class);
	private static final int COLTYPE_POSITION = 5;
    private static final int COLNAME_POSITION = 4;
    private static final PathFilter hiddenFileFilter = new PathFilter(){
        public boolean accept(Path p){
            String name = p.getName();
            return !name.startsWith("_") && !name.startsWith(".");
        }
    };
    private final ImportContext context;
    private static final int COLNULLABLE_POSITION = 11;
    private static final int COLSIZE_POSITION = 7;
    private static final int COLNUM_POSITION = 17;
    private HBaseAdmin admin;

    public HdfsImport(ImportContext context){
        this.context = context;
    }

    /**
     * Allows for multiple input paths separated by commas
     * @param input the input path pattern
     */
    public static Path[] getInputPaths(String input) {
        String [] list = StringUtils.split(input);
        Path[] result = new Path[list.length];
        for (int i = 0; i < list.length; i++) {
            result[i] = new Path(StringUtils.unEscapeString(list[i]));
        }
        return result;
    }

    public static List<FileStatus> listStatus(String input) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(input);
        if (dirs.length == 0)
            throw new IOException("No Path Supplied in job");
        List<Path> errors = Lists.newArrayListWithExpectedSize(0);

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        filters.add(hiddenFileFilter);
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (Path p : dirs) {
            FileSystem fs = FileSystem.get(SpliceUtils.config);
            FileStatus[] matches = fs.globStatus(p, inputFilter);
            if (matches == null) {
                errors.add(p);
            } else if (matches.length == 0) {
                errors.add(p);
            } else {
                for (FileStatus globStat : matches) {
                    if (globStat.isDirectory()) {
                        Collections.addAll(result, fs.listStatus(globStat.getPath(), inputFilter));
                    } else {
                        result.add(globStat);
                    }
                }
            }
        }

        if (!errors.isEmpty()) {
            throw new FileNotFoundException(errors.toString());
        }
        LOG.info("Total input paths to process : " + result.size());
        return result;
    }

    @SuppressWarnings("UnusedParameters")
    public static void SYSCS_IMPORT_DATA(String schemaName, String tableName,
                                         String insertColumnList,
                                         String columnIndexes,
                                         String fileName, String columnDelimiter,
                                         String characterDelimiter,
                                         String timestampFormat,
                                         String dateFormat,
                                         String timeFormat) throws SQLException {
    	Connection conn = getDefaultConn();
        try {
            LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
            final String transactionId = SpliceObserverInstructions.getTransactionId(lcc);
            try {
                if(schemaName==null)
                    schemaName = "APP";
                if(tableName==null)
                    throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException("NULL"));
                importData(transactionId, conn, schemaName.toUpperCase(), tableName.toUpperCase(),
                        insertColumnList, fileName, columnDelimiter,
                        characterDelimiter, timestampFormat,dateFormat,timeFormat);
            } catch (SQLException se) {
                try {
                    conn.rollback();
                } catch (SQLException e) {
                    se.setNextException(e);
                }
                throw se;
            }

            conn.commit();
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                }
            } catch (SQLException e) {
                SpliceLogUtils.error(LOG, "Unable to close index connection", e);
            }
        }
    }

    public static ResultSet importData(String transactionId, Connection connection,
                                       String schemaName,String tableName,
                                       String insertColumnList,String inputFileName,
                                       String delimiter,String charDelimiter) throws SQLException{
        if(connection ==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
        if(tableName==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));

        ImportContext.Builder builder = new ImportContext.Builder()
				.path(inputFileName)
				.stripCharacters(charDelimiter)
				.colDelimiter(delimiter)
                .transactionId(transactionId);

		buildColumnInformation(connection,schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,builder);

		long conglomId = getConglomid(connection,tableName.toUpperCase(),schemaName.toUpperCase());
		builder = builder.destinationTable(conglomId);
        HdfsImport importer;
		try {
            importer = new HdfsImport(builder.build());
			importer.open();
			importer.executeShuffle();
		} catch (StandardException e) {
			throw PublicAPI.wrapStandardException(e);
		}

		return importer;
	}

	public static ResultSet importData(String transactionId, Connection connection,
								String schemaName,String tableName,
								String insertColumnList,String inputFileName,
								String delimiter,String charDelimiter,String timestampFormat,
                                String dateFormat,String timeFormat) throws SQLException{
		if(connection ==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
		if(tableName==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));
        ImportContext.Builder builder;
        try{
            builder = new ImportContext.Builder()
                    .path(inputFileName)
                    .stripCharacters(charDelimiter)
                    .colDelimiter(delimiter)
                    .timestampFormat(timestampFormat)
                    .dateFormat(dateFormat)
                    .timeFormat(timeFormat)
                    .transactionId(transactionId);


            buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder);
        }catch(AssertionError ae){
            //the input data is bad in some way
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ID_PARSE_ERROR,ae.getMessage()));
        }

		long conglomId = getConglomid(connection,tableName,schemaName);
		builder = builder.destinationTable(conglomId);
        HdfsImport importer;
		try {
            importer = new HdfsImport(builder.build());
			importer.open();
			importer.executeShuffle();
		} catch(AssertionError ae){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(ae));
        } catch(StandardException e) {
			throw PublicAPI.wrapStandardException(e);
		}

		return importer;
	}


	@Override
	public void open() throws StandardException {
		try {
			admin = new HBaseAdmin(SpliceUtils.config);
		} catch (MasterNotRunningException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		} catch (ZooKeeperConnectionException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		}
	}

    @Override
    public int[] getRootAccessedCols(long tableNumber) {
        throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement getRootAccessedCols");
    }

    @Override
    public boolean isReferencingTable(long tableNumber) {
        throw new UnsupportedOperationException("class "+ this.getClass()+" does not implement isReferencingTable");
    }

	@Override
	public void executeShuffle() throws StandardException {
		List<FileStatus> files;
		try {
			files = listStatus(context.getFilePath().toString());
		} catch (IOException e) {
			throw Exceptions.parseException(e);
		}
		HTableInterface table = SpliceAccessManager.getHTable(context.getTableName().getBytes());

		try {
            CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
            List<JobFuture> jobFutures = Lists.newArrayListWithCapacity(files.size());
            for (FileStatus file: files) {
                context.setFilePath(file.getPath());
                jobFutures.add(SpliceDriver.driver().getJobScheduler().submit(getImportJob(table,codecFactory,file)));
            }

			for (JobFuture jobFuture: jobFutures) {
					jobFuture.completeAll();
			}
		} catch (InterruptedException e) {
            throw Exceptions.parseException(e);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        } // still need to cancel all other jobs ? // JL
		finally{
            Closeables.closeQuietly(table);
        }
    }

    @Override
	public void close() {
		try{
			admin.close();
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
		}
	}

	/*One-line public methods*/

    //TODO -sf- make HdfsImport report how many rows were returned
	@Override public int modifiedRowCount() { return 0; }

    @Override
    public void clearCurrentRow() {
        //no-op
    }

    /*no op methods*/
	@Override public ExecRow getExecRowDefinition() { return null; }
	@Override public boolean next() { return false; }
    @Override public SpliceOperation getRightOperation() { return null; } //noop
    @Override public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {  }
    @Override public String prettyPrint(int indentLevel) { return "HdfsImport"; }
    @Override public void setCurrentRowLocation(RowLocation rowLocation) { }

    /************************************************************************************************************/
	/*private helper functions*/

    private ImportJob getImportJob(HTableInterface table,CompressionCodecFactory codecFactory,FileStatus file) throws StandardException {
        CompressionCodec codec = codecFactory.getCodec(file.getPath());
        ImportJob importJob;
        if(codec==null ||codec instanceof SplittableCompressionCodec){
            try{
                importJob = new BlockImportJob(table, context);
            }catch(IOException ioe){
                throw Exceptions.parseException(ioe);
            }
        }else
            importJob = new FileImportJob(table,context);
        return importJob;
    }
    private static Connection getDefaultConn() throws SQLException {
        InternalDriver id = InternalDriver.activeDriver();
        if(id!=null){
            Connection conn = id.connect("jdbc:default:connection",null);
            if(conn!=null)
                return conn;
        }
        throw Util.noCurrentConnection();
    }

    private static long getConglomid(Connection conn, String tableName, String schemaName) throws SQLException {
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

    private static void buildColumnInformation(Connection connection, String schemaName, String tableName,
                                               String insertColumnList, ImportContext.Builder builder) throws SQLException {
        DatabaseMetaData dmd = connection.getMetaData();
        Map<String,ColumnContext.Builder> columns = getColumns(schemaName==null?"APP":schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,dmd);

        Map<String,Integer> pkCols = getPrimaryKeys(schemaName, tableName, dmd);
        int[] pkKeyMap = new int[columns.size()];
        Arrays.fill(pkKeyMap,-1);
        LOG.info("columns="+columns);
        LOG.info("pkCols="+pkCols);
        for(String pkCol:pkCols.keySet()){
            columns.get(pkCol).primaryKeyPos(pkCols.get(pkCol));
        }

        for(ColumnContext.Builder colBuilder:columns.values()){
            builder.addColumn(colBuilder.build());
        }
    }

    private static Map<String,ColumnContext.Builder> getColumns(String schemaName, String tableName,
                                                                String insertColumnList, DatabaseMetaData dmd) throws SQLException{
        ResultSet rs = null;
        Map<String,ColumnContext.Builder> columnMap = Maps.newHashMap();
        try{
            rs = dmd.getColumns(null,schemaName,tableName,null);
            if(insertColumnList!=null && !insertColumnList.equalsIgnoreCase("null")){
                List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
                while(rs.next()){
                    ColumnContext.Builder colBuilder = buildColumn(rs);
                    String colName = colBuilder.getColumnName();
                    Iterator<String> colIterator = insertCols.iterator();
                    while(colIterator.hasNext()){
                        String insertCol = colIterator.next();
                        if(insertCol.equalsIgnoreCase(colName)){
                            columnMap.put(rs.getString(4),colBuilder);
                            colIterator.remove();
                            break;
                        }
                    }
                }
            }else{
                while(rs.next()){
                    ColumnContext.Builder colBuilder = buildColumn(rs);

                    columnMap.put(colBuilder.getColumnName(),colBuilder);
                }
            }
            return columnMap;
        }finally{
            if(rs!=null)rs.close();
        }

    }

    private static ColumnContext.Builder buildColumn(ResultSet rs) throws SQLException {
        ColumnContext.Builder colBuilder = new ColumnContext.Builder();
        String colName = rs.getString(COLNAME_POSITION);
        colBuilder.columnName(colName);
        int colPos = rs.getInt(COLNUM_POSITION);
        colBuilder.columnNumber(colPos-1);
        int colType = rs.getInt(COLTYPE_POSITION);
        colBuilder.columnType(colType);
        boolean isNullable = rs.getInt(COLNULLABLE_POSITION)!=0;
        colBuilder.nullable(isNullable);
        if(colType== Types.CHAR||colType==Types.VARCHAR||colType==Types.LONGVARCHAR){
            int colSize = rs.getInt(COLSIZE_POSITION);
            colBuilder.length(colSize);
        }
        return colBuilder;
    }

    private static Map<String,Integer> getPrimaryKeys(String schemaName, String tableName,
                                                      DatabaseMetaData dmd) throws SQLException {
        //get primary key information
        ResultSet rs = null;
        try{
            rs = dmd.getPrimaryKeys(null,schemaName,tableName.toUpperCase());
            Map<String,Integer> pkCols = Maps.newHashMap();
            while(rs.next()){
                /*
                 * The column number of use is the KEY_SEQ field in the returned result,
                 * which is one-indexed. For convenience, we adjust it to be zero-indexed here.
                 */
                pkCols.put(rs.getString(4), rs.getShort(5) - 1);
            }
            return pkCols;
        }finally{
            if(rs!=null)rs.close();
        }
    }

    private static class MultiPathFilter implements PathFilter {
        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }
}
