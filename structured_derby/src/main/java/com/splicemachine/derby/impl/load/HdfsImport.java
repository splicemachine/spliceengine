package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ParallelVTI;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.stats.ThroughputStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.impl.jdbc.Util;
import org.apache.derby.jdbc.InternalDriver;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.SplittableCompressionCodec;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
	private static int COLNUM_POSITION = 17;

	private long insertedRowCount=0;
	private final ImportContext context;
	private HBaseAdmin admin;

    public static void SYSCS_IMPORT_DATA(String schemaName,String tableName,
                                              String insertColumnList, String columnIndexes,
                                              String fileName,String columnDelimiter,
                                              String characterDelimiter,
                                              String timestampFormat) throws SQLException{
        Connection conn = getDefaultConn();
        try{
            importData(conn,schemaName,tableName,
                    insertColumnList,fileName,columnDelimiter,
                    characterDelimiter,timestampFormat);
        }catch(SQLException se){
           try{
               conn.rollback();
           }catch(SQLException e){
               se.setNextException(e);
           }
            throw se;
        }

        conn.commit();
    }


    public static ResultSet importData(Connection connection,
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
				.colDelimiter(delimiter);

		buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder);

		long conglomId = getConglomid(connection,tableName,schemaName);
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

	public static ResultSet importData(Connection connection,
								String schemaName,String tableName,
								String insertColumnList,String inputFileName,
								String delimiter,String charDelimiter,String timestampFormat) throws SQLException{
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
                    .timestampFormat(timestampFormat);


		buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder);
        }catch(AssertionError ae){
            //the input data is bad in some way
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ID_PARSE_ERROR,ae.getMessage()));
        }

		long conglomId = getConglomid(connection,tableName,schemaName);
		builder = builder.destinationTable(conglomId);
        HdfsImport importer = null;
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

	public HdfsImport(ImportContext context){
		this.context = context;
	}

	@Override
	public void openCore() throws StandardException {
		try {
			admin = new HBaseAdmin(SpliceUtils.config);
		} catch (MasterNotRunningException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		} catch (ZooKeeperConnectionException e) {
			throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
		}
	}

	@Override
	public void executeShuffle() throws StandardException {
		CompressionCodecFactory codecFactory = new CompressionCodecFactory(SpliceUtils.config);
		CompressionCodec codec = codecFactory.getCodec(context.getFilePath());
		Importer importer;
		if(codec==null ||codec instanceof SplittableCompressionCodec){
			importer = new SplitImporter(admin,context);
		}else{
			importer = new SequentialImporter(admin,context);
		}

		try {
			insertedRowCount = importer.importData();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrow(LOG,StandardException.newException(SQLState.UNEXPECTED_IMPORT_ERROR,e));
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
	@Override public int modifiedRowCount() { return (int)insertedRowCount; }
	@Override public void open() throws StandardException { openCore(); }

	/*no op methods*/
	@Override public SinkStats sink() { return null; }
	@Override public ExecRow getExecRowDefinition() { return null; }
	@Override public boolean next() { return false; }
    @Override public SpliceOperation getRightOperation() { return null; } //noop
    @Override public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {  }

    /************************************************************************************************************/

	/*private helper functions*/

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
            s.setString(2,tableName);
            rs = s.executeQuery();
            if(rs.next()){
                return rs.getLong(1);
            }else{
                throw new SQLException(String.format("No Conglomerate id found for table [%s] in schema [%s] ",tableName,schemaName.toUpperCase()));
            }
        }finally{
            if(rs!=null) rs.close();
            if(s!=null) s.close();
        }
    }

    private static void buildColumnInformation(Connection connection, String schemaName, String tableName,
                                               String insertColumnList, ImportContext.Builder builder) throws SQLException {
        DatabaseMetaData dmd = connection.getMetaData();
        ResultSet rs = null;
        int numCols = 0;
        try{
            rs = dmd.getColumns(null,(schemaName==null?"APP":schemaName.toUpperCase()),tableName,null);
            if(insertColumnList!=null){
                List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
                while(rs.next()){
                    numCols++;
                    String colName = rs.getString(COLNAME_POSITION);
                    Iterator<String> colIterator = insertCols.iterator();
                    while(colIterator.hasNext()){
                        String insertCol = colIterator.next();
                        if(insertCol.equalsIgnoreCase(colName)){
                            builder = builder.column(rs.getInt(COLNUM_POSITION)-1,rs.getInt(COLTYPE_POSITION));
                            colIterator.remove();
                            break;
                        }
                    }
                }
                builder = builder.numColumns(numCols);
            }else{
                while(rs.next()){
                    numCols++;
                    builder = builder.column(rs.getInt(COLNUM_POSITION)-1,rs.getInt(COLTYPE_POSITION));
                }
                builder.numColumns(numCols);
            }
        }finally{
            if(rs!=null)rs.close();
        }

        //get primary key information
        try{
            rs = dmd.getPrimaryKeys(null,schemaName,tableName.toUpperCase());
            FormatableBitSet pkCols = new FormatableBitSet(numCols);
            while(rs.next()){
                /*
                 * The column number of use is the KEY_SEQ field in the returned result,
                 * which is one-indexed. For convenience, we adjust it to be zero-indexed here.
                 */
                pkCols.set(rs.getShort(5)-1);
            }
            if(pkCols.getNumBitsSet()>0){
                builder.primaryKeys(pkCols);
            }
        }finally{
            if(rs!=null)rs.close();
        }
	}

	private static int[] pushColumnInformation(Connection connection,
																						 String schemaName,String tableName) throws SQLException{

		/*
		 * Gets the column information for the specified table via standard JDBC
		 */
		DatabaseMetaData dmd = connection.getMetaData();

		//this will cause shit to break
		ResultSet rs = dmd.getColumns(null,schemaName,tableName,null);
		Map<Integer,Integer> indexTypeMap = new HashMap<Integer,Integer>();
		while(rs.next()){
			int colIndex = rs.getInt(COLNUM_POSITION);
			indexTypeMap.put(colIndex-1,rs.getInt(COLTYPE_POSITION));
		}

		return toIntArray(indexTypeMap);
	}

	private static int[] pushColumnInformation(Connection connection,
								String schemaName,String tableName,String insertColumnList,
								FormatableBitSet activeAccumulator) throws SQLException{
		/*
		 * Gets the column information for the specified table via standard JDBC
		 */
		DatabaseMetaData dmd = connection.getMetaData();

		//this will cause shit to break
		ResultSet rs = dmd.getColumns(null,schemaName,tableName,null);
		Map<Integer,Integer> indexTypeMap = new HashMap<Integer,Integer>();
		List<String> insertCols = null;
		if(insertColumnList!=null)
			insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
		while(rs.next()){
			String colName = rs.getString(COLNAME_POSITION);
			int colIndex = rs.getInt(COLNUM_POSITION);
			indexTypeMap.put(colIndex-1, rs.getInt(COLTYPE_POSITION));
			LOG.trace("found column "+colName+" in position "+ colIndex);
			if(insertColumnList!=null){
				Iterator<String> colIter = insertCols.iterator();
				while(colIter.hasNext()){
					String insertCol = colIter.next();
					if(insertCol.equalsIgnoreCase(colName)){
						LOG.trace("column "+ colName+" requested matches, adding");
						activeAccumulator.grow(colIndex);
						activeAccumulator.set(colIndex-1);
						colIter.remove();
						break;
					}
				}
			}
		}

		return toIntArray(indexTypeMap);
	}

	private static int[] toIntArray(Map<Integer, Integer> indexTypeMap) {
		int[] retArray = new int[indexTypeMap.size()];
		for(int i=0;i<retArray.length;i++){
			Integer next = indexTypeMap.get(i);
			if(next!=null)
				retArray[i] = next;
			else
				retArray[i] = -1; //shouldn't happen, but you never know

		}
		return retArray;
	}


}
