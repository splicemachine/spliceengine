package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.ParallelVTI;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.Path;
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
	private final Path filePath;
	private final String delimiter;
	private final int[] columnTypes;
	private final FormatableBitSet activeCols;
	private final long conglomId;
	private HBaseAdmin admin;

	public static ResultSet importData(Connection connection,
								String schemaName,String tableName,
								String insertColumnList,String inputFileName,
								String delimiter) throws SQLException{
		if(connection ==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
		if(tableName==null)
			throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));

		FormatableBitSet activeCols = new FormatableBitSet();

		int[] columnTypes = pushColumnInformation(connection,schemaName,tableName,insertColumnList,activeCols);

		long conglomId = getConglomid(connection,tableName);
		HdfsImport importer = new HdfsImport(inputFileName,conglomId,delimiter,columnTypes,activeCols);
		try {
			importer.open();
			importer.executeShuffle();
		} catch (StandardException e) {
			throw PublicAPI.wrapStandardException(e);
		}

		return importer;
	}
	
	public HdfsImport(String filePath, long conglomId,String delimiter,
										int[] columnTypes,FormatableBitSet activeCols){
		this.filePath = new Path(filePath);
		this.delimiter = delimiter;
		this.columnTypes = columnTypes;
		this.activeCols = activeCols;
		this.conglomId = conglomId;
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
		CompressionCodec codec = codecFactory.getCodec(filePath);
		Importer importer;
		if(codec==null ||codec instanceof SplittableCompressionCodec){
			importer = new SplitImporter(admin,conglomId,activeCols,columnTypes,delimiter,filePath);
		}else{
			importer = new SequentialImporter(admin,conglomId,activeCols,columnTypes,delimiter,filePath);
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
	@Override public long sink() { return 0; }
	@Override public ExecRow getExecRowDefinition() { return null; }
	@Override public boolean next() { return false; }
	@Override public SpliceOperation getRightOperation() { return null; } //noop
	@Override public void generateRightOperationStack(boolean initial, List<SpliceOperation> operations) {  }

/************************************************************************************************************/

	/*private helper functions*/
	private static long getConglomid(Connection conn, String tableName) throws SQLException {
		/*
		 * gets the conglomerate id for the specified human-readable table name
		 *
		 * TODO -sf- make this a stored procedure?
		 */
		ResultSet rs = null;
		PreparedStatement s = null;
		try{
			s = conn.prepareStatement(
					"select " +
						"conglomeratenumber " +
					"from " +
						"sys.sysconglomerates c," +
						"sys.systables t " +
					"where " +
						"t.tableid = c.tableid " +
						"and t.tablename = ?");
			s.setString(1,tableName);

			rs = s.executeQuery();
			if(rs.next()){
				return rs.getLong(1);
			}else{
				throw new SQLException("No Conglomerate id found for table name "+tableName);
			}
		}finally{
			if(rs!=null) rs.close();
			if(s!=null) s.close();
		}
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
		List<String> insertCols = Lists.newArrayList(Splitter.on(",").trimResults().split(insertColumnList));
		while(rs.next()){

			String colName = rs.getString(COLNAME_POSITION);
			int colIndex = rs.getInt(COLNUM_POSITION);
			indexTypeMap.put(colIndex-1, rs.getInt(COLTYPE_POSITION));
			Iterator<String> colIter = insertCols.iterator();
			while(colIter.hasNext()){
				String insertCol = colIter.next();
				if(insertCol.equalsIgnoreCase(colName)){
					activeAccumulator.grow(colIndex);
					activeAccumulator.set(colIndex-1);
					colIter.remove();
					break;
				}
			}
		}
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
