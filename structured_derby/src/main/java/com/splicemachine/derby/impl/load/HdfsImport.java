package com.splicemachine.derby.impl.load;

import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.sql.execute.operations.ParallelVTI;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.DataHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.job.JobFuture;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.ColumnDescriptor;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.tools.run;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.*;
import java.util.*;
import java.util.concurrent.CancellationException;
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
		private final ImportContext context;
		private static final int COLNULLABLE_POSITION = 11;
		private static final int COLSIZE_POSITION = 7;
		private static final int COLNUM_POSITION = 17;
		private static final int DECIMALDIGITS_POSIITON = 9;
		private static final int COLUMNDEFAULT_POSIITON = 13;
		private static final int ISAUTOINCREMENT_POSIITON = 23;
		private static final String AUTOINCREMENT_PREFIX = "AUTOINCREMENT: start ";
		private final long statementId;
		private final long operationId;
		private HBaseAdmin admin;

		public HdfsImport(ImportContext context,long statementId, long operationId){
				this.context = context;
				this.statementId = statementId;
				this.operationId = operationId;
		}

		public static void SYSCS_IMPORT_DATA(String schemaName, String tableName,
																				 String insertColumnList,
																				 String columnIndexes,
																				 String fileName, String columnDelimiter,
																				 String characterDelimiter,
																				 String timestampFormat,
																				 String dateFormat,
																				 String timeFormat) throws SQLException {
				Connection conn = SpliceAdmin.getDefaultConn();
				try {
						LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
						final String user = lcc.getSessionUserId();
						final String transactionId = SpliceObserverInstructions.getTransactionId(lcc);
						try {
								if(schemaName==null)
										schemaName = "APP";
								if(tableName==null)
										throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException("NULL"));
								importData(transactionId, user,conn, schemaName.toUpperCase(), tableName.toUpperCase(),
												insertColumnList, fileName, columnDelimiter,
												characterDelimiter, timestampFormat,dateFormat,timeFormat,lcc);
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

		public static ResultSet importData(String transactionId, String user,
																			 Connection connection,
																			 String schemaName,String tableName,
																			 String insertColumnList,String inputFileName,
																			 String delimiter,String charDelimiter,String timestampFormat,
																			 String dateFormat,String timeFormat,
																			 LanguageConnectionContext lcc) throws SQLException{
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

						if(lcc.getRunTimeStatisticsMode()){
								String xplainSchema = lcc.getXplainSchema();
								if(xplainSchema!=null)
										builder = builder.recordStats().xplainSchema(xplainSchema);
						}

						buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder,lcc);
				}catch(AssertionError ae){
						//the input data is bad in some way
						throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ID_PARSE_ERROR,ae.getMessage()));
				}

				long conglomId = SpliceAdmin.getConglomid(connection, schemaName, tableName);
				builder = builder.destinationTable(conglomId);

				HdfsImport importer;
				StatementInfo statementInfo = new StatementInfo(String.format("import(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
								schemaName,tableName,insertColumnList,inputFileName,delimiter,charDelimiter,timestampFormat,dateFormat,timeFormat),
								user,transactionId,
								1,SpliceDriver.driver().getUUIDGenerator());
				OperationInfo opInfo = new OperationInfo(
								SpliceDriver.driver().getUUIDGenerator().nextUUID(), statementInfo.getStatementUuid(),"Import", false, -1l);
				statementInfo.setOperationInfo(Arrays.asList(opInfo));

				SpliceDriver.driver().getStatementManager().addStatementInfo(statementInfo);
				try {
						importer = new HdfsImport(builder.build(),statementInfo.getStatementUuid(),opInfo.getOperationUuid());
						importer.open();
						SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext();
						runtimeContext.setStatementInfo(statementInfo);
						importer.executeShuffle(runtimeContext);
				} catch(AssertionError ae){
						throw PublicAPI.wrapStandardException(Exceptions.parseException(ae));
				} catch(StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}catch(CancellationException ce){
						throw PublicAPI.wrapStandardException(Exceptions.parseException(ce));
				}finally{
						String xplainSchema = lcc.getXplainSchema();
						boolean explain = xplainSchema !=null &&
										lcc.getRunTimeStatisticsMode();
						SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,explain? xplainSchema: null);
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
		public KeyEncoder getKeyEncoder(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				throw new UnsupportedOperationException();
		}

		@Override
		public DataHash getRowHash(SpliceRuntimeContext spliceRuntimeContext) throws StandardException {
				throw new UnsupportedOperationException();
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
		public void executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
				ImportFile file = new ImportFile(context.getFilePath().toString());
				byte[] tableName = context.getTableName().getBytes();
				try {
						splitToFit(tableName,file);
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				HTableInterface table = SpliceAccessManager.getHTable(tableName);

				List<Pair<JobFuture,JobInfo>> jobFutures = Lists.newArrayList();
				StatementInfo statementInfo = runtimeContext.getStatementInfo();
				Set<OperationInfo> opInfos = statementInfo.getOperationInfo();
				OperationInfo opInfo = null;
				//noinspection LoopStatementThatDoesntLoop
				for(OperationInfo opInfoField:opInfos){
						opInfo = opInfoField;
						break;
				}
				try {
						LOG.info("Importing files "+ file.getPaths());
						ImportJob importJob = new FileImportJob(table,context,statementId,file.getPaths(),operationId);
						long start = System.currentTimeMillis();
						JobFuture jobFuture = SpliceDriver.driver().getJobScheduler().submit(importJob);
						JobInfo info = new JobInfo(importJob.getJobId(),jobFuture.getNumTasks(),start);
						info.tasksRunning(jobFuture.getAllTaskIds());
						if(opInfo!=null)
								opInfo.addJob(info);
						jobFutures.add(Pair.newPair(jobFuture,info));

						try{
								jobFuture.completeAll(info);
						}catch(ExecutionException e){
								info.failJob();
								throw e;
						}
				} catch (InterruptedException e) {
						throw Exceptions.parseException(e);
				} catch (ExecutionException e) {
						throw Exceptions.parseException(e.getCause());
				} // still need to cancel all other jobs ? // JL
				catch (IOException e) {
						throw Exceptions.parseException(e);
				} finally{
						Closeables.closeQuietly(table);
						for(Pair<JobFuture,JobInfo> future:jobFutures){
								try {
										future.getFirst().cleanup();
								} catch (ExecutionException e) {
										LOG.error("Exception cleaning up import future",e);
								}
						}
				}
		}

		private void splitToFit(byte[] tableName, ImportFile file) throws IOException {
				/*
				 * We want to avoid situations where we have only a single region for a table
				 * whenever it is possible.
				 *
				 * Generally speaking, we don't want to split tables if there is already data
				 * present, or if the table has already been split a bunch of times. We also
				 * don't want to split the table if we aren't going to import very much data.
				 *
				 * This leads to a few heuristics for determining when a table should be split
				 * before importing:
				 *
				 * 1. Don't split if the total amount of data to be imported fits within
				 * a fixed number of regions (configurable). The default should be two or three
				 * regions.
				 * 2. Don't split if there are already splits present on the table.
				 *
				 * When it is decided that the table should be pre-split, the split is determined
				 * by the "importSizeRatio", which is the ratio of on-disk size to encoded size.
				 * In most situations, this ratio is pretty close to 1:1. HOWEVER, we have no
				 * information about the distribution of data within this file, so we are forced
				 * to assume uniformity (which is almost never true). Because of this, if we
				 * allow a 1:1 ratio, then when we actually insert data, we will likely end up
				 * with some regions with very little (if any) data in them, and some regions
				 * will be forced to split anyway.
				 *
				 * There really isn't anything we can do about this, except to not split as much.
				 * To attempt to minimize any excess regions due to poor distribution of data, we
				 * assume the size ratio is much smaller than it likely is. It's configurable, but
				 * should be somewhere in the range of 1:2 or 1:3--That is, 1 GB of data in Splice
				 * is equivalent to 2 or 3 GB of data in HDFS. This is HIGHLY unlikely to be true,
				 * but it will result in the creation of many fewer regions than might otherwise happen,
				 * which will help to avoid having straggler regions which must be cleaned up
				 * after the fact.
				 */
				long onDiskSize = file.getTotalLength();
				long regionSize = Long.parseLong(SpliceConstants.config.get(HConstants.HREGION_MAX_FILESIZE));
				int regionSplitFactor = SpliceConstants.importSplitFactor;

				long spliceSize = onDiskSize*regionSplitFactor;
				int numRegions = (int)(spliceSize/regionSize);

				if(numRegions<regionSplitFactor){
						//we have too little data to bother splitting
						return;
				}
				//we should split, but only if it hasn't already split
				List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
				if(tableRegions.size()>0) return;

				//initiate splits until we have reached numRegions
				for(int i=0;i<numRegions;i++){
						try {
								admin.split(tableName);
						} catch (InterruptedException e) {
								throw new IOException(e);
						}
				}
				//wait for all the splits to complete, but only for so long
				//we don't want to cause an infinite loop if something goes goofy
				//wait a total of 10*500 ms = 5 seconds
				int waitCount = 10;
				do{
						try {
								Thread.sleep(500);
						} catch (InterruptedException e) {
								throw new IOException(e);
						}
				}while((tableRegions = admin.getTableRegions(tableName)).size()<numRegions && (waitCount--)>0);

				/*
				 * HBase will be inclined to put all the newly split regions onto a single server.
				 * This doesn't help us distribute the load. Move them around to be as even as possible.
				 * If you don't specify a location, the admin will move it randomly, which is good enough
				 */
				byte[] destServerName = {};
				for(HRegionInfo info:tableRegions){
						admin.move(info.getEncodedNameAsBytes(), destServerName);
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

		@Override
		public long getStatementId() {
				return statementId;
		}

		//no-op
		@Override public void setStatementId(long statementId) {
			throw new UnsupportedOperationException("HdfsImport does not support mutating statment ids");
		}

		@Override public void setCurrentRowLocation(RowLocation rowLocation) { }

		@Override
		public String getName() {
				return "Import("+context.getFilePath().toString()+"->"+context.getTableName()+")";
		}

		@Override public boolean shouldRecordStats() { return false; }
		@Override public String getXplainSchema() { return null; }

		/************************************************************************************************************/
	/*private helper functions*/

//		private ImportJob getImportJob(HTableInterface table,CompressionCodecFactory codecFactory,Path file) throws StandardException {
////				CompressionCodec codec = codecFactory.getCodec(file);
//				ImportJob importJob;
//				/*
//				 * (December, 2013) We are disabling BlockImports for the time being because
//				 * they are error-prone and difficult to test at scale, and you can get nearly
//				 * as good of parallelism and performance from just dumping a bunch of files into
//				 * a single directory and running the import against the entire directory.
//				 *
//				 * In a couple of months, when we have a clearer need for that import
//				 * process as opposed to the more stable File import process, then we
//				 * can reopen this issue.
//				 */
////        if(codec==null ||codec instanceof SplittableCompressionCodec){
////            try{
////                importJob = new BlockImportJob(table, context);
////            }catch(IOException ioe){
////                throw Exceptions.parseException(ioe);
////            }
////        }else
//				importJob = new FileImportJob(table,context,statementId,operationId);
//				return importJob;
//		}

		private static void buildColumnInformation(Connection connection, String schemaName, String tableName,
																							 String insertColumnList, ImportContext.Builder builder,
																							 LanguageConnectionContext lcc) throws SQLException {
				DatabaseMetaData dmd = connection.getMetaData();
				Map<String,ColumnContext.Builder> columns = getColumns(schemaName==null?"APP":schemaName.toUpperCase(),tableName.toUpperCase(),insertColumnList,dmd);

				//TODO -sf- this invokes an additional scan--is there any way that we can avoid this?
				DataDictionary dataDictionary = lcc.getDataDictionary();
				TransactionController tc = lcc.getTransactionExecute();
				try {
						SchemaDescriptor sd = dataDictionary.getSchemaDescriptor(schemaName, tc,true);
						if(sd==null)
								throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName));
						TableDescriptor td = dataDictionary.getTableDescriptor(tableName,sd, tc);
						if(td==null)
								throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
						RowLocation[] rowLocations = dataDictionary.computeAutoincRowLocations(tc, td);

						for(ColumnContext.Builder cb:columns.values()){
							if(cb.isAutoIncrement()){
									cb.sequenceRowLocation(rowLocations[cb.getColumnNumber()].getBytes());
							}
						}
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}

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
				if(colType== Types.CHAR||colType==Types.VARCHAR||colType==Types.LONGVARCHAR||colType == Types.DECIMAL){
						int colSize = rs.getInt(COLSIZE_POSITION);
						colBuilder.length(colSize);
				}
				if (colType == Types.DECIMAL)
				{
						int decimalDigits = rs.getInt(DECIMALDIGITS_POSIITON);
						colBuilder.decimalDigits(decimalDigits);
				}
				/*
				 * The COLUMNDEFAULT position contains two separate entities: a default value (if the column
				 * has a default) or a String that looks like AUTOINCREMENT: start x increment y. We need to
				 * deal with each case separately
				 */
				String colDefault = rs.getString(COLUMNDEFAULT_POSIITON);
				String isAutoIncrement = rs.getString(ISAUTOINCREMENT_POSIITON);
				boolean hasIncrementPrefix = colDefault!=null && colDefault.startsWith(AUTOINCREMENT_PREFIX);
				if (isAutoIncrement.compareTo("YES") != 0 || !hasIncrementPrefix) {
						colBuilder.columnDefault(colDefault);
				}else if (hasIncrementPrefix){
						//colDefault looks like "AUTOINCREMENT: start x increment y
						colDefault = colDefault.substring(colDefault.indexOf('s'));
						int endIndex = colDefault.indexOf(' ', 6);
						long startVal = Long.parseLong(colDefault.substring(6, endIndex).trim());
						colDefault = colDefault.substring(endIndex);
						long incVal = Long.parseLong(colDefault.substring(10).trim());
						colBuilder.autoIncrementStart(startVal).autoIncrementIncrement(incVal);
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

}
