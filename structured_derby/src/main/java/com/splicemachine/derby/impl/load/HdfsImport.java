package com.splicemachine.derby.impl.load;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.utils.file.DefaultFileInfo;
import com.splicemachine.utils.file.FileInfo;
import org.apache.derby.iapi.error.PublicAPI;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.sql.dictionary.DataDictionary;
import org.apache.derby.iapi.sql.dictionary.SchemaDescriptor;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.TransactionController;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLLongint;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.jdbc.EmbedConnection;
import org.apache.derby.impl.jdbc.EmbedResultSet40;
import org.apache.derby.impl.sql.GenericColumnDescriptor;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ErrorState;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.IteratorNoPutResultSet;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;

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
public class HdfsImport {
		private static final Logger LOG = Logger.getLogger(HdfsImport.class);
		private final ImportContext context;
		private final long statementId;
		private final long operationId;
		private HBaseAdmin admin;

		public HdfsImport(ImportContext context,long statementId, long operationId){
				this.context = context;
				this.statementId = statementId;
				this.operationId = operationId;
		}

		@SuppressWarnings("UnusedDeclaration")
		public static void SYSCS_GET_AUTO_INCREMENT_ROW_LOCATIONS(String schemaName,String tableName,ResultSet[] resultSets) throws SQLException{
				Connection conn = SpliceAdmin.getDefaultConn();
				try{
						LanguageConnectionContext lcc = conn.unwrap(EmbedConnection.class).getLanguageConnection();
						DataDictionary dd = lcc.getDataDictionary();
						SchemaDescriptor sd = dd.getSchemaDescriptor(schemaName,lcc.getTransactionExecute(),true);
						if(sd==null)
								throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName);

						TableDescriptor td = dd.getTableDescriptor(tableName,sd,lcc.getTransactionExecute());
						if(td==null)
								throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName+"."+tableName);

						RowLocation[] rowLocations = dd.computeAutoincRowLocations(lcc.getTransactionExecute(), td);
						ExecRow template = new ValueRow(1);
						template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
						List<ExecRow> rows = Lists.newArrayList();
						if(rowLocations!=null){
								for(RowLocation location:rowLocations){
										template.resetRowArray();
										if(location!=null){
												byte[] loc = location.getBytes();
												template.getColumn(1).setValue(BytesUtil.toHex(loc));
										}
										rows.add(template.getClone());
								}
						}
						ResultColumnDescriptor[] rcds = new ResultColumnDescriptor[]{
										new GenericColumnDescriptor("location", DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
						};

						IteratorNoPutResultSet inprs = new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
						inprs.openCore();
						resultSets[0] = new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				} finally{
						try{
								conn.close();
						}catch(SQLException se){
								LOG.error("Unable to close default connection",se);
						}
				}
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
			IMPORT_DATA(schemaName,tableName,insertColumnList,fileName,columnDelimiter,characterDelimiter,timestampFormat,dateFormat,timeFormat,-1,null,new ResultSet[1]);
		}

		private static final ResultColumnDescriptor[] IMPORT_RESULT_COLUMNS = new GenericColumnDescriptor[]{
						new GenericColumnDescriptor("numFiles",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
						new GenericColumnDescriptor("numTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
						new GenericColumnDescriptor("numRowsImported",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
						new GenericColumnDescriptor("numBadRecords",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
		};
		public static void IMPORT_DATA(String schemaName, String tableName,
																				 String insertColumnList,
																				 String fileName,
																				 String columnDelimiter,
																				 String characterDelimiter,
																				 String timestampFormat,
																				 String dateFormat,
																				 String timeFormat,
																				 long maxBadRecords,
																				 String badRecordDirectory,
																				 ResultSet[] results
																				 ) throws SQLException {
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
								ExecRow resultRow = importData(transactionId, user, conn, schemaName.toUpperCase(), tableName.toUpperCase(),
												insertColumnList, fileName, columnDelimiter,
												characterDelimiter, timestampFormat, dateFormat, timeFormat, lcc, maxBadRecords, badRecordDirectory);
								EmbedConnection embedConnection = (EmbedConnection)conn;
								Activation activation = embedConnection.getLanguageConnection().getLastActivation();
								IteratorNoPutResultSet rs = new IteratorNoPutResultSet(Arrays.asList(resultRow),IMPORT_RESULT_COLUMNS,activation);
								rs.open();
								results[0] = new EmbedResultSet40(embedConnection,rs,false,null,true);

						}catch (StandardException e) {
								throw PublicAPI.wrapStandardException(e);
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

		public static ExecRow importData(String transactionId, String user,
																			 Connection connection,
																			 String schemaName,
																			 String tableName,
																			 String insertColumnList,
																			 String inputFileName,
																			 String delimiter,
																			 String charDelimiter,
																			 String timestampFormat,
																			 String dateFormat,
																			 String timeFormat,
																			 LanguageConnectionContext lcc,
																			 long maxBadRecords,
																			 String badRecordDirectory) throws SQLException{
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
										.transactionId(transactionId)
										.maxBadRecords(maxBadRecords)
										.badLogDirectory(badRecordDirectory==null?null: new Path(badRecordDirectory))
						;

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
						SpliceRuntimeContext runtimeContext = new SpliceRuntimeContext();
						runtimeContext.setStatementInfo(statementInfo);
						return importer.executeShuffle(runtimeContext);
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

		}


		public ExecRow executeShuffle(SpliceRuntimeContext runtimeContext) throws StandardException {
				try {
						admin = new HBaseAdmin(SpliceUtils.config);
				} catch (MasterNotRunningException e) {
						throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
				} catch (ZooKeeperConnectionException e) {
						throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
				}
				try{
						ImportFile file = new ImportFile(context.getFilePath().toString());
						FileSystem fileSystem = file.getFileSystem();
						//make sure that we can read and write as needed
						ImportUtils.validateReadable(context.getFilePath(),fileSystem,false);
						ImportUtils.validateReadable(file);
						ImportUtils.validateWritable(context.getBadLogDirectory(),fileSystem,true);

						byte[] tableName = context.getTableName().getBytes();
						try {
								splitToFit(tableName,file);
						} catch (IOException e) {
								throw Exceptions.parseException(e);
						}
						HTableInterface table = SpliceAccessManager.getHTable(SpliceDriver.driver().getTempTable().getTempTableName());

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
								JobStats jobStats = jobFuture.getJobStats();
								List<TaskStats> taskStats = jobStats.getTaskStats();
								long numImported = 0l;
								long numBadRecords = 0l;
								for(TaskStats stats:taskStats){
										long totalRowsWritten = stats.getTotalRowsWritten();
										long totalRead = stats.getTotalRowsProcessed();
										numImported+= totalRowsWritten;
										numBadRecords+=(totalRead-totalRowsWritten);
								}
								ExecRow result = new ValueRow(3);
								result.setRowArray(new DataValueDescriptor[]{
												new SQLInteger(file.getPaths().size()),
												new SQLInteger(jobFuture.getJobStats().getNumTasks()),
												new SQLLongint(Math.max(0,numImported)),
												new SQLLongint(numBadRecords)
								});
								return result;
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
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				} finally{
						if(admin!=null)
								Closeables.closeQuietly(admin);
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

	/*One-line public methods*/

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
						long conglomerateId = td.getHeapConglomerateId();
						builder.destinationTable(conglomerateId);
						RowLocation[] rowLocations = dataDictionary.computeAutoincRowLocations(tc, td);
						byte[][] rowLocBytes;
						if(rowLocations!=null){
								rowLocBytes = new byte[rowLocations.length][];
								for(int i=0;i<rowLocations.length;i++){
										if(rowLocations[i]!=null)
												rowLocBytes[i] = rowLocations[i].getBytes();
								}
						}else
							rowLocBytes = null;

						ImportUtils.buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder,rowLocBytes);
				} catch (StandardException e) {
						throw PublicAPI.wrapStandardException(e);
				}
		}

}
