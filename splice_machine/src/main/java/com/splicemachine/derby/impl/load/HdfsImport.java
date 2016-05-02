package com.splicemachine.derby.impl.load;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.GenericColumnDescriptor;
import com.splicemachine.db.impl.sql.execute.IteratorNoPutResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.management.OperationInfo;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.SpliceAdmin;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.JobStatusLogger;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.util.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Imports a delimiter-separated file located in HDFS in a parallel way.
 * <p/>
 * When importing data which is contained in HDFS, there is an inherent disconnect
 * between the data locality of any normal file in HDFS, and the data locality of the
 * individual region servers.
 * <p/>
 * <p>Under normal HBase circumstances, one would use HBase's provided bulk-import
 * capabilities, which uses MapReduce to align HFiles with HBase's location and then loads
 * them in one single go. This won't work in Splice's case, however, because each insertion
 * needs to update Secondary indices, validate constraints, and so on and so forth which
 * are not executed when bulk-loading HFiles.
 * <p/>
 * <p>Thus, we must parallelize insertions as much as possible, while still maintaining
 * as much data locality as possible. However, it is not an inherent given that any
 * block location has a corresponding region, nor is it given that any given RegionServer
 * has blocks contained on it. To make matters worse, when a RegionServer <em>does</em>
 * have blocks contained on it, there is no guarantee that the data in those blocks
 * is owned by that specific RegionServer.
 * <p/>
 * <p>There isn't a perfect solution to this problem, unfortunately. This implementation
 * favors situations in which a BlockLocation is co-located with a Region; as a consequence,
 * pre-splitting a Table into regions and spreading those regions out across the cluster is likely
 * to improve the performance of this import process.
 *
 * @author Scott Fines
 */
public class HdfsImport{
    private static final Logger LOG=Logger.getLogger(HdfsImport.class);

    private final ImportContext context;
    private final long statementId;
    private final long operationId;
    private HBaseAdmin admin;
    private final String qualifiedTableName;

    public HdfsImport(ImportContext context,String qualifiedTableName,long statementId,long operationId){
        this.context=context;
        this.statementId=statementId;
        this.operationId=operationId;
        this.qualifiedTableName=qualifiedTableName;
    }

    @SuppressWarnings("UnusedDeclaration")
    public static void SYSCS_GET_AUTO_INCREMENT_ROW_LOCATIONS(String schemaName,String tableName,ResultSet[] resultSets) throws SQLException{
        Connection conn=SpliceAdmin.getDefaultConn();
        try{
            LanguageConnectionContext lcc=conn.unwrap(EmbedConnection.class).getLanguageConnection();
            DataDictionary dd=lcc.getDataDictionary();
            SchemaDescriptor sd=dd.getSchemaDescriptor(schemaName,lcc.getTransactionExecute(),true);
            if(sd==null)
                throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName);

            TableDescriptor td=dd.getTableDescriptor(tableName,sd,lcc.getTransactionExecute());
            if(td==null)
                throw ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName+"."+tableName);

            RowLocation[] rowLocations=dd.computeAutoincRowLocations(lcc.getTransactionExecute(),td);
            ExecRow template=new ValueRow(1);
            template.setRowArray(new DataValueDescriptor[]{new SQLVarchar()});
            List<ExecRow> rows=Lists.newArrayList();
            if(rowLocations!=null){
                for(RowLocation location : rowLocations){
                    template.resetRowArray();
                    if(location!=null){
                        byte[] loc=location.getBytes();
                        template.getColumn(1).setValue(BytesUtil.toHex(loc));
                    }
                    rows.add(template.getClone());
                }
            }
            ResultColumnDescriptor[] rcds=new ResultColumnDescriptor[]{
                    new GenericColumnDescriptor("location",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.VARCHAR))
            };

            IteratorNoPutResultSet inprs=new IteratorNoPutResultSet(rows,rcds,lcc.getLastActivation());
            inprs.openCore();
            resultSets[0]=new EmbedResultSet40(conn.unwrap(EmbedConnection.class),inprs,false,null,true);
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }finally{
            try{
                conn.close();
            }catch(SQLException se){
                LOG.error("Unable to close default connection",se);
            }
        }
    }

    private static final ResultColumnDescriptor[] IMPORT_RESULT_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("numFiles",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("numTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("numRowsImported",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("numBadRecords",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    private static final ResultColumnDescriptor[] CHECK_RESULT_COLUMNS=new GenericColumnDescriptor[]{
            new GenericColumnDescriptor("numFiles",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("numTasks",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.INTEGER)),
            new GenericColumnDescriptor("numRowsCheckedOK",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT)),
            new GenericColumnDescriptor("numBadRecords",DataTypeDescriptor.getBuiltInDataTypeDescriptor(Types.BIGINT))
    };

    @SuppressWarnings("UnusedParameters")
    public static void SYSCS_IMPORT_DATA(String schemaName,String tableName,
                                         String insertColumnList,
                                         String columnIndexes,
                                         String fileName,String columnDelimiter,
                                         String characterDelimiter,
                                         String timestampFormat,
                                         String dateFormat,
                                         String timeFormat) throws SQLException{
        IMPORT_DATA(schemaName,tableName,
                insertColumnList,
                fileName,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                -1,
                null,
                new ResultSet[1]);
    }

    public static void UPSERT_DATA_FROM_FILE(String schemaName,String tableName,
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
    ) throws SQLException{
        doImport(schemaName,
                tableName,
                insertColumnList,
                fileName,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                maxBadRecords,
                badRecordDirectory,
                -1,
                true,
                false,
                results);
    }

    public static void UPSERT_CHECK_DATA_FROM_FILE(String schemaName,String tableName,
                                                   String insertColumnList,
                                                   String fileName,
                                                   String columnDelimiter,
                                                   String characterDelimiter,
                                                   String timestampFormat,
                                                   String dateFormat,
                                                   String timeFormat,
                                                   long maxBadRecords,
                                                   String badRecordDirectory,
                                                   long maxRecords,
                                                   ResultSet[] results
    ) throws SQLException{
        doImport(schemaName,
                tableName,
                insertColumnList,
                fileName,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                maxBadRecords,
                badRecordDirectory,
                maxRecords,
                true,
                true,
                results);
    }

    public static void IMPORT_DATA(String schemaName,String tableName,
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
    ) throws SQLException{
        doImport(schemaName,
                tableName,
                insertColumnList,
                fileName,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                maxBadRecords,
                badRecordDirectory,
                -1,
                false,
                false,
                results);
    }

    public static void IMPORT_CHECK_DATA(String schemaName,String tableName,
                                         String insertColumnList,
                                         String fileName,
                                         String columnDelimiter,
                                         String characterDelimiter,
                                         String timestampFormat,
                                         String dateFormat,
                                         String timeFormat,
                                         long maxBadRecords,
                                         String badRecordDirectory,
                                         long maxRecords,
                                         ResultSet[] results
    ) throws SQLException{
        doImport(schemaName,
                tableName,
                insertColumnList,
                fileName,
                columnDelimiter,
                characterDelimiter,
                timestampFormat,
                dateFormat,
                timeFormat,
                maxBadRecords,
                badRecordDirectory,
                maxRecords,
                false,
                true,
                results);
    }

    private static void doImport(String schemaName,String tableName,
                                 String insertColumnList,
                                 String fileName,
                                 String columnDelimiter,
                                 String characterDelimiter,
                                 String timestampFormat,
                                 String dateFormat,
                                 String timeFormat,
                                 long maxBadRecords,
                                 String badRecordDirectory,
                                 long maxRecords,
                                 boolean isUpsert,
                                 boolean isCheckScan,
                                 ResultSet[] results) throws SQLException{
        Connection conn=SpliceAdmin.getDefaultConn();
        try{
            LanguageConnectionContext lcc=conn.unwrap(EmbedConnection.class).getLanguageConnection();
            final String user=lcc.getSessionUserId();
            Activation activation=lcc.getLastActivation();
            BaseSpliceTransaction txn=((SpliceTransactionManager)activation.getTransactionController()).getRawTransaction();
            try{
                if(schemaName==null)
                    schemaName=SpliceConstants.SPLICE_USER;
                if(tableName==null)
                    throw PublicAPI.wrapStandardException(ErrorState.TABLE_NAME_CANNOT_BE_NULL.newException());

                EmbedConnection embedConnection=(EmbedConnection)conn;
                ExecRow resultRow=importData(txn,user,conn,schemaName.toUpperCase(),tableName.toUpperCase(),
                        insertColumnList,fileName,columnDelimiter,
                        characterDelimiter,timestampFormat,dateFormat,timeFormat,lcc,maxBadRecords,badRecordDirectory,
                        maxRecords,isUpsert,isCheckScan);
                IteratorNoPutResultSet rs=
                        new IteratorNoPutResultSet(Arrays.asList(resultRow),
                                (isCheckScan?CHECK_RESULT_COLUMNS:IMPORT_RESULT_COLUMNS),activation);
                rs.open();
                results[0]=new EmbedResultSet40(embedConnection,rs,false,null,true);

            }catch(StandardException e){
                throw PublicAPI.wrapStandardException(e);
            }
        }finally{
            try{
                if(conn!=null){
                    conn.close();
                }
            }catch(SQLException e){
                SpliceLogUtils.error(LOG,"Unable to close import connection",e);
            }
        }
    }

    private static long getFileSizeBytes(String filePath) throws SQLException{
        try{
            FileSystem fileSystem=FileSystem.get(SpliceUtils.config);
            Path path=new Path(filePath);
            return fileSystem.getContentSummary(path).getLength();
        }catch(IOException io){
            throw new SQLException(io);
        }
    }

    public static ExecRow importData(BaseSpliceTransaction txn,
                                     String user,
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
                                     String badRecordDirectory,
                                     long maxRecords,
                                     boolean upsert,
                                     boolean isCheckScan) throws SQLException{
        if(connection==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.CONNECTION_NULL));
        if(tableName==null)
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ENTITY_NAME_MISSING));
        ImportContext.Builder builder;
        try{
            builder=new ImportContext.Builder()
                    .path(inputFileName)
                    .stripCharacters(charDelimiter)
                    .colDelimiter(delimiter)
                    .timestampFormat(timestampFormat)
                    .dateFormat(dateFormat)
                    .timeFormat(timeFormat)
                    .maxBadRecords(maxBadRecords)
                    .badLogDirectory(badRecordDirectory==null?null:new Path(badRecordDirectory))
                    .maxRecords(maxRecords)
                    .upsert(upsert)
                    .checkScan(isCheckScan)
            ;

            if(lcc.getRunTimeStatisticsMode()){
                builder=builder.recordStats();
            }

            buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder,lcc,upsert);
        }catch(AssertionError ae){
            //the input data is bad in some way
            throw PublicAPI.wrapStandardException(StandardException.newException(SQLState.ID_PARSE_ERROR,ae.getMessage()));
        }

				/*
                 * Create a child transaction to actually perform the import under
				 */
        byte[] conglomBytes=Bytes.toBytes(Long.toString(builder.getDestinationConglomerate()));
        Txn parentTxn;
        try{
            parentTxn=((SpliceTransaction)txn).elevate(conglomBytes);
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }

        Txn childTransaction;
        try{
            childTransaction=TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn,Txn.IsolationLevel.SNAPSHOT_ISOLATION,true,conglomBytes);
        }catch(IOException e){
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }

        HdfsImport importer;
        StatementInfo statementInfo=new StatementInfo(String.format("import(%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                schemaName,tableName,insertColumnList,inputFileName,delimiter,charDelimiter,timestampFormat,dateFormat,timeFormat),
                user,childTransaction,
                1,SpliceDriver.driver().getUUIDGenerator());
        OperationInfo opInfo=new OperationInfo(
                SpliceDriver.driver().getUUIDGenerator().nextUUID(),statementInfo.getStatementUuid(),"Import",null,false,-1l);
        statementInfo.setOperationInfo(Arrays.asList(opInfo));

        SpliceDriver.driver().getStatementManager().addStatementInfo(statementInfo);
        boolean rollback=false;
        try{
            importer=new HdfsImport(builder.build(),schemaName+"."+tableName,statementInfo.getStatementUuid(),opInfo.getOperationUuid());
            SpliceRuntimeContext runtimeContext=new SpliceRuntimeContext(childTransaction);
            runtimeContext.setStatementInfo(statementInfo);
            return importer.executeShuffle(runtimeContext,childTransaction);
        }catch(AssertionError | CancellationException ae){
            rollback=true;
            throw PublicAPI.wrapStandardException(Exceptions.parseException(ae));
        }catch(StandardException e){
            rollback=true;
            throw PublicAPI.wrapStandardException(e);
        }finally{
            //put this stuff first to avoid a memory leak
            if(rollback){

                try{
                    SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,false,childTransaction);
                }catch(Exception e1){
                    LOG.error("Unable to complete failed statement in statement manager, but this is not fatal. We will still roll back transaction.",e1);
                }

                try{
                    childTransaction.rollback();
                }catch(IOException e){
										/*
										 * We were unable to rollback the transaction, which is bad.
										 *
										 * Unfortunately, we only tried to roll back because the import
										 * failed in some way anyway, which means that we don't want to make
										 * our failure to roll back the transaction the error that gets returned,
										 * so we can't throw directly, we'll just have to log and continue
										 * on
										 */
                    LOG.error("Unable to roll back child transaction, but don't want to swamp actual error causing rollback",e);
                }
            }else{
                reportStats(lcc,childTransaction,statementInfo);
                try{
                    childTransaction.commit();
                }catch(IOException e){
										/*
										 * We were unable to commit the transaction properly,
										 * so we have to give up and blow back on the client
										 */
                    LOG.error("Unable to commit import transaction after 5 attempts",e);
                    //noinspection ThrowFromFinallyBlock
                    throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
                }
            }
        }
    }

    private static void reportStats(LanguageConnectionContext lcc,Txn childTransaction,StatementInfo statementInfo) throws SQLException{
        boolean explain=lcc.getRunTimeStatisticsMode();
        try{
            SpliceDriver.driver().getStatementManager().completedStatement(statementInfo,explain,childTransaction);
        }catch(IOException e){
            try{
                childTransaction.rollback();
            }catch(IOException e1){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
            throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
        }catch(StandardException e){
            try{
                childTransaction.rollback();
            }catch(IOException e1){
                throw PublicAPI.wrapStandardException(Exceptions.parseException(e));
            }
            throw PublicAPI.wrapStandardException(e);
        }
    }


    public ExecRow executeShuffle(SpliceRuntimeContext runtimeContext,Txn txn) throws StandardException{
        try{
            admin=new HBaseAdmin(SpliceUtils.config);
        }catch(Exception e){
            throw StandardException.newException(SQLState.COMMUNICATION_ERROR,e);
        }
        try{
            ImportFile file=new ImportFile(context.getFilePath().toString());
            FileSystem fileSystem=file.getFileSystem();
            //make sure that we can read and write as needed
            ImportUtils.validateReadable(context.getFilePath(),fileSystem,false);
            ImportUtils.validateReadable(file);
            ImportUtils.validateWritable(context.getBadLogDirectory(),fileSystem,true);
            byte[] tableName=context.getTableName().getBytes();
            try{
                splitToFit(tableName,file);
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
            HTableInterface table=SpliceAccessManager.getHTable(SpliceDriver.driver().getTempTable().getTempTableName());

            List<Pair<JobFuture, ImportJobInfo>> jobFutures=Lists.newArrayList();
            JobStatusLogger jobStatusLogger=getJobStatusLogger();
            StatementInfo statementInfo=runtimeContext.getStatementInfo();
            Set<OperationInfo> opInfos=statementInfo.getOperationInfo();
            OperationInfo opInfo=null;
            //noinspection LoopStatementThatDoesntLoop
            for(OperationInfo opInfoField : opInfos){
                opInfo=opInfoField;
                break;
            }

            ImportJobInfo info=null;
            long numImported=0l;
            long numBadRecords=0l;
						/*
						 * Are we checking the import files or actually importing them?
						 * Set the verb appropriately for all of our logging.
						 */
            String logVerb=(context.isCheckScan()?"check":"import");

            try{
                LOG.info(String.format("%sing files %s",logVerb,file.getPaths()));
                ImportJob importJob=new FileImportJob(table,context,statementId,file.getPaths(),operationId,txn);
                long start=System.currentTimeMillis();
                JobFuture jobFuture=SpliceDriver.driver().getJobScheduler().submit(importJob);
                info=new ImportJobInfo(importJob.getJobId(),jobFuture,start,jobStatusLogger,context);
                long estImportTime=estimateImportTime();
                jobStatusLogger.log(String.format("Expected time for %s is %s.  Expected finish is %s.",
                        logVerb,StringUtils.formatTime(estImportTime),new Date(System.currentTimeMillis()+estImportTime)));
                jobStatusLogger.log(String.format("%sing %d files...",WordUtils.capitalize(logVerb),jobFuture.getNumTasks()));
                jobStatusLogger.log(String.format("Task status update interval is %s.",StringUtils.formatTime(SpliceConstants.importTaskStatusLoggingInterval)));
                info.tasksRunning(jobFuture.getAllTaskIds());

                statementInfo.addRunningJob(operationId, info);
                jobFuture.addCleanupTask(StatementInfo.completeOnClose(statementInfo, info));

//                if(opInfo!=null)
//                    opInfo.initializeJob(info);
                jobFutures.add(Pair.newPair(jobFuture,info));

                info.logStatusOfImportFiles(jobFuture.getNumTasks(),jobFuture.getRemainingTasks());
                try{
                    jobFuture.completeAll(info);
                }catch(ExecutionException e){
                    info.failJob();
                    throw e;
                }
                JobStats jobStats=jobFuture.getJobStats();
                List<TaskStats> taskStats=jobStats.getTaskStats();
                for(TaskStats stats : taskStats){
                    long totalRowsWritten=stats.getTotalRowsWritten();
                    long totalRead=stats.getTotalRowsProcessed();
                    numImported+=totalRowsWritten;
                    numBadRecords+=(totalRead-totalRowsWritten);
                }
                jobStatusLogger.log(String.format(
                        "%s finished with success. Total rows %sed%s: %,d. Total rows rejected: %,d. Total time for %s: %s.",
                        WordUtils.capitalize(logVerb),logVerb,(context.isCheckScan()?" OK":""),numImported,numBadRecords,logVerb,StringUtils.formatTimeDiff(info.getJobFinishMs(),info.getJobStartMs())));
                ExecRow result=new ValueRow(3);
                result.setRowArray(new DataValueDescriptor[]{
                        new SQLInteger(file.getPaths().size()),
                        new SQLInteger(jobFuture.getJobStats().getNumTasks()),
                        new SQLLongint(Math.max(0,numImported)),
                        new SQLLongint(numBadRecords)
                });
                return result;
            }catch(InterruptedException e){
                jobStatusLogger.log(String.format(
                        "%s has been interrupted. All imported rows will be rolled back. Total rows imported: %,d. Total rows rejected: %,d. Total time for import: %s.",
                        WordUtils.capitalize(logVerb),numImported,numBadRecords,(info==null?"Unknown":StringUtils.formatTimeDiff(info.getJobFinishMs(),info.getJobStartMs()))));
                throw Exceptions.parseException(e);
            }catch(ExecutionException e){
                Throwable cause=e.getCause();
                jobStatusLogger.log(String.format(
                        "%s has failed due to an execution error: %s. All imported rows will be rolled back. Total rows imported: %,d. Total rows rejected: %,d. Total time for import: %s.",
                        WordUtils.capitalize(logVerb),(cause==null?"Unknown":cause.getLocalizedMessage()),numImported,numBadRecords,(info==null?"Unknown":StringUtils.formatTimeDiff(info.getJobFinishMs(),info.getJobStartMs()))));
                throw Exceptions.parseException(cause);
            } // still need to cancel all other jobs ? // JL
            catch(IOException e){
                jobStatusLogger.log(String.format(
                        "%s has failed due to an I/O error: %s. All imported rows will be rolled back. Total rows imported: %,d. Total rows rejected: %,d. Total time for import: %s.",
                        WordUtils.capitalize(logVerb),(e==null?"Unknown":e.getLocalizedMessage()),numImported,numBadRecords,(info==null?"Unknown":StringUtils.formatTimeDiff(info.getJobFinishMs(),info.getJobStartMs()))));
                throw Exceptions.parseException(e);
            }finally{
                Closeables.closeQuietly(table);
                for(Pair<JobFuture, ImportJobInfo> future : jobFutures){
                    try{
                        future.getFirst().cleanup();
                    }catch(ExecutionException e){
                        LOG.error("Exception cleaning up import future",e);
                    }
                }
                info.cleanup();
                jobStatusLogger.closeLogFile();
            }
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }finally{
            if(admin!=null)
                Closeables.closeQuietly(admin);
        }
    }

    private JobStatusLogger getJobStatusLogger() throws IOException{
        if(SpliceConstants.enableImportStatusLogging){
            return new ImportJobStatusLogger(context,qualifiedTableName+"."+operationId);
        }else
            return NoopJobStatusLogger.INSTANCE;
    }

    /**
     * Return an estimate of the time required for the import job to finish.
     *
     * @return time estimate for the import job duration
     * @throws IOException
     */
    private long estimateImportTime() throws IOException{
	    	/*
	    	 * This is a very simple model to estimate the completion time of an import job based on
	    	 * the size of the data, calculated import ingestion rates, and the number of region servers.
	    	 *
	    	 * PLEASE NOTE: This is an incredibly basic estimation model.  Many things still need to be considered such as the following:
	    	 *   - number of indexes on the table that is being imported into
	    	 *   - number of import tasks
	    	 *   - total number of cores for the cluster
	    	 *   - general load on the cluster
	    	 *   - whether or not the files are compressed and which compression algorithm (gzip, pkzip, zip, etc.) is being used
	    	 *   - and also the fact that our import ingest rates fluctuate with region server activity such as compactions, region splits, etc.
	    	 * But hey, it's a start...
	    	 */

        // The following rates were empirically derived from SAP import timings.
        double importRatePerNode=1.9d*1024d*1024d/1000d;  // 1.9 MB/sec
        double standaloneBoost=1.0d;
        double checkScanBoost=1.0d;

        int numServers=getRegionServerCount();
        long importDataSize=getImportDataSize();

        // Standalone has a 25-50% higher import ingest rate since there is less contention and the disks are local (often they are flash too).
        if(numServers==1) standaloneBoost=1.3d;

        // Check scans are import jobs that only check the data and do not insert it into the database.
        // Check scans run in about 40% of the time of an actual import.
        if(context.isCheckScan()) checkScanBoost=0.4d;

        if(LOG.isDebugEnabled()) SpliceLogUtils.debug(LOG,
                "importDataSize(bytes)=%,d numServers=%d importRatePerNode(bytes/ms)=%,.2f standaloneBoost=%,.2f checkScanBoost=%,.2f",
                importDataSize,numServers,importRatePerNode,standaloneBoost,checkScanBoost);

        return (long)((double)importDataSize/((double)numServers*importRatePerNode*standaloneBoost)*checkScanBoost);
    }

    /**
     * Get the number of region servers in the cluster.
     *
     * @return number of region servers in the cluster
     * @throws IOException
     */
    private int getRegionServerCount() throws IOException{
        return SpliceUtilities.getAdmin().getClusterStatus().getServersSize();
    }

    /**
     * Return the total space consumed by the import data files.
     *
     * @return total space consumed by the import data files
     * @throws IOException
     */
    private long getImportDataSize() throws IOException{
        FileSystem fs=FileSystem.get(SpliceConstants.config);
        Path path=context.getFilePath();
        ContentSummary summary=fs.getContentSummary(path);
        if(LOG.isDebugEnabled()) SpliceLogUtils.debug(LOG,"Path=%s SpaceConsumed=%,d",path,summary.getSpaceConsumed());
        return summary.getSpaceConsumed();
    }

    private void splitToFit(byte[] tableName,ImportFile file) throws IOException{
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
        long onDiskSize=file.getTotalLength();
        long regionSize=SpliceConstants.config.getLong(HConstants.HREGION_MAX_FILESIZE,0);
        int regionSplitFactor=SpliceConstants.importSplitFactor;

        long spliceSize=onDiskSize*regionSplitFactor;
        int numRegions=(int)(spliceSize/regionSize);

        if(numRegions<regionSplitFactor){
            //we have too little data to bother splitting
            return;
        }
        //we should split, but only if it hasn't already split
        List<HRegionInfo> tableRegions=admin.getTableRegions(tableName);
        if(tableRegions.size()>0) return;

        //initiate splits until we have reached numRegions
        for(int i=0;i<numRegions;i++){
            try{
                admin.split(tableName);
            }catch(InterruptedException e){
                throw new IOException(e);
            }
        }
        //wait for all the splits to complete, but only for so long
        //we don't want to cause an infinite loop if something goes goofy
        //wait a total of 10*500 ms = 5 seconds
        int waitCount=10;
        do{
            try{
                Thread.sleep(500);
            }catch(InterruptedException e){
                throw new IOException(e);
            }
        }while((tableRegions=admin.getTableRegions(tableName)).size()<numRegions && (waitCount--)>0);

				/*
				 * HBase will be inclined to put all the newly split regions onto a single server.
				 * This doesn't help us distribute the load. Move them around to be as even as possible.
				 * If you don't specify a location, the admin will move it randomly, which is good enough
				 */
        byte[] destServerName={};
        for(HRegionInfo info : tableRegions){
            admin.move(info.getEncodedNameAsBytes(),destServerName);
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
    private static void buildColumnInformation(Connection connection,String schemaName,String tableName,
                                               String insertColumnList,ImportContext.Builder builder,
                                               LanguageConnectionContext lcc,boolean upsert) throws SQLException{
        //TODO -sf- this invokes an additional scan--is there any way that we can avoid this?
        DataDictionary dataDictionary=lcc.getDataDictionary();
        TransactionController tc=lcc.getTransactionExecute();
        try{
            SchemaDescriptor sd=dataDictionary.getSchemaDescriptor(schemaName,tc,true);
            if(sd==null)
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(schemaName));
            TableDescriptor td=dataDictionary.getTableDescriptor(tableName,sd,tc);
            if(td==null)
                throw PublicAPI.wrapStandardException(ErrorState.LANG_TABLE_NOT_FOUND.newException(tableName));
            long conglomerateId=td.getHeapConglomerateId();
            builder.tableVersion(td.getVersion());
            builder.destinationTable(conglomerateId);
            RowLocation[] rowLocations=dataDictionary.computeAutoincRowLocations(tc,td);
            byte[][] rowLocBytes;
            if(rowLocations!=null){
                rowLocBytes=new byte[rowLocations.length][];
                for(int i=0;i<rowLocations.length;i++){
                    if(rowLocations[i]!=null)
                        rowLocBytes[i]=rowLocations[i].getBytes();
                }
            }else
                rowLocBytes=null;

            ImportUtils.buildColumnInformation(connection,schemaName,tableName,insertColumnList,builder,rowLocBytes,upsert);
        }catch(StandardException e){
            throw PublicAPI.wrapStandardException(e);
        }
    }


}
