package com.splicemachine.derby.impl.load.spark;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.*;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionOperations;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.uuid.UUIDGenerator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.supercsv.prefs.CsvPreference;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.ExecutionException;

/**
 * Created by dgomezferro on 7/15/15.
 */
public class ImportFunction extends
        SpliceFlatMapFunction<SpliceOperation,Iterator<Tuple2<String,InputStream>>,StandardException> implements Externalizable {

    private static final Logger LOG = Logger.getLogger(ImportFunction.class);

    protected ImportContext importContext;
    protected FileSystem fileSystem;
    protected SpliceCsvReader reader;

    private Importer importer;

    protected byte[] taskId;
    protected TaskStatus status;
    private long logRowCountInterval = SpliceConstants.importTaskStatusReportingRowCount;   // The row count interval of when to log rows read.  For example, write to the log every 10,000 rows read.
    private long rowsRead = 0l;
    private long previouslyLoggedRowsRead = 0l;  // The number of rows read that was last logged.
    private ImportErrorReporter errorReporter;
    private TxnView parentTxn;
    private Txn txn;

    public ImportFunction(){

    }

    public ImportFunction(ImportContext importContext, Txn txn) {
        this.importContext = importContext;
        this.parentTxn = txn;
    }

    @Override
    public Iterable<StandardException> call(Iterator<Tuple2<String, InputStream>> iterator) throws Exception {
        if (!iterator.hasNext()) {
            return Collections.emptyList();
        }
        while(iterator.hasNext()) {
            taskId = Bytes.toBytes(new Random().nextLong());
            Tuple2<String, InputStream> tuple = iterator.next();
            String path = tuple._1();
            InputStream inputStream = tuple._2();
            reader = getCsvReader(new InputStreamReader(inputStream), importContext);
            fileSystem = FileSystem.get(URI.create(path), SpliceConstants.config);
            try {
                ExecRow row = getExecRow(importContext);

                rowsRead = 0l;
                previouslyLoggedRowsRead = 0l;  // The number of rows read that was last logged.
                long stopTime;
                Timer totalTimer = importContext.shouldRecordStats() ? Metrics.newTimer() : Metrics.noOpTimer();
                totalTimer.startTiming();
                long startTime = System.currentTimeMillis();
                RowErrorLogger errorLogger = getErrorLogger();
                errorReporter = getErrorReporter(row.getClone(), errorLogger);
                long maxRecords = importContext.getMaxRecords();  // The maximum number of records to import or check in the file.

                try {
                    errorLogger.open();
                    if (importer == null) {

                        importer = getImporter(row, errorReporter);
                    }

                    try {
                        boolean shouldContinue;
                        do {
                            SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);
                            String[] lines = reader.readAsStringArray();

                            shouldContinue = importer.processBatch(lines);
                            rowsRead++;

                            if ((rowsRead - previouslyLoggedRowsRead) >= logRowCountInterval) {
                                previouslyLoggedRowsRead = rowsRead;
                                if (LOG.isDebugEnabled()) {
                                    SpliceLogUtils.debug(LOG, "Imported %d total rows.  Rejected %d total bad rows.  File is %s.", (rowsRead - errorReporter.errorsReported()), errorReporter.errorsReported(), path);
                                }
                            }

                            if (maxRecords > 0 && rowsRead >= maxRecords) {
                                shouldContinue = false;
                            }

                        } while (shouldContinue);

                        previouslyLoggedRowsRead = rowsRead;
                    } catch (StandardException e) {
                        return Arrays.asList(e);
                    } catch (Exception e) {
                        throw new ExecutionException(e);
                    } finally {
                        Closeables.closeQuietly(reader);
                        /*
                         * We don't call closeQuietly(importer) here, because
                         * we need to make sure that we get out any IOExceptions
                         * that get thrown
                         */
                        try {
                            importer.close();
                            importer = null;
                        } finally {
                            //close error reporter AFTER importer finishes
                            Closeables.closeQuietly(errorReporter);
                            Closeables.closeQuietly(errorLogger);
                            Closeables.closeQuietly(inputStream);
                        }
                        TransactionLifecycle.getLifecycleManager().commit(txn.getTxnId());
                        stopTime = System.currentTimeMillis();
                    }
                    totalTimer.stopTiming();
                    //                TaskStats stats = new TaskStats(stopTime-startTime,rowsRead,
                    //                        rowsRead-errorReporter.errorsReported());
                } catch (Exception e) {
                    throw new ExecutionException(e);
                }
            } catch (StandardException e) {
                return Arrays.asList(e);
            }
        }
        return Collections.emptyList();
    }

    public static ExecRow getExecRow(ImportContext context) throws StandardException {
        ColumnContext[] cols = context.getColumnInformation();
        int size = cols[cols.length-1].getColumnNumber()+1;
        ExecRow row = new ValueRow(size);
        for(ColumnContext col:cols){
            DataValueDescriptor dataValueDescriptor = getDataValueDescriptor(col);
            row.setColumn(col.getColumnNumber()+1, dataValueDescriptor);
        }
        return row;
    }

    public static DataValueDescriptor getDataValueDescriptor(ColumnContext columnContext) throws StandardException {
        DataTypeDescriptor td = DataTypeDescriptor.getBuiltInDataTypeDescriptor(columnContext.getColumnType());
        if(!columnContext.isNullable())
            td = td.getNullabilityType(false);
        return td.getNull();
    }

    protected ImportErrorReporter getErrorReporter(ExecRow rowTemplate,RowErrorLogger errorLogger) {
        long maxBadRecords = importContext.getMaxBadRecords();
        if(maxBadRecords<0) return FailAlwaysReporter.INSTANCE;

        KVPair.Type importType = importContext.isUpsert()? KVPair.Type.UPSERT: KVPair.Type.INSERT;
        PairDecoder decoder = ImportUtils.newEntryEncoder(rowTemplate,importContext,getUuidGenerator(),importType).getDecoder(rowTemplate);

        long queueSize = maxBadRecords;
        if(maxBadRecords==0|| maxBadRecords > SpliceConstants.importLogQueueSize)
            queueSize = SpliceConstants.importLogQueueSize;

        QueuedErrorReporter delegate = new QueuedErrorReporter( (int)queueSize,
                SpliceConstants.importLogQueueWaitTimeMs, errorLogger, decoder);
				/*
				 * When maxBadRecords = 0, then we want to log everything and never fail. This is to match
				 * Oracle etc.'s behavior (and thus be more like what people expect). Otherwise, we have a maximum
				 * threshold that we need to adhere to.
				 */
        if(maxBadRecords==0)
            return delegate;
        else
            return new ThresholdErrorReporter(maxBadRecords, delegate);
    }

    protected UUIDGenerator getUuidGenerator() {
        return SpliceDriver.driver().getUUIDGenerator().newGenerator(128);
    }

    protected RowErrorLogger getErrorLogger() throws StandardException {
        if(importContext.getMaxBadRecords()<0)
            return NoopErrorLogger.INSTANCE;

				/*
				 * Made protected so that it can be easily overridden for testing.
				 */
        Path directory = importContext.getBadLogDirectory();
        if(directory==null){
            directory = importContext.getFilePath().getParent();
            //make sure that we can write to this directory, otherwise it's no good either
            ImportUtils.validateWritable(directory,fileSystem,false);
        }

        Path badLogFile = new Path(directory,"_BAD_"+importContext.getFilePath().getName()+"_"+Bytes.toLong(taskId));

        return new FileErrorLogger(fileSystem,badLogFile,128);
    }

    protected Importer getImporter(ExecRow row,ImportErrorReporter errorReporter) throws ExecutionException {
//        boolean shouldParallelize = false;
//        try {
//            shouldParallelize = reader.shouldParallelize(fileSystem, importContext);
//        } catch (IOException e) {
//            throw new ExecutionException(e);
//        }

        try {
            txn = beginChildTransaction(parentTxn, TransactionLifecycle.getLifecycleManager());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        if(LOG.isInfoEnabled())
            SpliceLogUtils.info(LOG,"Importing %s using transaction %s, which is a child of transaction %s",
                    reader.toString(),txn,txn.getParentTxnView());
        KVPair.Type importType = importContext.isUpsert()? KVPair.Type.UPSERT: KVPair.Type.INSERT;
//        if(!shouldParallelize) {
            return new SequentialImporter(importContext,row, txn,
                    SpliceDriver.driver().getTableWriter(),errorReporter,importType);
//        } else
//            return new ParallelImporter(importContext,row,
//                    SpliceConstants.maxImportProcessingThreads,
//                    SpliceConstants.maxImportReadBufferSize,txn,errorReporter,importType);
    }

    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {

        byte[] table = importContext.getTableName().getBytes();
        /*
         * We use an additive transaction structure here so that two separate files
         * in the same import process will not throw Write/Write conflicts with one another,
         * but will instead by passed through to the underlying constraint (that way, we'll
         * get UniqueConstraint violations instead of Write/Write conflicts).
         */
        return tc.beginChildTransaction(parentTxn, Txn.IsolationLevel.SNAPSHOT_ISOLATION, true, table);
    }


    public ImportContext getContext() {
        return importContext;
    }

    private SpliceCsvReader getCsvReader(Reader reader, ImportContext importContext) {
        return new SpliceCsvReader(
                reader,
                new CsvPreference.Builder(
                        importContext.getQuoteChar().charAt(0),
                        importContext.getColumnDelimiter().charAt(0),
                        "\n",
                        SpliceConstants.importMaxQuotedColumnLines).useNullForEmptyColumns(false).build());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(importContext);
        TransactionOperations.getOperationFactory().writeTxn(parentTxn, out);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        importContext = (ImportContext) in.readObject();
        parentTxn = TransactionOperations.getOperationFactory().readTxn(in);
    }
}
