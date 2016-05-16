package com.splicemachine.derby.stream.spark;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class InsertDataSetWriter<K,V> implements DataSetWriter{
    private JavaPairRDD<K, V> rdd;
    private OperationContext<? extends SpliceOperation> opContext;
    private Configuration config;
    private int[] pkCols;
    private String tableVersion;
    private ExecRow execRowDefinition;
    private RowLocation[] autoIncRowArray;
    private SpliceSequence[] sequences;
    private long heapConglom;
    private boolean isUpsert;
    private TxnView txn;
    private static final Logger LOG = Logger.getLogger(InsertDataSetWriter.class);

    public InsertDataSetWriter(){
    }

    public InsertDataSetWriter(JavaPairRDD<K, V> rdd,
                               OperationContext<? extends SpliceOperation> opContext,
                               Configuration config,
                               int[] pkCols,
                               String tableVersion,
                               ExecRow execRowDefinition,
                               RowLocation[] autoIncRowArray,
                               SpliceSequence[] sequences,
                               long heapConglom,
                               boolean isUpsert){
        this.rdd=rdd;
        this.opContext=opContext;
        this.config=config;
        this.pkCols=pkCols;
        this.tableVersion=tableVersion;
        this.execRowDefinition=execRowDefinition;
        this.autoIncRowArray=autoIncRowArray;
        this.sequences=sequences;
        this.heapConglom=heapConglom;
        this.isUpsert=isUpsert;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
            rdd.saveAsNewAPIHadoopDataset(config);
            if(opContext.getOperation()!=null){
                opContext.getOperation().fireAfterStatementTriggers();
            }
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLInteger((int)opContext.getRecordsWritten()));
            InsertOperation insertOperation=((InsertOperation)opContext.getOperation());
            if(insertOperation!=null) {
                List<String> badRecords = opContext.getBadRecords();
                opContext.getActivation().getLanguageConnectionContext().setFailedRecords(badRecords.size());
                if (badRecords.size() > 0) {
                    DataSet dataSet = new ControlDataSet<>(badRecords);
                    DistributedFileSystem fileSystem = SIDriver.driver().fileSystem();
                    Path path = generateFileSystemPathForWrite(insertOperation.statusDirectory, fileSystem, insertOperation);
                    dataSet.saveAsTextFile(path.toString());
                    opContext.getActivation().getLanguageConnectionContext().setBadFile(path.toString());
                    if (insertOperation.isAboveFailThreshold(badRecords.size())) {
                        throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(path.toString());
                    }
                }
            }
            return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TableWriter getTableWriter() throws StandardException{
        return new InsertPipelineWriter(pkCols,tableVersion,execRowDefinition,autoIncRowArray,sequences,heapConglom,
                txn,opContext,isUpsert);
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return opContext.getTxn();
        else
            return txn;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }

    private static Path generateFileSystemPathForWrite(String badDirectory,
                                                       DistributedFileSystem fileSystem,
                                                       SpliceOperation spliceOperation) throws StandardException {


        java.nio.file.Path inputFilePath = fileSystem.getPath(spliceOperation.getVTIFileName());
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "generateFileSystemPathForWrite(): badDirectory=%s, filePath=%s", badDirectory, inputFilePath);
        assert inputFilePath !=null;
        String vtiFileName=inputFilePath.getFileName().toString();

        if (badDirectory == null || badDirectory.isEmpty() || badDirectory.toUpperCase().equals("NULL")) {
            badDirectory = inputFilePath.getParent().toString();
        }

        ImportUtils.validateWritable(badDirectory,true);
        int i=0;
        while(true){
            String fileName=new org.apache.hadoop.fs.Path(vtiFileName).getName();
            fileName=fileName+(i==0?".bad":"_"+i+".bad");
            Path fileSystemPathForWrites=new Path(badDirectory,fileName);
            FileInfo info;
            try {
                info=fileSystem.getInfo(fileSystemPathForWrites.toString());
            }catch(IOException e){
                // File does not exist already, so we are done
                return fileSystemPathForWrites;
            }
            if (info == null) return fileSystemPathForWrites;
            i++;
        }
    }
}
