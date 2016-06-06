package com.splicemachine.derby.stream.control;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.TableWriter;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class ControlDataSetWriter<K> implements DataSetWriter{
    private final ControlPairDataSet<K, ExecRow> dataSet;
    private final OperationContext operationContext;
    private final AbstractPipelineWriter<ExecRow> pipelineWriter;
    private static final Logger LOG = Logger.getLogger(ControlDataSetWriter.class);

    public ControlDataSetWriter(ControlPairDataSet<K, ExecRow> dataSet,AbstractPipelineWriter<ExecRow> pipelineWriter,OperationContext opContext){
        this.dataSet=dataSet;
        this.operationContext=opContext;
        this.pipelineWriter=pipelineWriter;
    }

    @Override
    public DataSet<LocatedRow> write() throws StandardException{
        SpliceOperation operation=operationContext.getOperation();
        Txn txn = null;
        try{
            txn = SIDriver.driver().lifecycleManager().beginChildTransaction(getTxn(),pipelineWriter.getDestinationTable());
            pipelineWriter.setTxn(txn);
            operation.fireBeforeStatementTriggers();
            pipelineWriter.open(operation.getTriggerHandler(),operation);
            pipelineWriter.write(dataSet.values().toLocalIterator());
            ValueRow valueRow=new ValueRow(1);
            valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
            operationContext.getActivation().getLanguageConnectionContext().setRecordsImported(operationContext.getRecordsWritten());
            if(operation instanceof InsertOperation){
                InsertOperation insertOperation = (InsertOperation)operation;
                List<String> badRecords=operationContext.getBadRecords();
                /*
                 * In Control-side execution, we have different operation contexts for each operation,
                 * and all operations are held in this JVM. this means that parse errors could be present
                 * at the context for the lower operation (i.e. in an import), so we need to collect those errors
                 * directly.
                 */
                List<SpliceOperation> ops =insertOperation.getOperationStack();
                for(SpliceOperation op:ops){
                    if(op==null || op==insertOperation || op.getOperationContext()==null) continue;
                    badRecords.addAll(op.getOperationContext().getBadRecords());
                }
                operationContext.getActivation().getLanguageConnectionContext().setFailedRecords(badRecords.size());
                if(badRecords.size()>0){
                    DataSet dataSet=new ControlDataSet<>(badRecords);
                    DistributedFileSystem fileSystem=SIDriver.driver().fileSystem();
                    Path path = generateFileSystemPathForWrite(insertOperation.statusDirectory,fileSystem,insertOperation);
                    dataSet.saveAsTextFile(path.toString());
                    operationContext.getActivation().getLanguageConnectionContext().setBadFile(path.toString());
                    if(insertOperation.isAboveFailThreshold(badRecords.size())){
                        throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(path.toString());
                    }
                }
            }
            txn.commit();
            return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
        }catch(Exception e){
            if(txn!=null){
                try{
                    txn.rollback();
                }catch(IOException e1){
                    e.addSuppressed(e1);
                }
            }
            throw Exceptions.parseException(e);
        }finally{
            try{
                if(pipelineWriter!=null)
                    pipelineWriter.close();
                operation.fireAfterStatementTriggers();
            }catch(Exception e){
                throw Exceptions.parseException(e);
            }

        }
    }

    @Override
    public void setTxn(TxnView childTxn){
        pipelineWriter.setTxn(childTxn);
    }

    @Override
    public TableWriter getTableWriter(){
        return pipelineWriter;
    }

    @Override
    public TxnView getTxn(){
        return pipelineWriter.getTxn();
    }

    @Override
    public byte[] getDestinationTable(){
        return pipelineWriter.getDestinationTable();
    }

    /* ********************************************************************************************/
    /*private helper methods*/
    private static Path generateFileSystemPathForWrite(String badDirectory,
                                                       DistributedFileSystem fileSystem,
                                                       SpliceOperation spliceOperation) throws StandardException {

        Path inputFilePath = fileSystem.getPath(spliceOperation.getVTIFileName());
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
            String fileName=badDirectory+"/"+vtiFileName;
            fileName=fileName+(i==0?".bad":"_"+i+".bad");
            Path fileSystemPathForWrites=fileSystem.getPath(fileName);
            if(!Files.exists(fileSystemPathForWrites)){
                return fileSystemPathForWrites;
            }
            i++;
        }
    }
}
