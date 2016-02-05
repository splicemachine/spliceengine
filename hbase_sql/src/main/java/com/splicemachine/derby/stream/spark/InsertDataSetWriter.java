package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

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
        try{
            rdd.saveAsNewAPIHadoopDataset(config);
            if(opContext.getOperation()!=null){
                opContext.getOperation().fireAfterStatementTriggers();
            }
            ValueRow valueRow=new ValueRow(1);
            InsertOperation insertOperation=((InsertOperation)opContext.getOperation());
            if(insertOperation!=null && insertOperation.isImport()){
                List<String> badRecords=opContext.getBadRecords();
                if(badRecords.size()>0){
                    // System.out.println("badRecords -> " + badRecords);
                    DataSet dataSet=new ControlDataSet<>(badRecords);
                    Path path=null;
                    if(insertOperation.statusDirectory!=null && !insertOperation.statusDirectory.equals("NULL")){
                        FileSystem fileSystem=FileSystem.get(HConfiguration.INSTANCE.unwrapDelegate());
                        path=generateFileSystemPathForWrite(insertOperation.statusDirectory,fileSystem,insertOperation);
                        dataSet.saveAsTextFile().directory(path.toString()).build().write();
                    }
                    if(insertOperation.failBadRecordCount==0)
                        throw new RuntimeException(badRecords.get(0));
                    if(badRecords.size()>=insertOperation.failBadRecordCount)
                        throw new RuntimeException(ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(path==null?"--No Output File Provided--":path.toString()));
                }
            }

            valueRow.setColumn(1,new SQLInteger((int)opContext.getRecordsWritten()));
            return new ControlDataSet<>(Collections.singletonList(new LocatedRow(valueRow)));
        }catch(IOException ioe){
            throw Exceptions.parseException(ioe);
        }
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

    private static Path generateFileSystemPathForWrite(String badDirectory,FileSystem fileSystem,SpliceOperation spliceOperation) throws StandardException{
        try{
            String vtiFileName=spliceOperation.getVTIFileName();
            ImportUtils.validateWritable(badDirectory,true);
            int i=0;
            while(true){
                String fileName=new org.apache.hadoop.fs.Path(vtiFileName).getName();
                fileName=fileName+(i==0?".bad":"_"+i+".bad");
                Path fileSystemPathForWrites=new Path(badDirectory,fileName);
                if(!fileSystem.exists(fileSystemPathForWrites))
                    return fileSystemPathForWrites;
                i++;
            }
        }catch(IOException ioe){
            throw StandardException.plainWrapException(ioe);
        }
    }


}
