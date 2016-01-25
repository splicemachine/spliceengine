package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.pipeline.Exceptions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class InsertDataSetWriter<K,V> implements DataSetWriter{
    private final JavaPairRDD<K,V> rdd;
    private final OperationContext<? extends SpliceOperation> opContext;
    private final Configuration config;

    public InsertDataSetWriter(JavaPairRDD<K, V> rdd,
                               OperationContext<? extends SpliceOperation> opContext,
                               Configuration config){
        this.rdd=rdd;
        this.opContext=opContext;
        this.config = config;
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
                        path=ImportUtils.generateFileSystemPathForWrite(insertOperation.statusDirectory,fileSystem,insertOperation);
                        dataSet.saveAsTextFile().directory(path.toString()).build().write();
//                        fileSystem.close();
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
}
