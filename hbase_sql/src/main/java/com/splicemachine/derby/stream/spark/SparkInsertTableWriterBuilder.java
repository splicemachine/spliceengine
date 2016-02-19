package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.stream.output.SMOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkInsertTableWriterBuilder<K,V> extends InsertTableWriterBuilder{
    private transient JavaPairRDD<K,V> rdd;

    public SparkInsertTableWriterBuilder(JavaPairRDD<K,V> rdd){
        this.rdd=rdd;
    }

    public SparkInsertTableWriterBuilder(){
    }

    @SuppressWarnings("unchecked")
    @Override
    public DataSetWriter build() throws StandardException{
        if(operationContext.getOperation()!=null){
            operationContext.getOperation().fireBeforeStatementTriggers();
        }
        final Configuration conf=new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
        try{
            TableWriterUtils.serializeInsertTableWriterBuilder(conf,this);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,SMOutputFormat.class,SMOutputFormat.class);
        return new InsertDataSetWriter<>(rdd,
                operationContext,
                conf,
                pkCols,
                tableVersion,
                execRowDefinition,
                autoIncrementRowLocationArray,
                spliceSequences,
                heapConglom,
                isUpsert);
    }
}
