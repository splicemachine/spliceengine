package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.stream.output.SMOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDeleteTableWriterBuilder<K,V> extends DeleteTableWriterBuilder{
    private JavaPairRDD<K,V> rdd;

    public SparkDeleteTableWriterBuilder(JavaPairRDD<K, V> rdd){
        this.rdd=rdd;
    }

    @Override
    public DataSetWriter build() throws StandardException{
        try{
            operationContext.getOperation().fireBeforeStatementTriggers();
            Configuration conf=new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
            TableWriterUtils.serializeDeleteTableWriterBuilder(conf,this);
            conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,SMOutputFormat.class,SMOutputFormat.class);
            return new DeleteDataSetWriter<>(rdd,operationContext,conf);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
}
