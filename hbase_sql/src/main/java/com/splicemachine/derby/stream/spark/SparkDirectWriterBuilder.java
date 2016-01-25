package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.stream.index.HTableOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDirectWriterBuilder<K,V> extends DirectTableWriterBuilder{
    private JavaPairRDD<K, V> rdd;

    public SparkDirectWriterBuilder(JavaPairRDD<K, V> rdd){
        this.rdd=rdd;
    }

    @Override
    public DataSetWriter build() throws StandardException{
        try{
            Configuration conf=new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
            TableWriterUtils.serializeHTableWriterBuilder(conf,this);
            conf.setClass(MRJobConfig.OUTPUT_FORMAT_CLASS_ATTR,HTableOutputFormat.class,HTableOutputFormat.class);
            JavaSparkContext context=SpliceSpark.getContext();
            return new SparkDirectDataSetWriter<>(rdd,context,conf);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
}
