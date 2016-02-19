package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.control.ControlDataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.stream.output.SMOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.Collections;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkUpdateTableWriterBuilder<K,V> extends UpdateTableWriterBuilder{
    private transient JavaPairRDD<K,V> rdd;

    public SparkUpdateTableWriterBuilder(){
    }

    public SparkUpdateTableWriterBuilder(JavaPairRDD<K, V> rdd){
        this.rdd=rdd;
    }

    @Override
    public DataSetWriter build() throws StandardException{
        try{
            if (operationContext.getOperation() != null) {
                operationContext.getOperation().fireBeforeStatementTriggers();
            }
            Configuration conf=new Configuration(HConfiguration.INSTANCE.unwrapDelegate());
            TableWriterUtils.serializeUpdateTableWriterBuilder(conf,this);
            conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,SMOutputFormat.class,SMOutputFormat.class);
            return new SparkUpdateDataSetWriter<>(rdd,
                    operationContext,
                    conf,
                    heapConglom,
                    formatIds,
                    columnOrdering,
                    pkCols,
                    pkColumns,
                    tableVersion,
                    execRowDefinition,
                    heapList);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }
}
