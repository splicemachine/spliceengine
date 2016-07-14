/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.spark.api.java.JavaPairRDD;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.stream.output.SMOutputFormat;
import scala.util.Either;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkUpdateTableWriterBuilder<K,V> extends UpdateTableWriterBuilder{
    private transient JavaPairRDD<K,Either<Exception, V>> rdd;

    public SparkUpdateTableWriterBuilder(){
    }

    public SparkUpdateTableWriterBuilder(JavaPairRDD<K, Either<Exception, V>> rdd){
        this.rdd=rdd;
    }

    @Override
    public DataSetWriter build() throws StandardException{
        try{
            if (operationContext.getOperation() != null) {
                operationContext.getOperation().fireBeforeStatementTriggers();
            }
            Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
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
