/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
