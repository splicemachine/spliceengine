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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.stream.index.HTableOutputFormat;
import scala.util.Either;

/**
 * @author Scott Fines
 *         Date: 1/25/16
 */
public class SparkDirectWriterBuilder<K,V> extends DirectTableWriterBuilder{
    private JavaPairRDD<K, Either<Exception, V>> rdd;

    public SparkDirectWriterBuilder(){
    }

    public SparkDirectWriterBuilder(JavaPairRDD<K, Either<Exception, V>> rdd){
        this.rdd=rdd;
    }

    @Override
    public DataSetWriter build() throws StandardException{
        try{
            Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
            TableWriterUtils.serializeHTableWriterBuilder(conf,this);
            conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,HTableOutputFormat.class,HTableOutputFormat.class);
            JavaSparkContext context=SpliceSpark.getContext();
            return new SparkDirectDataSetWriter<>(rdd,context,opCtx,conf,skipIndex,destConglomerate,txn);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException{
        super.writeExternal(out);
        out.writeObject(rdd);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        super.readExternal(in);
        rdd = (JavaPairRDD)in.readObject();
    }
}
