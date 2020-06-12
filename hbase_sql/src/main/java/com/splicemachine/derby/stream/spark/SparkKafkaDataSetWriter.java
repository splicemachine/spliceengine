/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.util.LongAccumulator;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;


public class SparkKafkaDataSetWriter<V> implements DataSetWriter{
    private String topicName;
    private JavaRDD<V> rdd;

    public SparkKafkaDataSetWriter(){
    }

    public SparkKafkaDataSetWriter(JavaRDD<V> rdd,
                                   String topicName){
        this.rdd = rdd;
        this.topicName = topicName;
    }

    public void writeExternal(ObjectOutput out) throws IOException{
        out.writeUTF(topicName);
        out.writeObject(rdd);
    }

    @SuppressWarnings("unchecked")
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{
        topicName = in.readUTF();
        rdd = (JavaRDD)in.readObject();
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException{
        long start = System.currentTimeMillis();

        CountFunction countFunction = new CountFunction<>();
        KafkaStreamer kafkaStreamer = new KafkaStreamer(rdd.getNumPartitions(), topicName);
        JavaRDD streamed = rdd.map(countFunction).mapPartitionsWithIndex(kafkaStreamer, true);
        streamed.collect();

        Long count = countFunction.getCount().value();
        if(count == 0) {
            try {
                kafkaStreamer.noData();
            } catch(Exception e) {
                throw StandardException.newException("", e);
            }
        }

        long end = System.currentTimeMillis();
        ValueRow valueRow=new ValueRow(2);
        valueRow.setColumn(1,new SQLLongint(count));
        valueRow.setColumn(2,new SQLLongint(end-start));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @Override
    public void setTxn(TxnView childTxn){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public TxnView getTxn(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public byte[] getDestinationTable(){
        throw new UnsupportedOperationException();
    }

    public static class CountFunction<V> implements org.apache.spark.api.java.function.Function<V, V>{
        private LongAccumulator count = SpliceSpark.getContext().sc().longAccumulator("exported rows");
        public CountFunction(){

        }

        @Override
        public V call(V v) throws Exception{
            count.add(1);
            return v;
        }

        public LongAccumulator getCount() {
            return count;
        }
    }
}
