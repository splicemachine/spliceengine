package com.splicemachine.derby.impl;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;

/**
 * Created by jleach on 9/19/16.
 */
public class SpliceSparkSerializer extends KryoSerializer {

    public SpliceSparkSerializer(SparkConf conf) {
        super(conf);
    }

    @Override
    public Kryo newKryo() {
        return super.newKryo();
    }
}
