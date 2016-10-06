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
package com.splicemachine.serializer;

import com.esotericsoftware.kryo.Kryo;
import com.splicemachine.EngineDriver;
import com.splicemachine.utils.kryo.AbstractKryoPool;
import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoSerializer;
import org.apache.spark.serializer.SerializerInstance;

/**
 *
 * Spark's default KryoSerializer recreates the Kryo instance for each execution.  We need
 * to re-use them to lower the latency.
 *
 */
public class SpliceKryoSerializer extends KryoSerializer {
    public static volatile AbstractKryoPool kp;
    static {
        EngineDriver driver = EngineDriver.driver();
        int kpSize = (driver==null)?1000:driver.getConfiguration().getKryoPoolSize();
        kp = new AbstractKryoPool(kpSize) {
            @Override
            public Kryo newInstance() {
                return null;
            }
        };
    }
    public SpliceKryoSerializer(SparkConf conf) {
        super(conf);
    }

    @Override
    public SerializerInstance newInstance() {
        return new SpliceKryoSerializerInstance(this);
    }

    @Override
    public Kryo newKryo() {
        Kryo next = kp.get();
        return next == null?super.newKryo():next;
    }
}
