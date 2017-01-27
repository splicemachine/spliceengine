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
