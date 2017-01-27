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

package com.splicemachine.derby.hbase;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.pipeline.client.*;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.utils.kryo.ExternalizableSerializer;
import com.splicemachine.utils.kryo.KryoPool;

import java.util.Collection;

/**
 * Registry SOLELY for use with a client-server kryo interaction. DO NOT USE THIS FOR PERMANENT STORAGE!
 *
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class PipelineKryoRegistry implements KryoPool.KryoRegistry{
    @Override
    public void register(Kryo instance){
        instance.register(BulkWriteResult.class,BulkWriteResult.kryoSerializer(),10);
        instance.register(BulkWritesResult.class,new Serializer<BulkWritesResult>(){
            @Override
            public void write(Kryo kryo,Output output,BulkWritesResult object){
                kryo.writeClassAndObject(output,object.getBulkWriteResults());
            }

            @Override
            public BulkWritesResult read(Kryo kryo,Input input,Class type){
                Collection<BulkWriteResult> results=(Collection<BulkWriteResult>)kryo.readClassAndObject(input);
                return new BulkWritesResult(results);
            }
        },11);

        instance.register(WriteResult.class,ExternalizableSerializer.INSTANCE,12);
        instance.register(ConstraintContext.class,ExternalizableSerializer.INSTANCE,13);

    }

}
