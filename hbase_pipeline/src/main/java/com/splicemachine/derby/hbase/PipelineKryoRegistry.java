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
