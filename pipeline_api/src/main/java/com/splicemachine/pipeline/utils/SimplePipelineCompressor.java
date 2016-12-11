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

package com.splicemachine.pipeline.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.pipeline.client.BulkWrites;
import com.splicemachine.pipeline.client.PipelineEncoding;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class SimplePipelineCompressor implements PipelineCompressor{
    private final TxnOperationFactory txnOperationFactory;
    private final KryoPool kp;

    public SimplePipelineCompressor(KryoPool kp,TxnOperationFactory txnOperationFactory){
        this.txnOperationFactory = txnOperationFactory;
        this.kp = kp;
    }

    @Override
    public InputStream compressedInput(InputStream input) throws IOException{
        return input;
    }

    @Override
    public OutputStream compress(OutputStream output) throws IOException{
        return output;
    }

    @Override
    public byte[] compress(Object o) throws IOException{
        if(o instanceof BulkWrites){
            BulkWrites bw = (BulkWrites)o;
            return PipelineEncoding.encode(txnOperationFactory,bw);
        }else {
            Output out = new Output(128,-1);
            Kryo kryo = kp.get();
            try{
                kryo.writeObject(out,o);
                out.flush();
                return out.getBuffer();
            }finally{
                kp.returnInstance(kryo);
            }
        }
    }

    @Override
    public <T> T decompress(byte[] bytes,Class<T> clazz) throws IOException{
        if(clazz.isAssignableFrom(BulkWrites.class))
            return (T)PipelineEncoding.decode(txnOperationFactory,bytes);
        else{
            Input input = new Input(bytes);
            Kryo kryo = kp.get();
            try{
                return kryo.readObject(input,clazz);
            }finally{
                kp.returnInstance(kryo);
            }
        }
    }
}
