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
