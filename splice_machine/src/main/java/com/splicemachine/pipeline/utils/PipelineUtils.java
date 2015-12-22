package com.splicemachine.pipeline.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.io.Closeables;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import com.splicemachine.utils.kryo.KryoPool;

import java.io.*;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class PipelineUtils {

    public static byte[] toCompressedBytes(Object object,PipelineCompressor compressor) throws IOException{
        Output output=null;
        OutputStream compressedOutput=null;
        ByteArrayOutputStream baos=new ByteArrayOutputStream();
        KryoObjectOutput koo;
        KryoPool pool=SpliceKryoRegistry.getInstance();
        Kryo kryo=pool.get();
        try{
            compressedOutput=compressor!=null?compressor.compress(baos):baos;
            output=new Output(compressedOutput);
            koo=new KryoObjectOutput(output,kryo);
            koo.writeObject(object);
            koo.flush();
            compressedOutput.flush();
            return baos.toByteArray();
        }finally{
            pool.returnInstance(kryo);
            Closeables.closeQuietly(output);
            Closeables.closeQuietly(compressedOutput);
            Closeables.closeQuietly(baos);
        }
    }

    public static <T> T fromCompressedBytes(byte[] bytes,PipelineCompressor compressor) throws IOException{
        Input input=null;
        ByteArrayInputStream bais=null;
        InputStream compressedInput=null;
        KryoObjectInput koi;
        KryoPool pool=SpliceKryoRegistry.getInstance();
        Kryo kryo=pool.get();
        try{
            bais=new ByteArrayInputStream(bytes);
            compressedInput=compressor!=null?compressor.compressedInput(bais):bais;
            input=new Input(compressedInput);
            koi=new KryoObjectInput(input,kryo);
            return (T)koi.readObject();
        }catch(ClassNotFoundException e){
            throw new IOException(e);
        }finally{
            pool.returnInstance(kryo);
            Closeables.closeQuietly(input);
            Closeables.closeQuietly(compressedInput);
            Closeables.closeQuietly(bais);
        }
    }

}    