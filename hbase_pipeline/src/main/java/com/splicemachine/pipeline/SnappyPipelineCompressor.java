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

package com.splicemachine.pipeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.CodecPool;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class SnappyPipelineCompressor implements PipelineCompressor{
    private static final Logger LOG=Logger.getLogger(SnappyPipelineCompressor.class);
    private static final SnappyCodec snappy;
    private static final boolean supportsNative;

    static{
        snappy = new SnappyCodec();
        snappy.setConf(HConfiguration.unwrapDelegate());
        boolean sN;
        Method method;
        try{
            // Cloudera Path
            method=SnappyCodec.class.getMethod("isNativeCodeLoaded",null);
            sN=(Boolean)method.invoke(snappy,null);
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"basic snappy codec not supported, checking alternative method signature");
            try{
                method=SnappyCodec.class.getMethod("isNativeSnappyLoaded",Configuration.class);
                sN=(Boolean)method.invoke(snappy, HConfiguration.unwrapDelegate());
            }catch(Exception ioe){
                SpliceLogUtils.error(LOG,"Alternative signature did not work, No Snappy Codec Support",ioe);
                sN=false;
            }
        }
        if(!sN)
            SpliceLogUtils.error(LOG,"No Native Snappy Installed: Splice Machine's Write Pipeline will not compress data over the wire.");
        else
            SpliceLogUtils.info(LOG,"Snappy Installed: Splice Machine's Write Pipeline will compress data over the wire.");
        supportsNative = sN;
    }

    private final PipelineCompressor delegate;

    public SnappyPipelineCompressor(PipelineCompressor delegate){
        this.delegate=delegate;
    }

    @Override
    public InputStream compressedInput(InputStream input) throws IOException{
        if(supportsNative)
            return snappy.createInputStream(input);
        else return input;
    }

    @Override
    public OutputStream compress(OutputStream output) throws IOException{
        if(supportsNative)
            return snappy.createOutputStream(output);
        else return output;
    }

    @Override
    public byte[] compress(Object o) throws IOException{
        byte[] d = delegate.compress(o);
        if(!supportsNative) return d;
        ByteArrayOutputStream baos = new ByteArrayOutputStream(d.length);
        Compressor snappyCompressor = CodecPool.getCompressor(snappy);
        try {
            OutputStream os = snappy.createOutputStream(baos, snappyCompressor);
            os.write(d);
            os.flush();
            os.close();
        } finally {
            CodecPool.returnCompressor(snappyCompressor);
        }
        return baos.toByteArray();
    }

    @Override
    public <T> T decompress(byte[] bytes,Class<T> clazz) throws IOException{
        byte[] d = bytes;
        if (supportsNative) {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(d.length);

            Decompressor snappyDecompressor = CodecPool.getDecompressor(snappy);
            try {
                InputStream is = snappy.createInputStream(bais, snappyDecompressor);
                ByteStreams.copy(is, baos);
                baos.flush();
                d = baos.toByteArray();
                baos.close();
                is.close();
            } finally {
                CodecPool.returnDecompressor(snappyDecompressor);
            }
        }
        return delegate.decompress(d, clazz);
    }
}
