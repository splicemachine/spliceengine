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

package com.splicemachine.pipeline;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
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
        OutputStream os = snappy.createOutputStream(baos);
        os.write(d);
        os.flush();
        os.close();
        return baos.toByteArray();
    }

    @Override
    public <T> T decompress(byte[] bytes,Class<T> clazz) throws IOException{
        byte[] d = bytes;
        if (supportsNative) {
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            ByteArrayOutputStream baos = new ByteArrayOutputStream(d.length);
            InputStream is = snappy.createInputStream(bais);
            ByteStreams.copy(is, baos);
            baos.flush();
            d = baos.toByteArray();
            baos.close();
            is.close();
        }
        return delegate.decompress(d, clazz);
    }
}
