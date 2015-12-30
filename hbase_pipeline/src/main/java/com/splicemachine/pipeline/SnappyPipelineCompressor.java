package com.splicemachine.pipeline;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.pipeline.utils.PipelineCompressor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;

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
        snappy.setConf(SpliceConstants.config);
        boolean sN = false;
        Method method;
        try{
            // Cloudera Path
            method=SnappyCodec.class.getMethod("isNativeCodeLoaded",null);
            sN=(Boolean)method.invoke(snappy,null);
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"basic snappy codec not supported, checking alternative method signature");
            try{
                method=SnappyCodec.class.getMethod("isNativeSnappyLoaded",Configuration.class);
                sN=(Boolean)method.invoke(snappy,SpliceConstants.config);
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
        return snappy.createInputStream(input);
    }

    @Override
    public OutputStream compress(OutputStream output) throws IOException{
        return snappy.createOutputStream(output);
    }

    @Override
    public byte[] compress(Object o) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public <T> T decompress(byte[] bytes,Class<T> clazz) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
