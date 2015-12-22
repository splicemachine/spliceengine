package com.splicemachine.pipeline.utils;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import javax.security.auth.login.Configuration;
import java.lang.reflect.Method;

public class PipelineConstants{
    private static final Logger LOG=Logger.getLogger(PipelineConstants.class);
    public static final SnappyCodec snappy;


    public static boolean supportsNative;

    static{
        snappy=new org.apache.hadoop.io.compress.SnappyCodec();
        snappy.setConf(SpliceConstants.config);
        Method method;
        try{
            // Cloudera Path
            method=SnappyCodec.class.getMethod("isNativeCodeLoaded",null);
            supportsNative=(Boolean)method.invoke(snappy,null);
        }catch(Exception e){
            SpliceLogUtils.error(LOG,"basic snappy codec not supported, checking alternative method signature");
            try{
                method=SnappyCodec.class.getMethod("isNativeSnappyLoaded",Configuration.class);
                supportsNative=(Boolean)method.invoke(snappy,SpliceConstants.config);
            }catch(Exception ioe){
                SpliceLogUtils.error(LOG,"Alternative signature did not work, No Snappy Codec Support",ioe);
                supportsNative=false;
            }
        }
        if(!supportsNative)
            SpliceLogUtils.error(LOG,"No Native Snappy Installed: Splice Machine's Write Pipeline will not compress data over the wire.");
        else
            SpliceLogUtils.info(LOG,"Snappy Installed: Splice Machine's Write Pipeline will compress data over the wire.");
        supportsNative=false;
    }


    public static final ObjectArrayList<KVPair> emptyList=new ObjectArrayList<>();


}
