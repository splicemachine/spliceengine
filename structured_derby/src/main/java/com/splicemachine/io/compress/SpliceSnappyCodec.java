package com.splicemachine.io.compress;

import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

public class SpliceSnappyCodec extends SnappyCodec {
	public static boolean isLoadedCorrectly;
	
	  public static void checkNativeCodeLoaded() {
		  if (isLoadedCorrectly)
			  return;
	      if (!NativeCodeLoader.buildSupportsSnappy()) {
	        throw new RuntimeException("native snappy library not available: " +
	            "this version of libhadoop was built without " +
	            "snappy support.");
	      }
	      if (!SnappyCompressor.isNativeCodeLoaded()) {
	        throw new RuntimeException("native snappy library not available: " +
	            "SnappyCompressor has not been loaded.");
	      }
	      if (!SnappyDecompressor.isNativeCodeLoaded()) {
	        throw new RuntimeException("native snappy library not available: " +
	            "SnappyDecompressor has not been loaded.");
	      }
	      isLoadedCorrectly = true;
	  }

}
