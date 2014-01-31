package com.splicemachine.io.compress;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.io.compress.BlockCompressorStream;
import org.apache.hadoop.io.compress.BlockDecompressorStream;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.io.compress.Compressor;
import org.apache.hadoop.io.compress.Decompressor;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.io.compress.snappy.SnappyCompressor;
import org.apache.hadoop.io.compress.snappy.SnappyDecompressor;
import org.apache.hadoop.util.NativeCodeLoader;

public class SpliceSnappyCodec implements Configurable, CompressionCodec {
	  Configuration conf;
	  public static boolean isLoadedCorrectly;


	  /**
	   * Set the configuration to be used by this object.
	   *
	   * @param conf the configuration object.
	   */
	  @Override
	  public void setConf(Configuration conf) {
	    this.conf = conf;
	  }

	  /**
	   * Return the configuration used by this object.
	   *
	   * @return the configuration object used by this objec.
	   */
	  @Override
	  public Configuration getConf() {
	    return conf;
	  }

	  
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
	  	  
	  public static boolean isNativeCodeLoaded() {
	    return SnappyCompressor.isNativeCodeLoaded() && 
	        SnappyDecompressor.isNativeCodeLoaded();
	  }

	  /**
	   * Create a {@link CompressionOutputStream} that will write to the given
	   * {@link OutputStream}.
	   *
	   * @param out the location for the final output stream
	   * @return a stream the user can write uncompressed data to have it compressed
	   * @throws IOException
	   */
	  @Override
	  public CompressionOutputStream createOutputStream(OutputStream out)
	      throws IOException {
	    return createOutputStream(out, createCompressor());
	  }

	  /**
	   * Create a {@link CompressionOutputStream} that will write to the given
	   * {@link OutputStream} with the given {@link Compressor}.
	   *
	   * @param out        the location for the final output stream
	   * @param compressor compressor to use
	   * @return a stream the user can write uncompressed data to have it compressed
	   * @throws IOException
	   */
	  @Override
	  public CompressionOutputStream createOutputStream(OutputStream out,
	                                                    Compressor compressor)
	      throws IOException {
	    checkNativeCodeLoaded();
	    int bufferSize = conf.getInt(
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);

	    int compressionOverhead = (bufferSize / 6) + 32;

	    return new BlockCompressorStream(out, compressor, bufferSize,
	        compressionOverhead);
	  }

	  /**
	   * Get the type of {@link Compressor} needed by this {@link CompressionCodec}.
	   *
	   * @return the type of compressor needed by this codec.
	   */
	  @Override
	  public Class<? extends Compressor> getCompressorType() {
	    checkNativeCodeLoaded();
	    return SnappyCompressor.class;
	  }

	  /**
	   * Create a new {@link Compressor} for use by this {@link CompressionCodec}.
	   *
	   * @return a new compressor for use by this codec
	   */
	  @Override
	  public Compressor createCompressor() {
	    checkNativeCodeLoaded();
	    int bufferSize = conf.getInt(
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
	    return new SnappyCompressor(bufferSize);
	  }

	  /**
	   * Create a {@link CompressionInputStream} that will read from the given
	   * input stream.
	   *
	   * @param in the stream to read compressed bytes from
	   * @return a stream to read uncompressed bytes from
	   * @throws IOException
	   */
	  @Override
	  public CompressionInputStream createInputStream(InputStream in)
	      throws IOException {
	    return createInputStream(in, createDecompressor());
	  }

	  /**
	   * Create a {@link CompressionInputStream} that will read from the given
	   * {@link InputStream} with the given {@link Decompressor}.
	   *
	   * @param in           the stream to read compressed bytes from
	   * @param decompressor decompressor to use
	   * @return a stream to read uncompressed bytes from
	   * @throws IOException
	   */
	  @Override
	  public CompressionInputStream createInputStream(InputStream in,
	                                                  Decompressor decompressor)
	      throws IOException {
	    checkNativeCodeLoaded();
	    return new BlockDecompressorStream(in, decompressor, conf.getInt(
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT));
	  }

	  /**
	   * Get the type of {@link Decompressor} needed by this {@link CompressionCodec}.
	   *
	   * @return the type of decompressor needed by this codec.
	   */
	  @Override
	  public Class<? extends Decompressor> getDecompressorType() {
	    checkNativeCodeLoaded();
	    return SnappyDecompressor.class;
	  }

	  /**
	   * Create a new {@link Decompressor} for use by this {@link CompressionCodec}.
	   *
	   * @return a new decompressor for use by this codec
	   */
	  @Override
	  public Decompressor createDecompressor() {
	    checkNativeCodeLoaded();
	    int bufferSize = conf.getInt(
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_KEY,
	        CommonConfigurationKeys.IO_COMPRESSION_CODEC_SNAPPY_BUFFERSIZE_DEFAULT);
	    return new SnappyDecompressor(bufferSize);
	  }

	  /**
	   * Get the default filename extension for this kind of compression.
	   *
	   * @return <code>.snappy</code>.
	   */
	  @Override
	  public String getDefaultExtension() {
	    return ".snappy";
	  }

}
