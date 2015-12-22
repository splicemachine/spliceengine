package com.splicemachine.pipeline.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public interface PipelineCompressor{

   InputStream compressedInput(InputStream input) throws IOException;

   OutputStream compress(OutputStream output) throws IOException;

   byte[] compress(Object o) throws IOException;

   <T> T decompress(byte[] bytes, Class<T> clazz) throws IOException;
}
