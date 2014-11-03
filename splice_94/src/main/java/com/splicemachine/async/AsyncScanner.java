package com.splicemachine.async;

import com.splicemachine.hbase.MeasuredResultScanner;
import com.splicemachine.stream.CloseableStream;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public interface AsyncScanner extends MeasuredResultScanner{
    List<KeyValue> nextKeyValues() throws Exception;

    public void open() throws IOException;

    CloseableStream<List<KeyValue>> stream();
}
