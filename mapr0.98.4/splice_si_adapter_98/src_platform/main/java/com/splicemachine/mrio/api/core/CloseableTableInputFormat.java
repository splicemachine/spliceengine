package com.splicemachine.mrio.api.core;

import org.apache.hadoop.hbase.mapreduce.TableInputFormat;

import java.io.Closeable;
import java.io.IOException;

/**
 * Created by dgomezferro on 10/23/15.
 */
public class CloseableTableInputFormat extends TableInputFormat implements AutoCloseable, Closeable {
    @Override
    public void close() throws IOException {
        getHTable().close();
    }
}
