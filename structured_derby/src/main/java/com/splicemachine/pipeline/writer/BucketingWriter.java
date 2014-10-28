package com.splicemachine.pipeline.writer;

import com.splicemachine.hbase.RegionCache;
import com.splicemachine.pipeline.api.Writer;
import com.splicemachine.pipeline.utils.PipelineConstants;

import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 8/8/13
 */
public abstract class BucketingWriter extends PipelineConstants implements Writer{
    protected final RegionCache regionCache;
    protected final HConnection connection;

    protected BucketingWriter(RegionCache regionCache, HConnection connection) {
        this.regionCache = regionCache;
        this.connection = connection;
    }
    private Exception getError(List<Throwable> errors) {
        return new RetriesExhaustedWithDetailsException(errors,Collections.<Row>emptyList(),Collections.<String>emptyList());
    }

}
