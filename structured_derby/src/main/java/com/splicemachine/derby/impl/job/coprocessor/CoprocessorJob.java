package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.job.Job;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * Job intended for execution via HBase Coprocessors
 *
 * @author Scott Fines
 * Created on: 4/5/13
 */
public interface CoprocessorJob extends Job {

    Map<? extends RegionTask,Pair<byte[],byte[]>> getTasks() throws Exception;

    HTableInterface getTable();
}
