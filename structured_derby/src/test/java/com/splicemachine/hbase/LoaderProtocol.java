package com.splicemachine.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

/**
 * @author Scott Fines
 *         Created on: 4/29/13
 */
public interface LoaderProtocol extends CoprocessorProtocol {

    public void loadSomeData(Configuration conf, String tableName, int numRows,int startRow) throws Exception;
}
