package com.splicemachine.pipeline.client;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public interface RpcChannelFactory{
    CoprocessorRpcChannel newChannel(TableName tableName,byte[] regionKey) throws IOException;
}
