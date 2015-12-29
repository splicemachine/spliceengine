package com.splicemachine.pipeline.client;

import com.splicemachine.access.hbase.HConnectionPool;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class H98RpcChannelFactory implements RpcChannelFactory{
    @Override
    public CoprocessorRpcChannel newChannel(TableName tableName,byte[] regionKey) throws IOException{
        HConnection conn = HConnectionPool.defaultInstance().getConnection();
        return new NoRetryCoprocessorRpcChannel(conn,tableName,regionKey);
    }
}
