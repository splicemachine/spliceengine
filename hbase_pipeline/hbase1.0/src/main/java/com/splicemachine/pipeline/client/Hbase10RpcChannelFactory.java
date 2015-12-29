package com.splicemachine.pipeline.client;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class Hbase10RpcChannelFactory implements RpcChannelFactory{
    @Override
    public CoprocessorRpcChannel newChannel(TableName tableName,byte[] regionKey) throws IOException{
        Connection conn = HBaseConnectionFactory.getInstance().getConnection();
        return new NoRetryCoprocessorRpcChannel(conn,tableName,regionKey);
    }
}
