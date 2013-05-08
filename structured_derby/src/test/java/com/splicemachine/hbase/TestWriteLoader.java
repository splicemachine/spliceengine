package com.splicemachine.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/29/13
 */
public class TestWriteLoader extends BaseEndpointCoprocessor implements LoaderProtocol{
    private TableWriter testWriter;
    @Override
    public void start(CoprocessorEnvironment env) {
        try {
            testWriter = TableWriter.create(env.getConfiguration());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.start(env);
    }

    @Override
    public void loadSomeData(Configuration conf,String tableName,int numRows,int startRow) throws Exception {
        CallBuffer<Mutation> callBuffer = testWriter.synchronousWriteBuffer(tableName.getBytes());
        for(int i=startRow;i<startRow+numRows;i++){
            Put put = new Put(Bytes.toBytes(i));
            //set in case we're used in an SI world
            put.setAttribute("si-transaction-id",Bytes.toBytes(1));
            put.add("attributes".getBytes(),Bytes.toBytes(i),Bytes.toBytes(i*10));
            callBuffer.add(put);
        }
        callBuffer.flushBuffer();
        callBuffer.close();
    }
}
