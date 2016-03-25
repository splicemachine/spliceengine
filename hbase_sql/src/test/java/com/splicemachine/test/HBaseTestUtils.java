package com.splicemachine.test;

import java.io.IOException;
import java.sql.Connection;
import java.util.Map;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.hbase.SpliceRpcController;
import com.splicemachine.homeless.TestUtils;

/**
 * Test utilities requiring HBase
 */
public class HBaseTestUtils {

    public static boolean setBlockPreFlush(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPreFlush(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean setBlockPostFlush(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPostFlush(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean setBlockPreCompact(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPreCompact(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean setBlockPostCompact(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPostCompact(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean setBlockPreSplit(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPreSplit(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean setBlockPostSplit(String schemaName, String tableName, final boolean onOff, Connection connection) throws Throwable {
        String conglomerateId = Long.toString(TestUtils.baseTableConglomerateId(connection,
                                                                                schemaName, tableName));

        Map<byte[], Boolean> ret;
        try (Table table = HBaseConnectionFactory.getInstance(HConfiguration.getConfiguration()).getConnection().
            getTable(TableName.valueOf(HConfiguration.getConfiguration().getNamespace(), conglomerateId))) {
            ret = table.coprocessorService(SpliceMessage.BlockingProbeEndpoint.class,
                                           HConstants.EMPTY_START_ROW,
                                           HConstants.EMPTY_END_ROW,
                                           new Batch.Call<SpliceMessage.BlockingProbeEndpoint, Boolean>() {
                                               @Override
                                               public Boolean call(SpliceMessage.BlockingProbeEndpoint instance) throws IOException {
                                                   SpliceRpcController controller = new SpliceRpcController();
                                                   SpliceMessage.BlockingProbeRequest message = SpliceMessage.BlockingProbeRequest.newBuilder().setDoBlock(onOff).build();
                                                   BlockingRpcCallback<SpliceMessage.BlockingProbeResponse> rpcCallback = new BlockingRpcCallback<>();
                                                   instance.blockPostSplit(controller, message, rpcCallback);
                                                   SpliceMessage.BlockingProbeResponse response = rpcCallback.get();
                                                   return response.getDidBlock();
                                               }
                                           });
        }
        if (ret != null) {
            for(Boolean rsVal : ret.values()) {
                if (rsVal != onOff) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

}
