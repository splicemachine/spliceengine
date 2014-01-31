package com.splicemachine.hbase.writer;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.BatchProtocol;
import com.splicemachine.hbase.NoRetryExecRPCInvoker;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;

import java.io.IOException;
import java.lang.reflect.Proxy;

/**
 * @author Scott Fines
 *         Date: 1/31/14
 */
public class BulkWriteRPCInvoker implements BulkWriteInvoker {
		private static final Class<BatchProtocol> batchProtocolClass = BatchProtocol.class;
		private static final Class<? extends CoprocessorProtocol>[] protoClassArray = new Class[]{batchProtocolClass};

		private final HConnection connection;
		private final byte[] tableName;

		public BulkWriteRPCInvoker(HConnection connection, byte[] tableName) {
				this.connection = connection;
				this.tableName = tableName;
		}

		@Override
		public BulkWriteResult invoke(BulkWrite write,boolean refreshCache) throws IOException {
				Configuration config = SpliceConstants.config;
				NoRetryExecRPCInvoker invoker = new NoRetryExecRPCInvoker(config,
								connection,batchProtocolClass,tableName,write.getRegionKey(),refreshCache);

				BatchProtocol instance = (BatchProtocol) Proxy.newProxyInstance(config.getClassLoader(),
								protoClassArray,invoker);

				byte[] bytes = instance.bulkWrite(write.toBytes());
				BulkWriteResult response = BulkWriteResult.fromBytes(bytes);
				return response;
		}

		public static final class Factory implements BulkWriteInvoker.Factory{
				private final HConnection connection;
				private final byte[] tableName;

				public Factory(HConnection connection, byte[] tableName) {
						this.connection = connection;
						this.tableName = tableName;
				}

				@Override
				public BulkWriteInvoker newInstance() {
						return new BulkWriteRPCInvoker(connection,tableName);
				}
		}
}
