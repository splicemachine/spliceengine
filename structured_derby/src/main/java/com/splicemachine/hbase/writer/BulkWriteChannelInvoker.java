package com.splicemachine.hbase.writer;

import com.google.protobuf.ByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.NoRetryCoprocessorRpcChannel;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.hbase.table.SpliceRpcController;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 3/20/14
 */
public class BulkWriteChannelInvoker implements BulkWriteInvoker{

		private final HConnection connection;
		private final byte[] tableName;

		public BulkWriteChannelInvoker(HConnection connection, byte[] tableName) {
				this.connection = connection;
				this.tableName = tableName;
		}

		@Override
		public BulkWriteResult invoke(BulkWrite write, boolean refreshCache) throws IOException {
				NoRetryCoprocessorRpcChannel channel
								= new NoRetryCoprocessorRpcChannel(connection, TableName.valueOf(tableName),write.getRegionKey());

				try {
						SpliceMessage.SpliceIndexService service =
										ProtobufUtil.newServiceStub(SpliceMessage.SpliceIndexService.class,channel);

						//TODO -sf- replace BulkWrite with a protobuf
						SpliceMessage.BulkWriteRequest bwr =
										SpliceMessage.BulkWriteRequest.newBuilder().setBytes(ByteString.copyFrom(write.toBytes())).build();
						BlockingRpcCallback<SpliceMessage.BulkWriteResponse> doneCallback = new BlockingRpcCallback<SpliceMessage.BulkWriteResponse>();
						SpliceRpcController controller = new SpliceRpcController();
						service.bulkWrite(controller,bwr, doneCallback);
						Throwable error = controller.getThrowable();
						if(error!=null) {
							if(error instanceof IncorrectRegionException || error instanceof NotServingRegionException){
								/*
								 * We sent it to the wrong place, so we need to resubmit it. But since we
								 * pulled it from the cache, we first invalidate that cache
								 */
								connection.clearRegionCache(TableName.valueOf(tableName));	
							}
							throw Exceptions.getIOException(error);
						}
						SpliceMessage.BulkWriteResponse bulkWriteResponse = doneCallback.get();
						return BulkWriteResult.fromBytes(bulkWriteResponse.getBytes().toByteArray());
				} catch (Exception e) {
						throw Exceptions.getIOException(e);
				}
		}

		public static class Factory implements BulkWriteInvoker.Factory{
				private final HConnection connection;
				private final byte[] tableName;

				public Factory(HConnection connection, byte[] tableName) {
						this.connection = connection;
						this.tableName = tableName;
				}

				@Override
				public BulkWriteInvoker newInstance() {
						return new BulkWriteChannelInvoker(connection,tableName);
				}
		}
}
