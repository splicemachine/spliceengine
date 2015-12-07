package com.splicemachine.hbase;

import com.google.protobuf.*;
import com.splicemachine.hbase.table.SpliceRpcController;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.ipc.SpliceRetryingCall;
import org.apache.hadoop.hbase.ipc.SpliceRetryingCaller;
import org.apache.hadoop.hbase.ipc.SpliceRetryingCallerFactory;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * @author Scott Fines
 *         Date: 3/19/14
 */
public class NoRetryCoprocessorRpcChannel extends CoprocessorRpcChannel {
	private static final Logger LOG = Logger.getLogger(NoRetryCoprocessorRpcChannel.class);
	private final Connection connection;
	private final TableName table;
	private final byte[] row;
	private byte[] lastRegion;

	private SpliceRetryingCallerFactory rpcFactory;

	public NoRetryCoprocessorRpcChannel(Connection connection, TableName table, byte[] row) {
		this.connection = connection;
		this.table = table;
		this.row = row;
		this.rpcFactory = SpliceRetryingCallerFactory.instantiate(connection.getConfiguration());
	}

	@Override
	public void callMethod(Descriptors.MethodDescriptor method, RpcController controller, Message request, Message responsePrototype, RpcCallback<Message> callback) {
		Message response = null;
		try {
			response = callExecService(method, request, responsePrototype);
		} catch (IOException ioe) {
			if(controller instanceof SpliceRpcController){
				((SpliceRpcController)controller).setFailed(ioe);
			}else
				ResponseConverter.setControllerException(controller, ioe);
		}
		if (callback != null) {
			callback.run(response);
		}
	}

	@Override
	protected Message callExecService(Descriptors.MethodDescriptor method, Message request, Message responsePrototype) throws IOException {
		if (LOG.isTraceEnabled()) {
			LOG.trace("Call: "+method.getName()+", "+request.toString());
		}

		if (row == null) {
			throw new IllegalArgumentException("Missing row property for remote region location");
		}

		final ClientProtos.CoprocessorServiceCall call =
				ClientProtos.CoprocessorServiceCall.newBuilder()
						.setRow(SpliceZeroCopyByteString.wrap(row))
						.setServiceName(method.getService().getFullName())
						.setMethodName(method.getName())
						.setRequest(request.toByteString()).build();
		RegionServerCallable<ClientProtos.CoprocessorServiceResponse> callable =
				new RegionServerCallable<ClientProtos.CoprocessorServiceResponse>((HConnection)connection, table, row) {
					public ClientProtos.CoprocessorServiceResponse call(int i) throws Exception{
						return call(); //TODO -sf- is this correct? I think we need to do something with timeouts here
					}

					public ClientProtos.CoprocessorServiceResponse call() throws Exception {
						byte[] regionName = getLocation().getRegionInfo().getRegionName();
						return ProtobufUtil.execService(getStub(), call, regionName);
					}
				};
		SpliceRetryingCall<ClientProtos.CoprocessorServiceResponse> wrapperCall = new SpliceRetryingCaller<>(callable);
		SpliceRetryingCallerFactory.SpliceRpcRetryingCaller<ClientProtos.CoprocessorServiceResponse> caller = rpcFactory.newCaller();
		return callWithoutRetries(caller, wrapperCall, responsePrototype);
	}

	private Message callWithoutRetries(SpliceRetryingCallerFactory.SpliceRpcRetryingCaller<ClientProtos.CoprocessorServiceResponse> caller,
									   SpliceRetryingCall<ClientProtos.CoprocessorServiceResponse> callable,
									   Message responsePrototype) throws IOException{
		try{
			ClientProtos.CoprocessorServiceResponse result = caller.callWithoutRetries(callable);
			Message response;
			HBaseProtos.NameBytesPair nbPair=result.getValue();
			if (nbPair.hasValue()) {
				response = responsePrototype.newBuilderForType().mergeFrom(nbPair.getValue()).build();
			} else {
				response = responsePrototype.getDefaultInstanceForType();
			}
			lastRegion = result.getRegion().getValue().toByteArray();
			if (LOG.isTraceEnabled()) {
				LOG.trace("Result is region=" + Bytes.toStringBinary(lastRegion) + ", value=" + response);
			}

			return response;
		}catch(Throwable t){
			t = translateException(t);
			if(t instanceof IOException) throw (IOException)t;
			else throw new RuntimeException(t);
		}
	}

	private Throwable translateException(Throwable t) throws IOException {
        /*
         * Convenience error interpreter taken from HConnectionImplementation because the method isn't
         * public. Probably should move it to a more centralized, more easily dealt with scenario, but
         * this way we replicate Connection behavior more intelligently.
         */
		if (t instanceof UndeclaredThrowableException) {
			t = t.getCause();
		}
		if (t instanceof RemoteException) {
			RemoteException re = (RemoteException)t;
			t = re.unwrapRemoteException();
		}
		if (t instanceof DoNotRetryIOException) {
			throw (DoNotRetryIOException)t;
		}
		return t;
	}

	public byte[] getRegionName() {
		return lastRegion;
	}
}