package com.splicemachine.derby.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage.DropIndexRequest;
import com.splicemachine.coprocessor.SpliceMessage.DropIndexResponse;
import com.splicemachine.coprocessor.SpliceMessage.SpliceIndexManagementService;

/**
 * @author Scott Fines
 * Created on: 3/11/13
 */
public class SpliceIndexManagementEndpoint extends SpliceIndexManagementService implements CoprocessorService, Coprocessor {

	@Override
	public void start(CoprocessorEnvironment env) throws IOException {
	}

	@Override
	public void stop(CoprocessorEnvironment env) throws IOException {
	}

	@Override
	public Service getService() {
		return this;
	}

	@Override
	public void dropIndex(RpcController rpcController, DropIndexRequest dropIndexRequest,RpcCallback<DropIndexResponse> callback) {
			SpliceIndexEndpoint.factoryMap.get(dropIndexRequest.getBaseConglomId()).getFirst().dropIndex(dropIndexRequest.getIndexConglomId());
			DropIndexResponse.Builder dropIndexResponse = DropIndexResponse.newBuilder();
			callback.run(dropIndexResponse.build());
	}
}
