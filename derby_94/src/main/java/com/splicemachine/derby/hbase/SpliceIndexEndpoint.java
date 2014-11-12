package com.splicemachine.derby.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import com.splicemachine.pipeline.coprocessor.BatchProtocol;

public class SpliceIndexEndpoint extends BaseEndpointCoprocessor implements BatchProtocol{
		SpliceBaseIndexEndpoint endpoint;
		
		@Override
		public void start(CoprocessorEnvironment env) {
				endpoint = new SpliceBaseIndexEndpoint();
				endpoint.start(env);
				super.start(env);
		}
		
		@Override
		public void stop(CoprocessorEnvironment env) {
				endpoint.stop(env);
		}

		@Override
		public byte[] bulkWrites(byte[] bulkWrites) throws IOException {
			return endpoint.bulkWrites(bulkWrites);
		}
}