package com.splicemachine.hbase.table;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;

/**
 * @author Scott Fines
 *         Date: 3/20/14
 */
public class SpliceRpcController implements RpcController {

		private String errorMessage;
		private boolean isCancelled;

		private Throwable t;

		@Override
		public void reset() {
				this.isCancelled = false;
				this.errorMessage = null;
		}

		@Override
		public boolean failed() {
				return errorMessage!=null;
		}

		@Override
		public String errorText() {
				return errorMessage;
		}

		@Override
		public void startCancel() {
				this.isCancelled = true;
		}

		@Override
		public void setFailed(String reason) {
				this.errorMessage = reason;
		}

		@Override
		public boolean isCanceled() {
				return isCancelled;
		}

		@Override
		public void notifyOnCancel(RpcCallback<Object> callback) {
				throw new UnsupportedOperationException("Implement if needed");
		}

		public void setFailed(Throwable t){
				this.t = t;
				this.errorMessage = t.getMessage();
		}

		public Throwable getThrowable(){
				return t;
		}
}
