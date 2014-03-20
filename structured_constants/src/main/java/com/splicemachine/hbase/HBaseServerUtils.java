package com.splicemachine.hbase;

import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;
import org.apache.hadoop.hbase.ipc.RpcCallContext;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Utility methods for HBase servers
 *
 * @author Scott Fines
 * Date: 3/20/14
 */
public class HBaseServerUtils {

		private HBaseServerUtils(){}

		public static void checkCallerDisconnect(HRegion region, String task) throws CallerDisconnectedException {
				RpcCallContext currentCall = RpcServer.getCurrentCall();
				if(currentCall!=null){
						long afterTime =  currentCall.disconnectSince();
						if(afterTime>0){
								throw new CallerDisconnectedException(
												"Aborting on region " + region.getRegionNameAsString() + ", call " +
																task + " after " + afterTime + " ms, since " +
																"caller disconnected");
						}
				}
		}
}
