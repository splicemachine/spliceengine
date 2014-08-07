package com.splicemachine.si.api;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.impl.ClientTxnLifecycleManager;
import com.splicemachine.si.impl.KeepAliveScheduler;
import com.splicemachine.si.impl.QueuedKeepAliveScheduler;
import com.splicemachine.si.impl.UnsupportedLifecycleManager;
import com.splicemachine.annotations.ThreadSafe;

/**
 * @author Scott Fines
 * Date: 7/3/14
 */
public class TransactionLifecycle {
		private static final Object lock = new Integer("2");

		private static volatile @ThreadSafe TxnLifecycleManager lifecycleManager;
		private static volatile @ThreadSafe KeepAliveScheduler keepAliveScheduler;

		public static TxnLifecycleManager getLifecycleManager() {
				TxnLifecycleManager tc = lifecycleManager;
				if(tc==null)
						tc = initialize();
				return tc;
		}

		static void setKeepAliveScheduler(@ThreadSafe KeepAliveScheduler scheduler){
				synchronized(lock){
						TxnLifecycleManager tc = lifecycleManager;
						if(tc instanceof ClientTxnLifecycleManager)
								((ClientTxnLifecycleManager)tc).setKeepAliveScheduler(scheduler);

						keepAliveScheduler = scheduler;
				}
		}

		static void setTxnLifecycleManager(@ThreadSafe TxnLifecycleManager tc){
				synchronized (lock){
						lifecycleManager = tc;
				}
		}

		private static TxnLifecycleManager initialize() {
				synchronized (lock){
						TxnLifecycleManager tc = lifecycleManager;
						KeepAliveScheduler ka = keepAliveScheduler;
						if(tc!=null && ka!=null) return tc;

						TxnStore txnStore = TransactionStorage.getTxnStore();
						ClientTxnLifecycleManager lfManager = new ClientTxnLifecycleManager(TransactionTimestamps.getTimestampSource());
						lfManager.setStore(txnStore);

						if(ka==null){
								ka = new QueuedKeepAliveScheduler(SIConstants.transactionKeepAliveInterval/2,
												SIConstants.transactionKeepAliveInterval,4,txnStore);
								keepAliveScheduler = ka;
						}

						lfManager.setKeepAliveScheduler(ka);
						lifecycleManager = lfManager;
						return lfManager;
				}
		}

		/**
		 * Use this method when you want a lifecycle manager which will NOT operate lifecycle stages--e.g.
		 * that cannot commit, rollback, etc.
		 *
		 * This is useful when you are recreating a transaction on the other side of a serialization boundary,
		 * and you want to ensure that we don't accidentally cally lifecycle methods.
		 *
		 * @return a no-op life cycle manager.
		 */
		public static TxnLifecycleManager noOpLifecycleManager() {
				return UnsupportedLifecycleManager.INSTANCE;
		}
}
