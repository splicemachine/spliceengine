package com.splicemachine.si.impl;

import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.si.data.hbase.IHTable;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;
import java.util.Collection;

/**
 * Region-level transactional writer. Effectively defers to SITransactor for now,
 * but later we may want to change that.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
public class RegionTransactionalWriter implements TransactionalWriter{
		private final RollForward rollForward;
		private final ConstraintChecker constraintChecker;

		private final HbRegion oldSiRegion; //to fit with old API
		private final Transactor<IHTable, Mutation, Put> oldSiTransactor;

		public RegionTransactionalWriter(HRegion region,
																		 RollForward rollForward,
																		 ConstraintChecker constraintChecker) {
				this.rollForward = rollForward;
				this.constraintChecker = constraintChecker;
				this.oldSiRegion = new HbRegion(region);
				this.oldSiTransactor = HTransactorFactory.getTransactor();
		}

		@SuppressWarnings("unchecked")
		@Override
		public OperationStatus[] processKvBatch(byte[] family,
																						byte[] qualifier,
																						Collection<KVPair> mutations,
																						long txnId) throws IOException {
				return oldSiTransactor.processKvBatch(oldSiRegion,
								rollForward,family,qualifier,mutations,txnId,constraintChecker);
		}

		@Override
		public OperationStatus[] processKvBatch(byte[] family, byte[] qualifier,
																						Collection<KVPair> mutations, Txn txn) throws IOException {
				return oldSiTransactor.processKvBatch(oldSiRegion,rollForward,txn,
								family,qualifier,mutations,constraintChecker);
		}
}
