package com.splicemachine.si.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.SIObserver;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HbRegion;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collection;

/**
 * Base implementation of a TransactionalRegion
 * @author Scott Fines
 * Date: 7/1/14
 */
public class TxnRegion implements TransactionalRegion {
		private final HRegion region;
		private final RollForward rollForward;
		private final ReadResolver readResolver;

		private final TxnSupplier txnSupplier;

		/*Old API classes..eventually, we will replace them with better designs*/
		private final DataStore dataStore;
		private final SDataLib dataLib;
		private final Transactor transactor;
		private final HbRegion hbRegion;

		private final boolean transactionalWrites; //if false, then will use straightforward writes

		public TxnRegion(HRegion region,
										 RollForward rollForward,
										 ReadResolver readResolver,
										 TxnSupplier txnSupplier,
										 DataStore dataStore,
										 SDataLib dataLib,
										 Transactor transactor) {
				this.region = region;
				this.rollForward = rollForward;
				this.readResolver = readResolver;
				this.txnSupplier = txnSupplier;
				this.dataStore = dataStore;
				this.dataLib = dataLib;
				this.transactor = transactor;
				this.hbRegion = new HbRegion(region);

				this.transactionalWrites = SIObserver.doesTableNeedSI(region.getTableDesc().getNameAsString());
		}

		@Override
		public TxnFilter unpackedFilter(Txn txn) throws IOException {
				return new SimpleTxnFilter(txnSupplier,txn,readResolver,dataStore);
		}

		@Override
		public TxnFilter packedFilter(Txn txn, EntryPredicateFilter predicateFilter, boolean countStar) throws IOException {
				return new PackedTxnFilter(unpackedFilter(txn),new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
		}

		@Override
		public DDLFilter ddlFilter(Txn ddlTxn) throws IOException {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		@Override
		public SICompactionState compactionFilter() throws IOException {
				return new SICompactionState(dataStore,txnSupplier);
		}

		@Override
		public boolean rowInRange(byte[] row) {
				return HRegion.rowIsInRange(region.getRegionInfo(),row);
		}

		@Override
		public boolean isClosed() {
				return region.isClosed()||region.isClosing();
		}

		@Override
		public boolean containsRange(byte[] start, byte[] stop) {
				return HRegionUtil.containsRange(region, start, stop);
		}

		@Override
		public String getTableName() {
				return region.getTableDesc().getNameAsString();
		}

		@Override
		public void updateWriteRequests(long writeRequests) {
				HRegionUtil.updateWriteRequests(region, writeRequests);
		}

		@Override
		public void updateReadRequests(long readRequests) {
				HRegionUtil.updateReadRequests(region,readRequests);
		}

		@Override
		@SuppressWarnings("unchecked")
		public OperationStatus[] bulkWrite(Txn txn,
																			 byte[] family, byte[] qualifier,
																			 ConstraintChecker constraintChecker, //TODO -sf- can we encapsulate this as well?
																			 Collection<KVPair> data) throws IOException {
				if(transactionalWrites)
						return transactor.processKvBatch(hbRegion,rollForward,txn,family,qualifier,data,constraintChecker);
				else{
						Pair<Mutation, Integer>[] pairsToProcess = new Pair[data.size()];
						int i=0;
						for(KVPair pair:data){
								pairsToProcess[i] = new Pair<Mutation, Integer>(getMutation(pair,txn), null);
								i++;
						}
						return region.batchMutate(pairsToProcess);
				}
		}

		@Override public String getRegionName() { return region.getRegionNameAsString(); }

		private Mutation getMutation(KVPair kvPair, Txn txn) throws IOException {
				assert kvPair.getType()== KVPair.Type.INSERT: "Performing an update/delete on a non-transactional table";
				Put put = new Put(kvPair.getRow());
				put.add(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES,txn.getTxnId(),kvPair.getValue());
				put.setAttribute(SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME, SpliceConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
				return put;
		}
}
