package com.splicemachine.si.impl;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;

/**
 * Uses an HTable and Coprocessors to fetch transaction views.
 *
 * @author Scott Fines
 * Date: 6/20/14
 */
public class HTableTxnAccess implements TxnAccess {

		private final TxnAccess externalStore;

		private final HTableInterfaceFactory tableFactory;
		public HTableTxnAccess(TxnAccess externalStore, HTableFactory tableFactory) {
				this.externalStore = externalStore;
				this.tableFactory = tableFactory;
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				HTableInterface table = tableFactory.createHTableInterface(SpliceConstants.config,SpliceConstants.TRANSACTION_TABLE_BYTES);

				try{
						byte[] rowKey = TxnUtils.getRowKey(txnId);
						Get get = new Get(rowKey);

						Result result = table.get(get);
						assert result!=null : "No transaction found for id "+ txnId;

						KeyValue data = result.getColumnLatest(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);
						MultiFieldDecoder decoder = MultiFieldDecoder.wrap(data.getBuffer(), data.getValueOffset(), data.getValueLength());
						/*
						 * Data is encoded using the TxnUtils.encode(Txn) method, which
						 * is as follows
						 * (beginTimestamp,parentTxnId,isDependent,isAdditive,isolationLevel,commitTimestamp,globalCommitTimestamp,state)
						 *
						 * However, some of those values may be null, so we use a lookup Inheriting Txn view
						 */
						long beginTs = decoder.decodeNextLong();
						long parentTxnId = -1l;
						if(!decoder.nextIsNull()) parentTxnId = decoder.decodeNextLong();
						else decoder.skip();

						boolean hasDependent = !decoder.nextIsNull();
						boolean dependent = false;
						if(hasDependent)
								dependent = decoder.decodeNextBoolean();
						else decoder.skip();

						boolean hasAdditive = !decoder.nextIsNull();
						boolean additive = false;
						if(hasAdditive)
								additive = decoder.decodeNextBoolean();
						else decoder.skip();

						Txn.IsolationLevel level = null;
						if(!decoder.nextIsNull())
								level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
						else decoder.skipLong();

						long commitTs = -1l;
						if(!decoder.nextIsNull())
								commitTs = decoder.decodeNextLong();
						else decoder.skipLong();

						long globalCommitTs = -1l;
						if(!decoder.nextIsNull())
								globalCommitTs = decoder.decodeNextLong();
						else decoder.skipLong();

						Txn.State state = Txn.State.decode(decoder.array(),decoder.offset(),decoder.skipLong());

						Txn parentTxn = parentTxnId>0? externalStore.getTransaction(parentTxnId): Txn.ROOT_TRANSACTION;

						return new InheritingTxnView(parentTxn,txnId,beginTs,level,hasDependent,dependent,hasAdditive,additive,true,true,
										commitTs,globalCommitTs,state);
				}finally{
						tableFactory.releaseHTableInterface(table);
				}
		}

		@Override public boolean transactionCached(long txnId) { return false; }

		@Override
		public void cache(Txn toCache) {
				//no-op
		}
}
