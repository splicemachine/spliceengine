package com.splicemachine.si.impl;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Longs;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.utils.CloseableIterator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Iterator specifically designed to safely iterate over the Transaction table, returning
 * transactions in order.
 *
 * When the boolean {@code fillGaps = TRUE}, then this Iterator will attempt to fill gaps
 * in the Transaction table with a transaction that has an ERROR state. This is needed to
 * support Transactional DDL operations to establish a firm boundary between the start
 * of a DDL statement and the start of new operations.
 *
 * When the boolean {@code filterErrors = TRUE}, then this Iterator will skip transactions
 * that are marked with the ERROR status code. This is so that when iterators look at the
 * transaction table, they can avoid seeing transactions that were intentionally written
 * there by the Transactional DDL operation.
 *
 * A "Gap" in the Transaction table is defined by a Transaction that is POSSIBLE to be created,
 * but is missing for some reason. For example, if Transaction {@code Ta} was started at time 1,
 * and Transaction {@code Tb} was started at 3, then there <em>could</em> be a transaction that
 * was started at time 2. However, if {@code Ta} was committed at time 2, then no transaction
 * could possibly have a begin time of 2. On the other hand, if {@code Ta} committed at time 4,
 * then time 2 is still a possible transaction, and will be replaced with an ERROR status if
 * {@code fillGaps = TRUE}
 *
 * @author Scott Fines
 * Date: 2/21/14
 */
public class TransactionTableIterator<Table,Scan> implements CloseableIterator<Transaction> {
		private final boolean fillGaps;
		private final boolean filterErrors;

		private final long startTransaction;
		private final EncodedTransactionSchema txnSchema;
		private final SDataLib dataLib;
		private final Table transactionTable;
		private final STableReader reader;
		private final TransactionStore store;

		private CloseableIterator<Result>[] scanners;
		private PeekingIterator<Transaction>[] mergeIterators;

		private Transaction previousTxn; //for detecting a gap

		private final Function<Result,Transaction> decoder = new Function<Result, Transaction>() {
				@Override
				public Transaction apply(@Nullable Result input) {
						assert input!=null : "No Result to decode!";
						byte[] tId = input.getValue(txnSchema.siFamily,txnSchema.idQualifier);
						assert tId!=null: "No transaction id found!";

						long txnId = Bytes.toLong(tId);
						try {
								return SIEncodingUtils.decodeTransactionResults(txnId,input,txnSchema,store);
						} catch (IOException e) {
								throw new RuntimeException(e);
						}
				}
		};
		private Iterator<Transaction> filledGaps;
		private Transaction next;
		private static final Predicate<Transaction> nonErrorPredicate = new Predicate<Transaction>() {
				@Override
				public boolean apply(@Nullable Transaction input) {
						assert input != null;
						return input.getStatus()!=TransactionStatus.ERROR;
				}
		};

		public TransactionTableIterator(boolean fillGaps,
																		boolean filterErrors,
																		long startTransaction,
																		EncodedTransactionSchema txnSchema,
																		SDataLib dataLib,
																		Table transactionTable,
																		STableReader reader,
																		TransactionStore store) {
				this.fillGaps = fillGaps;
				this.filterErrors = filterErrors;
				this.startTransaction = startTransaction;
				this.txnSchema = txnSchema;
				this.dataLib = dataLib;
				this.transactionTable = transactionTable;
				this.reader = reader;
				this.store = store;
		}

		@Override
		public void close() throws IOException {
				if(scanners!=null){
						for(CloseableIterator<Result> scanner:scanners){
								scanner.close();
						}
				}
		}

		/**
		 * @throws java.lang.RuntimeException wrapping an IOException if something goes wrong
		 * @return true if the iterator has another element, false otherwise
		 */
		@Override
		public boolean hasNext() {
				if(scanners==null){
						try {
								makeScanners();
						} catch (IOException e) {
								throw new RuntimeException(e);
						}
				}
				if(filledGaps!=null && filledGaps.hasNext()){
						next = filledGaps.next();
						return true;
				}

				Transaction minTxn = null;
				Iterator<Transaction> minIterator = null;
				for(PeekingIterator<Transaction> txnIterator:mergeIterators){
						if(!txnIterator.hasNext()) continue;
						Transaction txn = txnIterator.peek();

						if(minTxn==null || Longs.compare(minTxn.getLongTransactionId(),txn.getLongTransactionId())>0){
								minTxn = txn;
								minIterator = txnIterator;
						}
				}
				if(minTxn==null) return false;

				if(fillGaps && previousTxn!=null){
						try {
								filledGaps = fillGaps(previousTxn,minTxn);
								if(filledGaps.hasNext()){
										minIterator = filledGaps;
								}
						} catch (IOException e) {
								throw new RuntimeException(e);
						}
				}

				next = minIterator.next();
				return true;
		}


		@Override
		public Transaction next() {
				if(next==null) {
						if(!hasNext()) throw new NoSuchElementException();
				}
				previousTxn = next;
				next = null;
				return previousTxn;
		}


		@Override public void remove() { throw new UnsupportedOperationException(); }

		@SuppressWarnings("unchecked")
		private void makeScanners() throws IOException {
				//noinspection unchecked
				scanners = new CloseableIterator[SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT];
				mergeIterators = new PeekingIterator[SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT];
				for(byte i=0;i<SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT;i++){
						final byte[] startKey = dataLib.newRowKey(new Object[]{i,startTransaction});
						final byte[] endKey = i==SpliceConstants.TRANSACTION_TABLE_BUCKET_COUNT-1 ? null: dataLib.newRowKey(new Object[]{(byte)(i+1)});
						final Scan scan = (Scan)dataLib.newScan(startKey,endKey,null,null,null);
						CloseableIterator<Result> iterator = reader.scan(transactionTable, scan);
						scanners[i] = iterator;
						Iterator<Transaction> baseIterator = Iterators.transform(iterator, decoder);
						if(filterErrors)
								baseIterator = Iterators.filter(baseIterator,nonErrorPredicate);
						mergeIterators[i] = Iterators.peekingIterator(baseIterator);
				}
		}

		@SuppressWarnings("unchecked")
		private Iterator<Transaction> fillGaps(Transaction previousTxn, Transaction minTxn) throws IOException {
			/*
			 * Find all the actual gaps between the two transactions (possible ids that do not exist). Of course,
			 * there's a potential that those transactions WERE created between our first read and this fill, so if
			 * the checkAndPut doesn't succeed, we need to re-read that transaction id
			 */
				long startTxn = previousTxn.getLongTransactionId();
				long stopTxn = minTxn.getLongTransactionId();
				//it's reasonable to assume that you won't have more than 2^32 missing transactions without a problem elsewhere
				int numPossibleGaps = (int)(stopTxn-startTxn) -1;

				Long pTCommit = previousTxn.getEffectiveCommitTimestamp();
				if(pTCommit!=null && pTCommit<stopTxn){
						numPossibleGaps--; //remove the transaction that might have started with previousTxn's commitTimestamp
				}
				Long pGTCommit = previousTxn.getGlobalCommitTimestamp();
				if(pGTCommit!=null && pGTCommit<stopTxn){
						numPossibleGaps--;
				}
				if(numPossibleGaps<=0)
						return Iterators.emptyIterator();

				//fill the gaps
				List<Transaction> transactions = Lists.newArrayListWithCapacity(numPossibleGaps);
				long txnId = startTxn+1;
				final TransactionParams missingParams = new TransactionParams(Transaction.rootTransaction.getTransactionId(),
								true, false, false, false, false);
				while(txnId<stopTxn){
						if(pTCommit!=null && txnId == pTCommit) {
								txnId++;
								continue;
						}else if(pGTCommit!=null && txnId == pGTCommit){
								txnId++;
								continue;
						}

						boolean success = store.recordNewTransactionDirect(transactionTable, txnId, missingParams, TransactionStatus.ERROR, txnId, 0);
						if(!success)
								transactions.add(store.getTransaction(txnId));
						else
								transactions.add(new Transaction(StubTransactionBehavior.instance,
												txnId, txnId,
												System.currentTimeMillis(), //keepAlive
												Transaction.rootTransaction,
												true, false, false, false, false,
												TransactionStatus.ERROR,
												null, null, 0l));
						txnId++;
				}

				if(filterErrors)
						return Iterators.filter(transactions.iterator(),nonErrorPredicate);
				else
						return transactions.iterator();
		}
}
