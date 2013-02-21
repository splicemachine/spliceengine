package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.common.cache.Cache;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.utils.SIUtils;

public class SICompactionScanner implements InternalScanner {
    private InternalScanner internalScanner;
    private Cache<Long,Transaction> transactionalCache;
    
    public SICompactionScanner(InternalScanner internalScanner, Cache<Long,Transaction> transactionalCache) {
       this.internalScanner = internalScanner;
       this.transactionalCache = transactionalCache;
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
       return next(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> result, int limit) throws IOException {
       boolean moreRows = false;
       List<KeyValue> raw = new ArrayList<KeyValue>(limit);
       while (limit == -1 || result.size() < limit) {
          moreRows = internalScanner.next(raw, limit);
          if (raw.size() == 0) {
          	return moreRows;
          }
          for (KeyValue kv : raw) {
          	if (SIUtils.isEmptyCommitTimestamp(kv))	{
          		Transaction transaction = Transaction.readTransaction(kv.getTimestamp(), transactionalCache);
      			switch (transaction.getTransactionState()) {
					case ABORT:
					case ERROR:
						next(result,limit);
						break;
					case ACTIVE:
						break;
					case COMMIT:
	            		KeyValue keyValue = new KeyValue(kv.getRow(),kv.getFamily(),kv.getQualifier(),kv.getTimestamp(),Bytes.toBytes(transaction.getCommitTimestamp()));
	            		result.add(keyValue);
	            		
						break;
					default:
						break;
      			
      			}

          	} else {
	                 result.add(kv);		            		
          	}
          }
       }
       return moreRows;
    }

    @Override
    public void close() throws IOException {
       internalScanner.close();
    }
    
 }
