package com.splicemachine.derby.impl.sql.execute.operations;

import org.apache.derby.iapi.sql.execute.ExecRow;

public interface HashMerger<K,T extends ExecRow> {
   void merge(HashBuffer<K,T> currentRows, T currentRow, T nextRow);
   T shouldMerge(HashBuffer<K,T> currentRows, K key);
}
