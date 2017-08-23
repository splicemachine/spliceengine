package com.splicemachine.si.impl.functions.isolationlevels;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.Record;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;

/**
 *
 *
 */
public class SnapshotIsolation implements Function<Record[],Record[]> {
    Txn activeTxn;
    public SnapshotIsolation(Txn activeTxn) {
        this.activeTxn = activeTxn;
    }

    @Nullable
    @Override
    public Record[] apply(@Nullable Record[] records) {
        if (records == null)
            return null;
        for (int i = 0; i< records.length; i++) {
            if (records[i] == null)
                continue;
            if (records[i].isResolved()) {
                if (activeTxn.getTxnId() > records[i].getEffectiveTimestamp()) { // Resolved

                }

            }
        }
        return records;
    }
}
