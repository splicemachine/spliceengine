package com.splicemachine.si.impl.functions;

import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.Record;
import org.spark_project.guava.base.Function;
import javax.annotation.Nullable;
import java.util.concurrent.locks.Lock;

/**
 *
 * Obtain Locks on Records, return Lock[] and modify finalStatus in place.
 *
 */
public class ObtainLocks implements Function<Record[],Lock[]> {
    private Partition table;
    private  MutationStatus[] finalStatus;
    private final OperationStatusFactory operationStatusLib;


    public ObtainLocks(Partition table, MutationStatus[] finalStatus,OperationStatusFactory operationStatusLib) {
        this.table = table;
        this.finalStatus = finalStatus;
        this.operationStatusLib = operationStatusLib;
    }

    @Nullable
    @Override
    public Lock[] apply(Record[] records) {
        Lock[] locks = new Lock[records.length];
        try {
            for (int i = 0; i < records.length; i++) {
                Lock lock = table.getRowLock(records[i]);
                if(lock.tryLock())
                    locks[i]=lock;
                else
                    finalStatus[i]=operationStatusLib.notRun();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return locks;
    }
}
