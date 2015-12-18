package com.splicemachine.si.data.hbase;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.HMutationStatus;
import com.splicemachine.storage.MutationStatus;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;

/**
 * HBase implementation of an OperationStatus factory
 * Created by jleach on 12/14/15.
 */
public class HOperationStatusFactory implements OperationStatusFactory{
    private static final ConstraintChecker NO_OP_CHECKER = new ConstraintChecker(){
        @Override
        public MutationStatus checkConstraint(KVPair mutation,DataResult existingRow) throws IOException{
            return HMutationStatus.success();
        }
    };
    @Override
    public boolean processPutStatus(MutationStatus operationStatus) throws IOException{
        return false;
    }

    @Override
    public MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus){
        return null;
    }

    @Override
    public MutationStatus success(){
        return HMutationStatus.success();
    }

    @Override
    public MutationStatus notRun(){
        return HMutationStatus.notRun();
    }

    @Override
    public MutationStatus failure(String messsage){
        return new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.FAILURE,messsage));
    }

    @Override
    public MutationStatus failure(Throwable t){
        return new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.FAILURE,t.getMessage()));
    }

    @Override
    public ConstraintChecker getNoOpConstraintChecker(){
        return NO_OP_CHECKER;
    }
}
