package com.splicemachine.si.impl.data;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.MutationStatus;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class NoOpConstraintChecker implements ConstraintChecker{
    private final OperationStatusFactory opFactory;

    public NoOpConstraintChecker(OperationStatusFactory opFactory){
        this.opFactory=opFactory;
    }

    @Override
    public MutationStatus checkConstraint(KVPair mutation,DataResult existingRow) throws IOException{
        return opFactory.success();
    }
}
