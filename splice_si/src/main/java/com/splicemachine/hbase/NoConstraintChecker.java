package com.splicemachine.hbase;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.server.ConstraintChecker;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.regionserver.OperationStatus;

import java.io.IOException;

/**
 * Created by jleach on 12/9/15.
 */
public class NoConstraintChecker {
    public static final ConstraintChecker NO_CONSTRAINT = new ConstraintChecker<OperationStatus,Result>() {
        private final OperationStatus SUCCESS = new OperationStatus(HConstants.OperationStatusCode.SUCCESS);
        @Override
        public OperationStatus checkConstraint(KVPair mutation, Result existingRow) throws IOException {
            return SUCCESS;
        }
    };

}
