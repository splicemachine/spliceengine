/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.HMutationStatus;
import com.splicemachine.storage.MutationStatus;
import com.splicemachine.storage.Record;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.NoSuchColumnFamilyException;
import org.apache.hadoop.hbase.regionserver.OperationStatus;
import java.io.IOException;

/**
 * HBase implementation of an OperationStatus factory
 * Created by jleach on 12/14/15.
 */
public class HOperationStatusFactory implements OperationStatusFactory{
    public static final HOperationStatusFactory INSTANCE = new HOperationStatusFactory();

    private static final ConstraintChecker NO_OP_CHECKER = new ConstraintChecker(){
        @Override
        public MutationStatus checkConstraint(Record mutation, Record existingRow) throws IOException{
            return HMutationStatus.success();
        }
    };

    private HOperationStatusFactory(){}

    @Override
    public boolean processPutStatus(MutationStatus operationStatus) throws IOException{
        OperationStatus opStat = ((HMutationStatus)operationStatus).unwrapDelegate();
         switch (opStat.getOperationStatusCode()) {
            case NOT_RUN:
                throw new IOException("Could not acquire Lock");
            case BAD_FAMILY:
                throw new NoSuchColumnFamilyException(opStat.getExceptionMsg());
            case SANITY_CHECK_FAILURE:
                throw new IOException("Sanity Check failure:" + opStat.getExceptionMsg());
            case FAILURE:
                throw new IOException(opStat.getExceptionMsg());
            default:
                return true;
         }
    }

    @Override
    public MutationStatus getCorrectStatus(MutationStatus status,MutationStatus oldStatus){
        switch (((HMutationStatus)oldStatus).unwrapDelegate().getOperationStatusCode()) {
            case SUCCESS:
                return status;
            case NOT_RUN:
            case BAD_FAMILY:
            case SANITY_CHECK_FAILURE:
            case FAILURE:
                return oldStatus;
        }
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
    public MutationStatus failure(String message){
        return new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.FAILURE,message));
    }

    @Override
    public MutationStatus failure(Throwable t){
        if (t instanceof IOException) {
            return new HMutationStatus(new ExtendedOperationStatus(HConstants.OperationStatusCode.FAILURE, (IOException) t));
        } else {
            return new HMutationStatus(new OperationStatus(HConstants.OperationStatusCode.FAILURE, t.getMessage()));
        }
    }

    @Override
    public ConstraintChecker getNoOpConstraintChecker(){
        return NO_OP_CHECKER;
    }

}

