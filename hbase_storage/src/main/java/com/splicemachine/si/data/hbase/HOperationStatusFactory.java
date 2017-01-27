/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.data.hbase;

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.storage.DataResult;
import com.splicemachine.storage.HMutationStatus;
import com.splicemachine.storage.MutationStatus;
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
        public MutationStatus checkConstraint(KVPair mutation,DataResult existingRow) throws IOException{
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

