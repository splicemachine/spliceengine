/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.si.data.hbase.coprocessor;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.SingletonCoprocessorService;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/26/16
 */
public class SpliceRSRpcServices extends SpliceMessage.SpliceRegionServerCoprocessorService implements SingletonCoprocessorService, Coprocessor {
    private static final Logger LOG=Logger.getLogger(SpliceRSRpcServices.class);

    @Override
    public void start(CoprocessorEnvironment env) throws IOException{
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException{

    }

    @Override
    public Service getService(){
        return this;
    }

    @Override
    public void getOldestActiveTransaction(RpcController controller,
                                           SpliceMessage.SpliceOldestActiveTransactionRequest request,
                                           RpcCallback<SpliceMessage.SpliceOldestActiveTransactionResponse> callback) {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "getOldestActiveTransaction");
        SpliceMessage.SpliceOldestActiveTransactionResponse.Builder writeResponse = SpliceMessage.SpliceOldestActiveTransactionResponse.newBuilder();

        long oldestActiveTransaction = SIDriver.driver().getTxnStore().oldestActiveTransaction();
        writeResponse.setOldestActiveTransaction(oldestActiveTransaction);
        callback.run(writeResponse.build());
    }
}
