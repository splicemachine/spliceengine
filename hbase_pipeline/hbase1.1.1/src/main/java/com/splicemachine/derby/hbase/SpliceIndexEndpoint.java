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

package com.splicemachine.derby.hbase;

import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.coprocessor.SpliceMessage;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Endpoint to allow special batch operations that the HBase API doesn't explicitly enable
 * by default (such as bulk-processed mutations)
 *
 * @author Scott Fines
 *         Created on: 3/11/13
 */
public class SpliceIndexEndpoint extends BaseSpliceIndexEndpoint implements Coprocessor,CoprocessorService{
    private static final Logger LOG=Logger.getLogger(SpliceIndexEndpoint.class);

    @Override
    @SuppressWarnings("unchecked")
    public void start(final CoprocessorEnvironment env) throws IOException{
        startAction(env);
    }

    @Override
    public void bulkWrite(RpcController controller,
                          SpliceMessage.BulkWriteRequest request,
                          RpcCallback<SpliceMessage.BulkWriteResponse> done){
        try{
            byte[] bytes=bulkWrites(request.getBytes().toByteArray());
            if(bytes==null||bytes.length<=0)
                LOG.error("No bytes constructed for the result!");

            SpliceMessage.BulkWriteResponse response =SpliceMessage.BulkWriteResponse.newBuilder()
                    .setBytes(ZeroCopyLiteralByteString.wrap(bytes)).build();
            done.run(response);
        }catch(IOException e){
            LOG.error("Unexpected exception performing bulk write: ",e);
            ((ServerRpcController)controller).setFailedOn(e);
        }
    }

    @Override
    public Service getService(){
        return this;
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException{
        stopAction(env);
    }
}
