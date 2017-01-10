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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import javax.annotation.concurrent.ThreadSafe;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 10/27/15
 */
@ThreadSafe
class ByteBufferMapTableLoader implements BroadcastJoinCache.JoinTableLoader{
    public static BroadcastJoinCache.JoinTableLoader INSTANCE = new ByteBufferMapTableLoader();

    private ByteBufferMapTableLoader(){} //singleton class

    @Override
    public JoinTable.Factory load(Callable<Stream<ExecRow>> streamLoader,int[] innerHashKeys,int[] outerHashKeys, ExecRow outerTemplateRow) throws ExecutionException{
        Map<ByteBuffer, List<ExecRow>> table=new HashMap<>();

        try(Stream<ExecRow> innerRows=streamLoader.call()){
            ExecRow right;
            while((right=innerRows.next())!=null){
                if(innerSerializers==null){
                    innerSerializers=VersionedSerializers.latestVersion(false).getSerializers(right);
                    innerKeyEncoder=new KeyEncoder(NoOpPrefix.INSTANCE,
                            BareKeyHash.encoder(innerHashKeys,null,innerSerializers),NoOpPostfix.INSTANCE);
                }

                ByteBuffer key=ByteBuffer.wrap(innerKeyEncoder.getKey(right));
                List<ExecRow> rows=table.get(key);
                if(rows==null){
                    rows=new ArrayList<>(1);
                    table.put(key,rows);
                }
                rows.add(right.getClone());
            }
        }catch(StreamException e){
           throw new ExecutionException(e.getCause());
        }catch(Exception e){
            if(e instanceof ExecutionException) throw (ExecutionException)e;
            else throw new ExecutionException(e);
        }

        return new ByteBufferMappedJoinTable.Factory(table,outerHashKeys,outerTemplateRow);
    }
}
