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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.JoinTable;
import com.splicemachine.derby.utils.marshall.BareKeyHash;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.NoOpPostfix;
import com.splicemachine.derby.utils.marshall.NoOpPrefix;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
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

        DescriptorSerializer[] innerSerializers=null;
        KeyEncoder innerKeyEncoder=null;

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
