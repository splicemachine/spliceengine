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

package com.splicemachine.mapreduce;

import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 2/28/14
 */
public class HBaseBulkLoadReducer extends Reducer<ImmutableBytesWritable,
        ImmutableBytesWritable, ImmutableBytesWritable, KeyValue>{

    private boolean throwErrorOnDuplicates;
    private long txnId;
    private Counter rowCounter;

    protected void reduce(ImmutableBytesWritable row,java.lang.Iterable<ImmutableBytesWritable> kvs,
                          Reducer<ImmutableBytesWritable, ImmutableBytesWritable, ImmutableBytesWritable, KeyValue>.Context context)
            throws java.io.IOException, InterruptedException{
                /*
				 * In a good world, we would have NO duplicates--that is, each row will have 1 and only
				 * 1 associated value with it. However, there are situations (unique constraint violations)
				 * where this may not be true. In those cases, we will use the throwErrorOnDuplicates
				 * to determine whether or not to explode.
				 */
        boolean seen=false;
        try{
            for(ImmutableBytesWritable value : kvs){
                byte[] rowBytes=row.copyBytes();
                if(seen){
                    if(throwErrorOnDuplicates)
                        throw new IOException("Duplicate entries detected for key "+Bytes.toStringBinary(rowBytes));
                }else seen=true;

                KeyValue dataKv=new KeyValue(rowBytes,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,txnId,value.get());
                context.write(row,dataKv);
                rowCounter.increment(1);
            }
        }catch(Throwable t){
            t.printStackTrace();
            throw new IOException(t);
        }
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException{
        super.setup(context);
        rowCounter=context.getCounter("import","rows");
        throwErrorOnDuplicates=context.getConfiguration().getBoolean("import.throwErrorOnDuplicate",true);
        String txnIdStr=context.getConfiguration().get("import.txnId");
        if(txnIdStr==null)
            throw new IOException("No Transaction id found, unable to create data!");
        try{
            txnId=Long.parseLong(txnIdStr);
        }catch(NumberFormatException nfe){
            throw new IOException("Unknown format for Transaction Id: "+txnIdStr);
        }
    }
}
