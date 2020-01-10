/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.mrio.api.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.SQLException;

import com.splicemachine.db.client.am.SQLExceptionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.hadoop.io.Writable;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

public class ExecRowWritable implements Writable{

	DescriptorSerializer[] serializers = null;
	MultiFieldDecoder decoder = MultiFieldDecoder.create();
	MultiFieldEncoder encoder = null;
	
	ExecRow value = null;
	byte[] bytes = null;
	int length = 0;
	private ExecRow row;

	public ExecRowWritable(ExecRow row) {
	}
	
	public byte[] translate2byte(ExecRow row) throws StandardException
	{
		if(row == null)
			return null;
		DataValueDescriptor[] fields = row.getRowArray();
		if(fields.length == 0)
			return null;
		if(serializers == null) {
			throw StandardException.newException(
			SQLState.AMBIGIOUS_PROTOCOL);
		}
		if(encoder == null){
			encoder = MultiFieldEncoder.create(fields.length);
		}
		else{encoder.reset();}
		for (int i = 0; i < fields.length; i++) {
			if (fields[i] == null)
				continue;
			serializers[i].encode(encoder, fields[i], false);
																
		}
		
		return encoder.build();
	}
	
	private ExecRow constructEmptyExecRow() throws StandardException {
		return row.getClone();
	}
	
	public ExecRow translateFromBytes(byte[] row) throws StandardException
	{
		ExecRow execRow = constructEmptyExecRow();
		
		if(execRow == null)
			return null;
		DataValueDescriptor[] fields = execRow.getRowArray();
		if(serializers == null) {
			throw StandardException.newException(
			SQLState.AMBIGIOUS_PROTOCOL);
		}
		decoder.set(row);
		
        for(int i = 0; i < fields.length; i++){
        		if(fields[i] == null)
        			continue;
        		serializers[i].decode(decoder, fields[i], false); 
        }
    
		ExecRow afterDecoding = execRow.getNewNullRow();
		afterDecoding.setRowArray(fields);
		return afterDecoding;
	}
	
	public ExecRow get()
	{
		return value;
	}
	
	public void set(ExecRow row) 
	{
		value = row;
		if(row == null)
		{
			length = 0;
			return;
		}
		
		try {
			bytes = translate2byte(row);
			if(bytes == null)
			{
				length = 0;
				return;
			}
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			throw new RuntimeException("should not have happened", e);
		}
		length = bytes.length;
	}
	
	public byte[] getBytes()
	{
		return bytes;
	}
	
	public int getLength()
	{
		return length;
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		//Log.debug("readFields in ExecRowWrapper, read length:"+String.valueOf(in.readInt()));
		
		int totalBuffer = in.readInt();
		
		if(totalBuffer == 0)
		{
			value = null;
			return;
		}
		byte[] b = new byte[totalBuffer]; 
		in.readFully(b);
		try {
			this.value = translateFromBytes(b);
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override
	public void write(DataOutput out) throws IOException {		
		if(value == null)
			out.writeInt(0);
		else
		{
			out.writeInt(bytes.length);
			out.write(bytes, 0, bytes.length);
		}
	}

	@Override
	public String toString() {
		return value==null?"NULL":value.toString();
	}
	
}
