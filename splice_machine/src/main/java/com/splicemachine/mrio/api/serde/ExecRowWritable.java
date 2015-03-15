package com.splicemachine.mrio.api.serde;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;

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
	private int[] colTypes;
	
	public ExecRowWritable(int[] colTypes)
	{
		this.colTypes = colTypes;
	}
	
	public byte[] translate2byte(ExecRow row) throws StandardException
	{
		if(row == null)
			return null;
		DataValueDescriptor[] fields = row.getRowArray();
		if(fields.length == 0)
			return null;
		if(serializers == null)
			serializers = VersionedSerializers.latestVersion(true).getSerializers(row);
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
	
	private ExecRow constructEmptyExecRow() throws StandardException
	{
		ExecRow row = new ValueRow(colTypes.length);
		DataValueDescriptor[] data = createDVD();
		row.setRowArray(data);
		return row;
	}
	
	public ExecRow translateFromBytes(byte[] row) throws StandardException
	{
		ExecRow execRow = constructEmptyExecRow();
		
		if(execRow == null)
			return null;
		DataValueDescriptor[] fields = execRow.getRowArray();
		if(serializers == null)
			serializers = VersionedSerializers.latestVersion(true).getSerializers(fields);
		
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

	private DataValueDescriptor[] createDVD() throws StandardException {
		DataValueDescriptor dvds[] = new DataValueDescriptor[colTypes.length];
		for(int pos = 0; pos < colTypes.length; pos++)
		{
			dvds[pos] = DataTypeDescriptor.getBuiltInDataTypeDescriptor(colTypes[pos]).getNull();
		}
		return dvds;
	}

	@Override
	public String toString() {
		return value==null?"NULL":value.toString();
	}
	
}
