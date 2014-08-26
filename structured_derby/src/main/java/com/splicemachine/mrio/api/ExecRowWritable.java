package com.splicemachine.mrio.api;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDouble;
import org.apache.derby.iapi.types.SQLInteger;
import org.apache.derby.iapi.types.SQLReal;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.io.Writable;
import org.mortbay.log.Log;

import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;

public class ExecRowWritable implements Writable{

	DescriptorSerializer[] serializers = null;
	MultiFieldDecoder decoder = MultiFieldDecoder.create();
	MultiFieldEncoder encoder = null;
	DataValueDescriptor[] data = new DataValueDescriptor[]{
			new SQLInteger(1),
			new SQLVarchar("Hello")};
	ExecRow value = null;
	byte[] bytes = null;
	
	public byte[] translate2byte(ExecRow row) throws StandardException
	{
		DataValueDescriptor[] fields = row.getRowArray();
		if(serializers == null)
			serializers = VersionedSerializers.latestVersion(true).getSerializers(row);
		if(encoder == null)
			encoder = MultiFieldEncoder.create(fields.length);
		for(int i = 0; i < fields.length; i++)
		{	
			if(fields[i] == null)
				continue;
			serializers[i].encode(encoder, fields[i], false); //assume no primary key
		}
		
		return encoder.build();
	}
	
	private ExecRow constructExecRowWithData()
	{
		
		ExecRow row = new ValueRow(data.length);
		row.setRowArray(data);
		return row;
	}
	
	private ExecRow constructEmptyExecRow()
	{
		ExecRow row = new ValueRow(data.length);
		
		return row;
	}
	
	public ExecRow translateFromBytes(byte[] row) throws StandardException
	{
		ExecRow execRow = constructExecRowWithData();
		DataValueDescriptor[] fields = execRow.getRowArray();
		if(serializers == null)
			serializers = VersionedSerializers.latestVersion(true).getSerializers(fields);
		
		decoder.set(row);
		//System.out.println(decoder.decodeNextInt());
		
		for(int i = 0; i < fields.length; i++)
		{
			
			if(fields[i] == null)
				continue;
			serializers[i].decode(decoder, fields[i], false); //assume no primary key
		}
		
		System.out.println(new String(row));
		ExecRow afterDecoding = constructEmptyExecRow();
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
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		Log.debug("readFields in ExecRowWrapper, read length:"+String.valueOf(in.readInt()));
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
		// TODO Auto-generated method stub
		if(value == null)
			out.writeInt(0);
		else
		{
			try {
				byte[] bytes = translate2byte(value);
				out.writeInt(bytes.length);
				out.write(bytes);
			} catch (StandardException e) {
				// TODO Auto-generated catch block
				out.writeInt(0);
				throw new IOException("ExecRowWritable Error in write, "+e.getCause());
				
			}
		}
	}

	public static void main(String args[])
	{
		ExecRowWritable erw = new ExecRowWritable();
		byte[] org;
		try {
			org = erw.translate2byte(erw.constructExecRowWithData());
			ExecRow row = erw.translateFromBytes(org);
			int testint = row.getRowArray()[0].getInt();
			String teststr = row.getRowArray()[1].getString();
			System.out.println(testint);
			System.out.println(teststr);
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

