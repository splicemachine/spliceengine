package com.splicemachine.derby.impl.store.access.hbase;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.store.access.Qualifier;
import org.apache.derby.iapi.types.J2SEDataValueFactory;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.log4j.Logger;

public class HBaseQualifierFilter extends FilterBase {
	private static Logger LOG = Logger.getLogger(HBaseQualifierFilter.class);
	private boolean filter = false;
	protected static J2SEDataValueFactory dvf = new J2SEDataValueFactory();
	protected Qualifier[][] qualifier;
	protected int[] format_ids;
	protected int[] collation_ids;

	static {
		try {
			dvf.boot(true, new Properties());
//			dvf.getNull(formatId, collationType)
		} catch (StandardException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HBaseQualifierFilter() {
	}

	public HBaseQualifierFilter(final Qualifier[][] qualifier,int[] format_ids, int[] collation_ids) {
		this.qualifier = qualifier;
		this.format_ids = format_ids;
		this.collation_ids = collation_ids;
	}


	@Override
	public void reset() {
		this.filter = false;
	}

	@Override
	public void filterRow(List<KeyValue> keyValues) {

	}

	@Override
	public boolean filterRow() {
		return this.filter;
	}
	
	
	/**
	 * 
	 * Read fields and set bits from binary stream to initialize local variables 
	 * and configure operations.
	 * 
	 * @param input binary stream
	 * 
	 */
	@Override
	public void readFields(final DataInput in) throws IOException {  
		/*
		this.columnFamily = Bytes.toString(Bytes.readByteArray(in));
		this.columnQualifier = Bytes.toString(Bytes.readByteArray(in));
		this.value = Bytes.readByteArray(in);
		if(this.value.length == 0) {
			this.value = null;
		}
		this.compareOp = CompareOp.valueOf(in.readUTF());
		this.index = Index.toIndex(Bytes.toString(Bytes.readByteArray(in)));
		this.icol = index.getIndexColumn(columnFamily, columnQualifier);
		this.foundColumn = in.readBoolean();
		this.matchedColumn = in.readBoolean();
		this.filterIfMissing = in.readBoolean();
		this.latestVersionOnly = in.readBoolean();
		*/
	}

	/**
	 * 
	 * write fields and set bits to binary stream.
	 * 
	 * @param output binary stream
	 */
	@Override
	public void write(final DataOutput out) throws IOException {
		out.writeInt(format_ids.length);
		out.writeInt(collation_ids.length);
		//out.wri
/*
		if (qualifier == null)
			out.writeInt(v)
		else
			out.write(qualifier.)
*/
		/*Bytes.
		Bytes.writeByteArray(out, Bytes.toBytes(this.columnFamily));
		Bytes.writeByteArray(out, Bytes.toBytes(this.columnQualifier));
		Bytes.writeByteArray(out, this.value);
		out.writeUTF(compareOp.name());
		Bytes.writeByteArray(out, Bytes.toBytes(index.toJSon()));
		out.writeBoolean(foundColumn);
		out.writeBoolean(matchedColumn);
		out.writeBoolean(filterIfMissing);
		out.writeBoolean(latestVersionOnly);
		*/
	}
	

}