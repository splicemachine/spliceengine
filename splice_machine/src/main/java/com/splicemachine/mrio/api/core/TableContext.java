package com.splicemachine.mrio.api.core;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.commons.lang.SerializationUtils;
import org.apache.hadoop.hbase.util.Base64;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.load.ColumnContext;

public class TableContext implements Externalizable {
	protected ColumnContext[] columns;
	protected int[] pkCols;
	protected int[] execRowFormatIds;
	protected long conglomerateId;

	public TableContext() {
		
	}
	
	public TableContext(ColumnContext[] columns, int[] pkCols, int[] execRowFormatIds, long conglomerateId) {
		assert columns!= null && pkCols != null && execRowFormatIds != null: "Null columns or columnOrdering Passed to the TableContext";
		this.columns = columns;
		this.pkCols = pkCols;
		this.execRowFormatIds = execRowFormatIds;
		this.conglomerateId = conglomerateId;
	}

	@Override
	public void writeExternal(ObjectOutput out) throws IOException {
		out.writeInt(columns.length);
		for (ColumnContext column: columns)
			out.writeObject(column);
		out.writeInt(pkCols.length);
		for (int co: pkCols)
			out.writeInt(co);
		out.writeInt(execRowFormatIds.length);
		for (int co: execRowFormatIds)
			out.writeInt(co);
		out.writeLong(conglomerateId);
	}

	@Override
	public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
		int length = in.readInt();
		columns = new ColumnContext[length];
		for (int i = 0; i< length; i++)
			columns[i] = (ColumnContext) in.readObject();
		length = in.readInt();
		pkCols = new int[length];
		for (int i = 0; i< length; i++)
			pkCols[i] = in.readInt();
		length = in.readInt();
		execRowFormatIds = new int[length];
		for (int i = 0; i< length; i++)
			execRowFormatIds[i] = in.readInt();
		conglomerateId = in.readLong();
	}
	
	public static TableContext getTableContextFromBase64String(String base64String) throws IOException, StandardException {
		return (TableContext) SerializationUtils.deserialize(Base64.decode(base64String));
	}
	
	public String getTableContextBase64String() throws IOException, StandardException {
		return Base64.encodeBytes(SerializationUtils.serialize(this));
	}
	
}
