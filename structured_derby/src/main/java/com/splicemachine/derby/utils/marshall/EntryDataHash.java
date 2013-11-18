package com.splicemachine.derby.utils.marshall;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.storage.EntryEncoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class EntryDataHash extends BareKeyHash implements DataHash{
		protected EntryEncoder entryEncoder;
		protected ExecRow currentRow;
		protected BitSet notNullFields;

		public EntryDataHash(int[] keyColumns, boolean[] keySortOrder) {
				super(keyColumns, keySortOrder,true);
		}

		@Override
		public void setRow(ExecRow rowToEncode) {
				this.currentRow = rowToEncode;
		}

		@Override
		public byte[] encode() throws StandardException, IOException {
				if(entryEncoder==null)
						entryEncoder = buildEntryEncoder();

				entryEncoder.reset(getNotNullFields(currentRow,notNullFields));

				pack(entryEncoder.getEntryEncoder(),currentRow);
				return entryEncoder.encode();
		}

		protected EntryEncoder buildEntryEncoder() {
				int nCols = currentRow.nColumns();
				notNullFields = getNotNullFields(currentRow,new BitSet(nCols));
				DataValueDescriptor[] fields = currentRow.getRowArray();
				BitSet scalarFields = new BitSet(nCols);
				BitSet floatFields = new BitSet(nCols);
				BitSet doubleFields = new BitSet(nCols);
				int i=0;
				for(DataValueDescriptor field:fields){
						if(DerbyBytesUtil.isScalarType(field))
								scalarFields.set(i);
						else if(DerbyBytesUtil.isFloatType(field))
								floatFields.set(i);
						else if(DerbyBytesUtil.isDoubleType(field))
								doubleFields.set(i);
						i++;
				}
				return EntryEncoder.create(SpliceDriver.getKryoPool(),nCols,notNullFields,scalarFields,floatFields,doubleFields);
		}

		protected BitSet getNotNullFields(ExecRow row,BitSet notNullFields) {
				notNullFields.clear();
				int i=0;
				for(DataValueDescriptor dvd:row.getRowArray()){
						if(!dvd.isNull())
								notNullFields.set(i);
						i++;
				}
				return notNullFields;
		}

		@Override
		public KeyHashDecoder getDecoder() {
				return null;  //To change body of implemented methods use File | Settings | File Templates.
		}

		public void close() throws IOException {
				if(entryEncoder!=null)
						entryEncoder.close();
		}
}
