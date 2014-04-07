package com.splicemachine.derby.utils.marshall;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class EntryDataHash extends BareKeyHash implements DataHash<ExecRow>{
		protected EntryEncoder entryEncoder;
		protected ExecRow currentRow;
		protected DataValueDescriptor dvds;
		protected KryoPool kryoPool;

		public EntryDataHash(int[] keyColumns, boolean[] keySortOrder,DescriptorSerializer[] serializers) {
				this(keyColumns, keySortOrder, SpliceKryoRegistry.getInstance(),serializers);
		}

		public EntryDataHash(int[] keyColumns, boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers) {
				super(keyColumns, keySortOrder,true,kryoPool,serializers);
				this.kryoPool = kryoPool;
		}

		@Override
		public void setRow(ExecRow rowToEncode) {
				this.currentRow = rowToEncode;
		}

		@Override
		public byte[] encode() throws StandardException, IOException {
				if(entryEncoder==null)
						entryEncoder = buildEntryEncoder();

				int nCols = currentRow.nColumns();
				BitSet notNullFields = new BitSet(nCols);
				entryEncoder.reset(getNotNullFields(currentRow,notNullFields));

				pack(entryEncoder.getEntryEncoder(),currentRow);
				return entryEncoder.encode();
		}

		protected EntryEncoder buildEntryEncoder() {
				int nCols = currentRow.nColumns();
				BitSet notNullFields = getNotNullFields(currentRow,new BitSet(nCols));
				DataValueDescriptor[] fields = currentRow.getRowArray();
				BitSet scalarFields = new BitSet(nCols);
				BitSet floatFields = new BitSet(nCols);
				BitSet doubleFields = new BitSet(nCols);
				int i=0;
				for(DataValueDescriptor field:fields){
						if(DerbyBytesUtil.isScalarType(field, null))
								scalarFields.set(i);
						else if(DerbyBytesUtil.isFloatType(field))
								floatFields.set(i);
						else if(DerbyBytesUtil.isDoubleType(field))
								doubleFields.set(i);
						i++;
				}
				return EntryEncoder.create(kryoPool,nCols,notNullFields,scalarFields,floatFields,doubleFields);
		}

		protected BitSet getNotNullFields(ExecRow row,BitSet notNullFields) {
				notNullFields.clear();
				int i=0;
				for(DataValueDescriptor dvd:row.getRowArray()){
						if(!dvd.isNull()){
								if(keyColumns==null||i>=keyColumns.length || keyColumns[i]!=-1)
										notNullFields.set(i);
						}
						i++;
				}
				return notNullFields;
		}

		@Override
		public KeyHashDecoder getDecoder() {
				return new EntryDataDecoder(keyColumns,keySortOrder,serializers);
		}

		public void close() throws IOException {
				if(entryEncoder!=null)
						entryEncoder.close();
		}
}
