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

package com.splicemachine.derby.utils.marshall;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.storage.EntryEncoder;
import com.splicemachine.utils.kryo.KryoPool;

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
				if(keyColumns!=null){
						for( int pos:keyColumns){
								if(pos<0) continue;
								DataValueDescriptor field = fields[pos];
								if(field==null) continue;
								DescriptorSerializer serializer = serializers[pos];
								if(serializer.isScalarType())
										scalarFields.set(pos);
								else if(serializer.isFloatType())
										floatFields.set(pos);
								else if(serializer.isDoubleType())
										doubleFields.set(pos);
						}
				}else{
						int i=0;
						for(DataValueDescriptor field:fields){
								if(field==null) continue;
							    DescriptorSerializer serializer = serializers[i];
								if(serializer.isScalarType())
										scalarFields.set(i);
								else if(DerbyBytesUtil.isFloatType(field))
										floatFields.set(i);
								else if(DerbyBytesUtil.isDoubleType(field))
										doubleFields.set(i);
								i++;
						}
				}
				return EntryEncoder.create(kryoPool,nCols,notNullFields,scalarFields,floatFields,doubleFields);
		}

		protected BitSet getNotNullFields(ExecRow row,BitSet notNullFields) {
				notNullFields.clear();
				if(keyColumns!=null){
						DataValueDescriptor[] fields = row.getRowArray();
						for(int keyColumn:keyColumns){
								if(keyColumn<0) continue;
								DataValueDescriptor dvd = fields[keyColumn];
								if(dvd!=null &&!dvd.isNull())
										notNullFields.set(keyColumn);
						}
				}else{
						int i=0;
						for(DataValueDescriptor dvd:row.getRowArray()){
								if(dvd!=null &&!dvd.isNull()){
										notNullFields.set(i);
								}
								i++;
						}
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
				super.close();
		}
}
