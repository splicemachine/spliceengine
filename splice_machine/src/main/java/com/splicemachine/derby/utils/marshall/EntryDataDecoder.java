/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.utils.marshall;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.kryo.KryoPool;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class EntryDataDecoder extends BareKeyHash implements KeyHashDecoder{
		private EntryDecoder entryDecoder;

		public EntryDataDecoder(int[] keyColumns,
															 boolean[] keySortOrder,
															 DescriptorSerializer[] serializers) {
				this(keyColumns, keySortOrder, SpliceKryoRegistry.getInstance(),serializers);
		}

		protected EntryDataDecoder(int[] keyColumns,
															 boolean[] keySortOrder,
															 KryoPool kryoPool,
															 DescriptorSerializer[] serializers) {
				super(keyColumns, keySortOrder,true,kryoPool,serializers);
		}

		@Override
		public void set(byte[] bytes, int hashOffset, int length) {
				if(entryDecoder==null)
						entryDecoder =new EntryDecoder();

				entryDecoder.set(bytes,hashOffset,length);
		}

		@Override
		public void close() throws IOException {
				super.close();
				if(entryDecoder!=null)
						entryDecoder.close();
		}

		@Override
		public void decode(ExecRow destination) throws StandardException {
				BitIndex index = entryDecoder.getCurrentIndex();
				MultiFieldDecoder decoder;
				try {
						decoder = entryDecoder.getEntryDecoder();
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				DataValueDescriptor[] fields = destination.getRowArray();
				if(keyColumns!=null){
						for(int i=index.nextSetBit(0);i>=0 && i<keyColumns.length;i=index.nextSetBit(i+1)){
								int pos = keyColumns[i];
								if(pos<0) continue;
								DataValueDescriptor dvd = fields[pos];
								if(dvd==null){
										entryDecoder.seekForward(decoder, i);
										continue;
								}
								DescriptorSerializer serializer = serializers[pos];
								boolean sortOrder = keySortOrder != null && !keySortOrder[i];
								serializer.decode(decoder,dvd,sortOrder);
						}
				}else{
						for(int i=index.nextSetBit(0);i>=0 && i<fields.length;i=index.nextSetBit(i+1)){
								DataValueDescriptor dvd = fields[i];
								if(dvd==null){
										entryDecoder.seekForward(decoder,i);
										continue;
								}
								boolean sortOrder = keySortOrder != null && !keySortOrder[i];
								DescriptorSerializer serializer = serializers[i];
								serializer.decode(decoder,dvd,sortOrder);
						}
				}
		}

		public EntryDecoder getFieldDecoder(){
			return entryDecoder;
		}
}
