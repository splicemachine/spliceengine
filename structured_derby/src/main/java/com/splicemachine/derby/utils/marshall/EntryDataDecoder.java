package com.splicemachine.derby.utils.marshall;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

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
