package com.splicemachine.derby.utils.marshall;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.utils.DerbyBytesUtil;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;
import java.util.GregorianCalendar;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class BareKeyHash{

		protected final int[] keyColumns;
		protected final boolean[] keySortOrder;
		protected final boolean sparse;
		protected final DescriptorSerializer[] serializers;

		protected final KryoPool kryoPool;
		private GregorianCalendar calendar;

		protected BareKeyHash(int[] keyColumns,
													boolean[] keySortOrder,
													boolean sparse,
													KryoPool kryoPool,
													DescriptorSerializer[] serializers) {
				this.keyColumns = keyColumns;
				this.keySortOrder = keySortOrder;
				this.sparse = sparse;
				this.kryoPool = kryoPool;
				this.serializers = serializers;
		}

		public static DataHash encoder(int[] keyColumns, boolean[] keySortOrder,DescriptorSerializer[] serializers){
				return encoder(keyColumns, keySortOrder, SpliceKryoRegistry.getInstance(),serializers);
		}

		public static DataHash encoder(int[] keyColumns, boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
				return new Encoder(keyColumns,keySortOrder,kryoPool,serializers);
		}

		public static KeyHashDecoder decoder(int[] keyColumns, boolean[] keySortOrder,DescriptorSerializer[] serializers){
				return decoder(keyColumns, keySortOrder, SpliceKryoRegistry.getInstance(), serializers);
		}

		public static KeyHashDecoder decoder(int[] keyColumns, boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers){
				return new Decoder(keyColumns,keySortOrder,kryoPool,serializers);
		}

		protected void pack(MultiFieldEncoder encoder,ExecRow currentRow) throws StandardException, IOException {
				encoder.reset();
				DataValueDescriptor[] dvds = currentRow.getRowArray();
				if(keySortOrder!=null){
						for(int i=0;i<keyColumns.length;i++){
								int pos = keyColumns[i];
								if(pos==-1) continue; //skip columns marked with a -1
								DescriptorSerializer serializer = serializers[pos];
								DataValueDescriptor dvd = dvds[pos];
								serializer.encode(encoder, dvd, !keySortOrder[i]);
						}
				}else{
						for(int keyColumn:keyColumns){
								if(keyColumn==-1) continue; //skip columns marked with a -1
								DescriptorSerializer serializer = serializers[keyColumn];
								DataValueDescriptor dvd = dvds[keyColumn];
								serializer.encode(encoder, dvd, false);
						}
				}
		}

		protected void unpack(ExecRow destination,MultiFieldDecoder decoder) throws StandardException {
				DataValueDescriptor[] fields = destination.getRowArray();
				if(keySortOrder!=null){
						for(int i=0;i<keyColumns.length;i++){
								if(keyColumns[i] !=-1){
										DataValueDescriptor field = fields[keyColumns[i]];
										decodeNext(decoder, field,!keySortOrder[i]);
								}
						}
				}else{
						for(int rowSpot:keyColumns){
								if(rowSpot!=-1 && rowSpot < fields.length){
										DataValueDescriptor field = fields[rowSpot];
										decodeNext(decoder,field,false);
								}
						}
				}
		}

		protected void decodeNext(MultiFieldDecoder decoder, DataValueDescriptor field,boolean sortOrder) throws StandardException {
				if(DerbyBytesUtil.isNextFieldNull(decoder, field)){
						field.setToNull();
						DerbyBytesUtil.skip(decoder, field);
				}else
						DerbyBytesUtil.decodeInto(decoder,field,sortOrder);
		}

		protected void encodeField(MultiFieldEncoder encoder, DataValueDescriptor[] dvds, int position, boolean desc) throws StandardException {
				DataValueDescriptor dvd = dvds[position];
				if(dvd==null){
						if(!sparse)
								encoder.encodeEmpty();
				}
				else if(dvd.isNull()){
						if(!sparse)
								DerbyBytesUtil.encodeTypedEmpty(encoder, dvd, desc, true);
				}else {
						if(calendar==null && DerbyBytesUtil.isTimeFormat(dvd))
								calendar = new GregorianCalendar();
						DerbyBytesUtil.encodeInto(encoder,dvd,desc,calendar);
				}
		}


		private static class Decoder extends BareKeyHash implements KeyHashDecoder{
				private MultiFieldDecoder decoder;

				private Decoder(int[] keyColumns, boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers) {
						super(keyColumns,keySortOrder,false,kryoPool,serializers);
				}

				@Override
				public void set(byte[] bytes, int hashOffset,int length) {
						if(decoder==null)
								decoder = MultiFieldDecoder.create(kryoPool);

						decoder.set(bytes,hashOffset,length);
				}

				@Override
				public void decode(ExecRow destination) throws StandardException {
						unpack(destination, decoder);
				}

		}

		private static class Encoder extends BareKeyHash implements DataHash<ExecRow> {
				private MultiFieldEncoder encoder;

				private ExecRow currentRow;

				public Encoder(int[] keyColumns, boolean[] keySortOrder,KryoPool kryoPool,DescriptorSerializer[] serializers) {
						super(keyColumns,keySortOrder,false,kryoPool,serializers);
				}

				@Override
				public void setRow(ExecRow rowToEncode) {
						this.currentRow = rowToEncode;
				}

				@Override
				public byte[] encode() throws StandardException, IOException {
						if(encoder==null)
								encoder = MultiFieldEncoder.create(kryoPool,keyColumns.length);

						pack(encoder,currentRow);
						return encoder.build();
				}

				@Override
				public KeyHashDecoder getDecoder(){
						return new Decoder(keyColumns,keySortOrder,kryoPool,serializers);
				}
		}
}
