package com.splicemachine.utils;

import com.splicemachine.constants.bytes.BytesUtil;

import org.apache.lucene.util.ArrayUtil;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class ByteSlice implements Externalizable {
		private static final byte[] EMPTY_BYTE_ARRAY = new byte[]{};
		private byte[] buffer;
		private int offset;
		private int length;

		public ByteSlice() {  }

		public static ByteSlice empty(){
				return new ByteSlice(null,0,0);
		}

		public static ByteSlice wrap(ByteBuffer buffer){
				byte[] data = new byte[buffer.remaining()];
				buffer.get(data);
				return new ByteSlice(data,0,data.length);
		}

		public static ByteSlice wrap(byte[] data, int offset, int length) {
				return new ByteSlice(data,offset,length);
		}

		public static ByteSlice wrap(byte[] rowKey) {
				if(rowKey==null) return new ByteSlice(null,0,0);
				return new ByteSlice(rowKey,0,rowKey.length);
		}

		private ByteSlice(byte[] buffer, int offset, int length) {
				this.buffer = buffer;
				this.offset = offset;
				this.length = length;
		}

		public int offset() {
				return offset;
		}

		public int length() {
				return length;
		}

		public byte[] getByteCopy() {
				if(length<=0) return EMPTY_BYTE_ARRAY;
				return BytesUtil.slice(buffer, offset, length);
		}

		public ByteBuffer asBuffer(){
				if(length<=0) return null;
				return ByteBuffer.wrap(buffer,offset,length);
		}

		public void get(byte[] destination, int destOffset){
				assert destOffset + length <=destination.length: "Incorrect size to copy!";
				if(length<=0) return; //nothing to do

				System.arraycopy(buffer,offset,destination,destOffset,length);
		}

		public void set(byte[] buffer, int offset, int length) {
				this.buffer = buffer;
				this.offset = offset;
				this.length = length;
		}

		public void updateSlice(ByteSlice slice, boolean reverse){
				byte[] data = slice.array();
				int dOff = slice.offset();
				int dLen = slice.length();
				if(reverse && dLen>0){
						byte[] copy = new byte[dLen];
						System.arraycopy(data,dOff,copy,0,dLen);
						for(int i=0;i<copy.length;i++){
								copy[i] ^=0xff;
						}
						data = copy;
						dOff = 0;
				}
				set(data, dOff, dLen);
		}
		public void set(ByteSlice rowSlice, boolean reverse) {
				byte[] data;
				int offset, length;

				if(reverse){
						data = rowSlice.data(true);
						offset = 0;
						length = data.length;
				}else{
						data = rowSlice.buffer;
						offset = rowSlice.offset;
						length = rowSlice.length;
				}
				set(data,offset,length);
		}

		public byte[] data(boolean reverse){
				if(length<=0) return EMPTY_BYTE_ARRAY;
				byte[] data = BytesUtil.slice(buffer,offset,length);
				if(reverse){
						for(int i=0;i<data.length;i++){
								data[i] ^=0xff;
						}
				}
				return data;
		}

		public byte[] array() {
				return buffer;
		}

		public void reset(){
				length=0;
				buffer =null; //allow GC to collect
		}

		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (!(o instanceof ByteSlice)) return false;

				ByteSlice that = (ByteSlice) o;

				if (length != that.length) return false;
				if (offset != that.offset) return false;
				return ArrayUtil.equals(buffer, offset, that.buffer, that.offset, length);
		}

		@Override
		public int hashCode() {
				int result = buffer != null ? Arrays.hashCode(buffer) : 0;
				result = 31 * result + offset;
				result = 31 * result + length;
				return result;
		}

		@Override
		public void readExternal(ObjectInput in) throws IOException,ClassNotFoundException {
			offset = in.readInt();
			length = in.readInt();
			buffer = new byte[length];
			in.readFully(buffer);
		}

		@Override
		public void writeExternal(ObjectOutput out) throws IOException {
			out.writeInt(offset);
			out.writeInt(length);
			out.write(buffer,offset,length);
		}

}
