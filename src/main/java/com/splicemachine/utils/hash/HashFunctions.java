package com.splicemachine.utils.hash;

/**
 * Utility class for constructing hash functions of various types.
 *
 * @author Scott Fines
 * Date: 11/12/13
 */
public class HashFunctions {

		private HashFunctions(){}

		/**
		 * The Same hash function as is used by java.util.HashMap.
		 *
		 * @return the same hash function as used by java.util.HashMap
		 */
		public static ByteHash32 utilHash(){
			return new ByteHash32() {
					@Override
					public int hash(byte[] bytes, int offset, int length) {
							int h = 1;
							int end = offset+length;
							for(int i=offset;i<end;i++){
									h = 31*h + bytes[offset];
							}

							h ^= (h>>>20)^(h>>>12);
							return h ^(h>>>7)^(h>>>4);
					}
			};
		}

		public static ByteHash32 murmur3(final int seed){
				return new ByteHash32() {
						@Override
						public int hash(byte[] bytes, int offset, int length) {
								return MurmurHash.murmur3_32(bytes,offset,length,seed);
						}
				};
		}
}

