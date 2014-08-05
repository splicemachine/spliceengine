package com.splicemachine.utils.hash;

/**
 * Representation of a 32-bit hash function which can be applied to
 * a byte [].
 *
 * @author Scott Fines
 * Date: 11/12/13
 */
public interface ByteHash32 {

		public int hash(byte[] bytes, int offset,int length);
}
