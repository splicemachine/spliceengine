package com.splicemachine.derby.utils.marshall;

/**
 * Represent a "Prefix" for a RowKey.
 *
 * Row keys in Splice come in three parts:
 *
 * Prefix (this interface)
 * Hash
 * Postfix
 *
 * Where each of the three parts is optional.
 *
 * @author Scott Fines
 * Date: 11/15/13
 */
public interface HashPrefix {

		int getPrefixLength();

		void encode(byte[] bytes,int offset,byte[] hashBytes);
}
