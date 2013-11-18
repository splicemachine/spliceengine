package com.splicemachine.derby.utils.marshall;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

/**
 * @author Scott Fines
 * Date: 11/15/13
 */
public class KeyEncoder {
		private final HashPrefix prefix;
		private final DataHash hash;
		private final KeyPostfix postfix;

		public KeyEncoder(HashPrefix prefix, DataHash hash, KeyPostfix postfix) {
				this.prefix = prefix;
				this.hash = hash;
				this.postfix = postfix;
		}

		public byte[] getKey(ExecRow row) throws StandardException {
				hash.setRow(row);
				byte[] hashBytes = hash.encode();

				int prefixLength = prefix.getPrefixLength();
				byte[] finalRowKey = new byte[prefixLength+hashBytes.length+postfix.getPostfixLength(hashBytes)];
				prefix.encode(finalRowKey, 0, hashBytes);
				System.arraycopy(hashBytes,0,finalRowKey,prefixLength,hashBytes.length);
				postfix.encodeInto(finalRowKey,prefixLength+hashBytes.length,hashBytes);

				return finalRowKey;
		}

		public KeyDecoder getDecoder(){
				return new KeyDecoder(hash.getDecoder(),prefix.getPrefixLength());
		}
}
