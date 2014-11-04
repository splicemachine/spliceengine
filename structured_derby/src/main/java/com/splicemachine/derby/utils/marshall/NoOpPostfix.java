package com.splicemachine.derby.utils.marshall;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class NoOpPostfix implements KeyPostfix {
		public static final KeyPostfix INSTANCE = new NoOpPostfix();

		private NoOpPostfix(){}

		@Override public int getPostfixLength(byte[] hashBytes) { return 0; }
		@Override public void encodeInto(byte[] keyBytes, int postfixPosition, byte[] hashBytes) { } //no-op

		@Override public void close() throws IOException {  }
}
