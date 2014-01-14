package com.splicemachine.derby.utils.marshall;

/**
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class NoOpPrefix implements HashPrefix {
		public static final HashPrefix INSTANCE = new NoOpPrefix();

		private NoOpPrefix(){}

		@Override public int getPrefixLength() {return 0;}
		@Override public void encode(byte[] bytes, int offset, byte[] hashBytes) {}//no-op
}
