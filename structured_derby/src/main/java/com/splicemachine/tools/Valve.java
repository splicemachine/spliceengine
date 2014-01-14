package com.splicemachine.tools;

/**
 * @author Scott Fines
 *         Date: 11/25/13
 */
public interface Valve {

		public static enum SizeSuggestion{
				INCREMENT{
						@Override
						public int adjust(int currentSize) {
								return currentSize+1;
						}
				},
				DECREMENT{
						@Override
						public int adjust(int currentSize) {
								return currentSize-1;
						}
				},
				HALVE{
						@Override
						public int adjust(int currentSize) {
								return currentSize/2;
						}
				},
				DOUBLE{
						@Override
						public int adjust(int currentSize) {
								return currentSize*2;
						}
				};

				public int adjust(int currentSize){ return currentSize;}
		}

		void adjustValve(SizeSuggestion suggestion);

		int tryAllow();

		void release();

		int getAvailable();

		void setMaxPermits(int newMax);
}
