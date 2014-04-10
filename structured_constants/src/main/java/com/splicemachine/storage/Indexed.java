package com.splicemachine.storage;

/**
 * @author Scott Fines
 * Date: 4/4/14
 */
public interface Indexed {

		/**
		 * Returns the next set bit at or higher than {@code position}.
		 *
		 * @param currentPosition the position to start from
		 * @return the index of the next set bit that has index equal to or higher than {@code position}
		 */
		int nextSetBit(int currentPosition);

		boolean isScalarType(int position);

		boolean isDoubleType(int position);

		boolean isFloatType(int position);

		int getPredicatePosition(int encodedPos);
}
