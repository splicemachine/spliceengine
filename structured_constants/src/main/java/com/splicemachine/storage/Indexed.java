package com.splicemachine.storage;

/**
 * Represents a Row index.
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

		/**
		 * @param position the position to check
		 * @return true if the value at the position is "Scalar typed"--that is, encoded identically to a long
		 */
		boolean isScalarType(int position);

		/**
		 * @param position the position to check
		 * @return true if the value at the position is "Double typed"--that is, encoded identically to a double
		 */
		boolean isDoubleType(int position);

		/**
		 * @param position the position to check
		 * @return true if the value at the position is "Float typed"--that is, encoded identically to a float
		 */
		boolean isFloatType(int position);

		/**
		 * @param encodedPos the physical position in the index.
		 * @return the "predicate position"--that is, the position in the final decoded row of the encoded position.
		 * Mainly this is useful when decoding key columns.
		 */
		int getPredicatePosition(int encodedPos);
}
