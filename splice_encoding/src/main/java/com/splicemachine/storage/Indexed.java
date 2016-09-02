/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

}
