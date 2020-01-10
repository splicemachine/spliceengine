/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
