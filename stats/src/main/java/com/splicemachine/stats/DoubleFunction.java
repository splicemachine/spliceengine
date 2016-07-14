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

package com.splicemachine.stats;

/**
 * Represents a function from a double to a double.
 *
 * This avoids auto-boxing during function application.
 *
 * @author Scott Fines
 * Date: 3/27/14
 */
public interface DoubleFunction {

		/**
		 * get the value of the function at the specified location.
		 *
		 * @param x the input to the function
		 * @return the value equivalent to y=f(x).
		 */
		double y(double x);
}
