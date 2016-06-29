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
