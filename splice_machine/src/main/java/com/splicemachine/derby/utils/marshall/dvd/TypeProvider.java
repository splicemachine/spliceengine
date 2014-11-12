package com.splicemachine.derby.utils.marshall.dvd;

/**
 * @author Scott Fines
 *         Date: 4/9/14
 */
public interface TypeProvider {
		boolean isScalar(int keyFormatId);

		boolean isFloat(int keyFormatId);

		boolean isDouble(int keyFormatId);
}
