package com.splicemachine.derby.impl.load;

import java.io.IOException;

/**
 * Convenience interface for switching between different import strategies based on properties
 * of the file being imported (whether or not it's compressed, etc.)
 *
 * @author Scott Fines
 * Created: 1/30/13 2:41 PM
 */
interface Importer {

	/**
	 * Kick off the import.
	 *
	 * @return the number of rows which were imported
	 * @throws IOException if something bad happens and the import failed.
	 */
	long importData() throws IOException;
}
