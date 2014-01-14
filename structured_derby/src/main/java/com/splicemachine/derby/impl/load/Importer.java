package com.splicemachine.derby.impl.load;

import java.io.Closeable;

/**
 * Accepts, Processes, and Writes rows that are fed to it.
 *
 * @author Scott Fines
 * Created on: 9/30/13
 */
public interface Importer extends Closeable {

    void process(String[] parsedRow) throws Exception;

    boolean isClosed();
}
