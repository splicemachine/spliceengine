package com.splicemachine.derby.iapi.sql.execute;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * @author P Trolard
 *         Date: 13/04/2014
 */
public interface StandardCloseable {
    public void close() throws StandardException, IOException;
}
