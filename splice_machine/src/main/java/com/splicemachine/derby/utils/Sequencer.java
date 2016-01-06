package com.splicemachine.derby.utils;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public interface Sequencer{

    long next() throws IOException;

    void setPosition(long sequence) throws IOException;
}
