package com.splicemachine.utils;

import java.io.Closeable;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 7/9/14
 */
public interface Source<T> extends Closeable {

    boolean hasNext() throws IOException;

    T next() throws IOException;
}
