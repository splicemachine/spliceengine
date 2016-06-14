package com.splicemachine.collections;

import java.io.Closeable;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 2/20/14
 */
public interface CloseableIterator<E> extends Iterator<E>,Closeable {
}
