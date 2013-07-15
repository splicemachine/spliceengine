package com.splicemachine.storage;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Created on: 7/8/13
 */
public interface Predicate extends Externalizable {

    boolean match(int column,byte[] data, int offset, int length);
}
