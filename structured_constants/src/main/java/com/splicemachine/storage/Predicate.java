package com.splicemachine.storage;

import java.io.Externalizable;

/**
 * @author Scott Fines
 *         Created on: 7/8/13
 */
public interface Predicate extends Externalizable {

    int getColumn();

    boolean match(byte[] data, int offset, int length);
}
