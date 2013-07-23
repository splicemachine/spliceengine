package com.splicemachine.storage;

import java.io.Externalizable;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public interface Predicate extends Externalizable {

    boolean applies(int column);

    boolean match(int column,byte[] data, int offset, int length);

    /**
     * @return true if this predicate should ALSO be applied after the row is fully composed.
     */
    boolean checkAfter();

    void setCheckedColumns(BitSet checkedColumns);

    void reset();

}
