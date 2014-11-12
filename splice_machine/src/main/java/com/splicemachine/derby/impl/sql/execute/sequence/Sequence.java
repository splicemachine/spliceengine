package com.splicemachine.derby.impl.sql.execute.sequence;

import org.apache.derby.iapi.error.StandardException;

public interface Sequence {
	   public long getNext() throws StandardException;
}
