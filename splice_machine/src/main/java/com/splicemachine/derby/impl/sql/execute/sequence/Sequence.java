package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.db.iapi.error.StandardException;

public interface Sequence {
	   public long getNext() throws StandardException;
}
