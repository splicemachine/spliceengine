package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.db.iapi.services.io.FormatableInstanceGetter;


public class HBaseClassInfo extends FormatableInstanceGetter {

	public Object getNewInstance() {
		return new HBaseRowLocation();
	}
}
