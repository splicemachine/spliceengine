package com.splicemachine.si.api;

import com.splicemachine.si.data.hbase.IHTable;
import com.splicemachine.si.impl.RollForwardAction;

/**
 * @author Scott Fines
 *         Date: 2/13/14
 */
public interface RollForwardFactory<Data,Table extends IHTable> {

		RollForwardAction newAction(Table table);
}
